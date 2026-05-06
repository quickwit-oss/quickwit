// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::str::FromStr;
use std::sync::Arc;
use std::{env, fmt};

use anyhow::Context;
use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{
    LogExporter, Protocol as OtlpWireProtocol, SpanExporter, WithExportConfig,
};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{BatchConfigBuilder, SdkTracerProvider};
use opentelemetry_sdk::{Resource, trace};
use quickwit_common::{get_bool_from_env, get_from_env, get_from_env_opt};
use quickwit_serve::{BuildInfo, EnvFilterReloadFn};
use time::format_description::BorrowedFormatItem;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::field::RecordFields;
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::format::{
    DefaultFields, Format, FormatEvent, FormatFields, Full, Json, JsonFields, Writer,
};
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

use crate::QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER_ENV_KEY;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OtlpProtocol {
    Grpc,
    HttpProtobuf,
    HttpJson,
}

impl OtlpProtocol {
    fn log_exporter(&self) -> anyhow::Result<LogExporter> {
        match self {
            OtlpProtocol::Grpc => LogExporter::builder().with_tonic().build(),
            OtlpProtocol::HttpProtobuf => LogExporter::builder()
                .with_http()
                .with_protocol(OtlpWireProtocol::HttpBinary)
                .build(),
            OtlpProtocol::HttpJson => LogExporter::builder()
                .with_http()
                .with_protocol(OtlpWireProtocol::HttpJson)
                .build(),
        }
        .context("failed to initialize OTLP logs exporter")
    }

    fn span_exporter(&self) -> anyhow::Result<SpanExporter> {
        match self {
            OtlpProtocol::Grpc => SpanExporter::builder().with_tonic().build(),
            OtlpProtocol::HttpProtobuf => SpanExporter::builder()
                .with_http()
                .with_protocol(OtlpWireProtocol::HttpBinary)
                .build(),
            OtlpProtocol::HttpJson => SpanExporter::builder()
                .with_http()
                .with_protocol(OtlpWireProtocol::HttpJson)
                .build(),
        }
        .context("failed to initialize OTLP traces exporter")
    }
}

impl FromStr for OtlpProtocol {
    type Err = anyhow::Error;

    fn from_str(protocol_str: &str) -> anyhow::Result<Self> {
        const OTLP_PROTOCOL_GRPC: &str = "grpc";
        const OTLP_PROTOCOL_HTTP_PROTOBUF: &str = "http/protobuf";
        const OTLP_PROTOCOL_HTTP_JSON: &str = "http/json";

        match protocol_str {
            OTLP_PROTOCOL_GRPC => Ok(OtlpProtocol::Grpc),
            OTLP_PROTOCOL_HTTP_PROTOBUF => Ok(OtlpProtocol::HttpProtobuf),
            OTLP_PROTOCOL_HTTP_JSON => Ok(OtlpProtocol::HttpJson),
            other => anyhow::bail!(
                "unsupported OTLP protocol `{other}`, supported values are \
                 `{OTLP_PROTOCOL_GRPC}`, `{OTLP_PROTOCOL_HTTP_PROTOBUF}` and \
                 `{OTLP_PROTOCOL_HTTP_JSON}`"
            ),
        }
    }
}

#[cfg(feature = "tokio-console")]
use crate::QW_ENABLE_TOKIO_CONSOLE_ENV_KEY;

/// Load the default logging filter from the environment. The filter can later
/// be updated using the result callback of [setup_logging_and_tracing].
fn startup_env_filter(level: Level) -> anyhow::Result<EnvFilter> {
    let env_filter = env::var("RUST_LOG")
        .map(|_| EnvFilter::from_default_env())
        .or_else(|_| EnvFilter::try_new(format!("quickwit={level},tantivy=WARN")))
        .context("failed to set up tracing env filter")?;
    Ok(env_filter)
}

type ReloadLayer = tracing_subscriber::reload::Layer<EnvFilter, tracing_subscriber::Registry>;

pub fn setup_logging_and_tracing(
    level: Level,
    ansi_colors: bool,
    build_info: &BuildInfo,
) -> anyhow::Result<(
    EnvFilterReloadFn,
    Option<(SdkTracerProvider, SdkLoggerProvider)>,
)> {
    #[cfg(feature = "tokio-console")]
    {
        if get_bool_from_env(QW_ENABLE_TOKIO_CONSOLE_ENV_KEY, false) {
            console_subscriber::init();
            return Ok((quickwit_serve::do_nothing_env_filter_reload_fn(), None));
        }
    }
    global::set_text_map_propagator(TraceContextPropagator::new());

    let event_format = EventFormat::get_from_env();
    let fmt_fields = event_format.format_fields();
    let registry = tracing_subscriber::registry();

    let (reloadable_env_filter, reload_handle) = ReloadLayer::new(startup_env_filter(level)?);

    #[cfg(not(feature = "jemalloc-profiled"))]
    let registry = registry.with(reloadable_env_filter).with(
        tracing_subscriber::fmt::layer()
            .event_format(event_format)
            .fmt_fields(fmt_fields)
            .with_ansi(ansi_colors),
    );

    #[cfg(feature = "jemalloc-profiled")]
    let registry = jemalloc_profiled::configure_registry(
        registry,
        event_format,
        fmt_fields,
        ansi_colors,
        level,
        reloadable_env_filter,
    )?;

    // Note on disabling ANSI characters: setting the ansi boolean on event format is insufficient.
    // It is thus set on layers, see https://github.com/tokio-rs/tracing/issues/1817
    let provider_opt = if get_bool_from_env(QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER_ENV_KEY, false) {
        let global_protocol_str =
            get_from_env("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc".to_string(), false);
        let global_protocol = OtlpProtocol::from_str(&global_protocol_str)?;

        let traces_protocol_opt =
            get_from_env_opt::<String>("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", false);
        let traces_protocol = traces_protocol_opt
            .as_deref()
            .map(OtlpProtocol::from_str)
            .transpose()?
            .unwrap_or(global_protocol);

        let span_exporter = traces_protocol.span_exporter()?;
        let span_processor = trace::BatchSpanProcessor::builder(span_exporter)
            .with_batch_config(
                BatchConfigBuilder::default()
                    // Quickwit can generate a lot of spans, especially in debug mode, and the
                    // default queue size of 2048 is too small.
                    .with_max_queue_size(32_768)
                    .build(),
            )
            .build();

        let resource = Resource::builder()
            .with_service_name("quickwit")
            .with_attribute(KeyValue::new("service.version", build_info.version.clone()))
            .build();

        let logs_protocol_opt =
            get_from_env_opt::<String>("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", false);
        let logs_protocol = logs_protocol_opt
            .as_deref()
            .map(OtlpProtocol::from_str)
            .transpose()?
            .unwrap_or(global_protocol);
        let log_exporter = logs_protocol.log_exporter()?;
        let logger_provider = SdkLoggerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(log_exporter)
            .build();

        let tracing_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_span_processor(span_processor)
            .with_resource(resource)
            .build();

        let tracer = tracing_provider.tracer("quickwit");
        let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        // Bridge between tracing logs and otel tracing events
        let logs_otel_layer = OpenTelemetryTracingBridge::new(&logger_provider);

        registry
            .with(telemetry_layer)
            .with(logs_otel_layer)
            .try_init()
            .context("failed to register tracing subscriber")?;
        Some((tracing_provider, logger_provider))
    } else {
        registry
            .try_init()
            .context("failed to register tracing subscriber")?;
        None
    };

    Ok((
        Arc::new(move |env_filter_def: &str| {
            let new_env_filter = EnvFilter::try_new(env_filter_def)?;
            reload_handle.reload(new_env_filter)?;
            Ok(())
        }),
        provider_opt,
    ))
}

/// We do not rely on the RFC3339 implementation, because it has a nanosecond precision.
/// See discussion here: https://github.com/time-rs/time/discussions/418
fn time_formatter() -> UtcTime<Vec<BorrowedFormatItem<'static>>> {
    let time_format = time::format_description::parse(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
    )
    .expect("time format description should be valid");
    UtcTime::new(time_format)
}

enum EventFormat<'a> {
    Full(Format<Full, UtcTime<Vec<BorrowedFormatItem<'a>>>>),
    Json(Format<Json>),
    Ddg(DdgFormat),
}

impl EventFormat<'_> {
    /// Gets the log format from the environment variable `QW_LOG_FORMAT`.
    fn get_from_env() -> Self {
        match get_from_env_opt::<String>("QW_LOG_FORMAT", false)
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("json") => EventFormat::Json(tracing_subscriber::fmt::format().json()),
            Some("ddg") => EventFormat::Ddg(DdgFormat::new()),
            _ => {
                let full_format = tracing_subscriber::fmt::format()
                    .with_target(true)
                    .with_timer(time_formatter());
                EventFormat::Full(full_format)
            }
        }
    }

    fn format_fields(&self) -> FieldFormat {
        match self {
            EventFormat::Full(_) | EventFormat::Ddg(_) => {
                FieldFormat::Default(DefaultFields::new())
            }
            EventFormat::Json(_) => FieldFormat::Json(JsonFields::new()),
        }
    }
}

impl<S, N> FormatEvent<S, N> for EventFormat<'_>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        match self {
            EventFormat::Full(format) => format.format_event(ctx, writer, event),
            EventFormat::Json(format) => format.format_event(ctx, writer, event),
            EventFormat::Ddg(format) => format.format_event(ctx, writer, event),
        }
    }
}

/// Outputs JSON with `timestamp`, `level`, `service`, `source`, and `message` fields.
/// The `message` is formatted using the regular text formatter (level, target, spans, fields).
///
/// Example output:
/// ```json
/// {"timestamp":"2025-03-23T14:30:45Z","level":"INFO","service":"quickwit","ddsource":"quickwit","message":"INFO quickwit_search: hello"}
/// ```
struct DdgFormat {
    text_format: Format<Full, ()>,
}

impl DdgFormat {
    fn new() -> Self {
        Self {
            text_format: tracing_subscriber::fmt::format()
                .with_target(true)
                .without_time(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for DdgFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        // Render the event as text using the Full formatter (without timestamp)
        let mut message = String::with_capacity(256);
        self.text_format
            .format_event(ctx, Writer::new(&mut message), event)?;
        let message = message.trim();

        // Timestamp (RFC 3339)
        let timestamp = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|_| fmt::Error)?;

        let level = event.metadata().level().as_str();

        // Write JSON with properly escaped message
        let escaped_message = serde_json::to_string(message).map_err(|_| fmt::Error)?;
        writeln!(
            writer,
            r#"{{"timestamp":"{timestamp}","level":"{level}","service":"quickwit","ddsource":"quickwit","message":{escaped_message}}}"#
        )
    }
}

enum FieldFormat {
    Default(DefaultFields),
    Json(JsonFields),
}

impl FormatFields<'_> for FieldFormat {
    fn format_fields<R: RecordFields>(&self, writer: Writer<'_>, fields: R) -> fmt::Result {
        match self {
            FieldFormat::Default(default_fields) => default_fields.format_fields(writer, fields),
            FieldFormat::Json(json_fields) => json_fields.format_fields(writer, fields),
        }
    }
}

/// Logger configurations specific to the jemalloc profiler.
///
/// A custom event formatter is used to print the backtrace of the
/// profiling events.
#[cfg(feature = "jemalloc-profiled")]
pub(super) mod jemalloc_profiled {
    use std::fmt;

    use quickwit_common::jemalloc_profiled::JEMALLOC_PROFILER_TARGET;
    use time::format_description::BorrowedFormatItem;
    use tracing::{Event, Level, Metadata, Subscriber};
    use tracing_subscriber::Layer;
    use tracing_subscriber::filter::filter_fn;
    use tracing_subscriber::fmt::format::{DefaultFields, Writer};
    use tracing_subscriber::fmt::time::{FormatTime, UtcTime};
    use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::registry::LookupSpan;

    use super::{EventFormat, FieldFormat, startup_env_filter, time_formatter};
    use crate::logger::ReloadLayer;

    /// An event formatter specific to the memory profiler output.
    ///
    /// Also displays a backtrace after the spans and fields of the tracing
    /// event (into separate lines).
    struct ProfilingFormat {
        time_formatter: UtcTime<Vec<BorrowedFormatItem<'static>>>,
    }

    impl Default for ProfilingFormat {
        fn default() -> Self {
            Self {
                time_formatter: time_formatter(),
            }
        }
    }

    impl<S, N> FormatEvent<S, N> for ProfilingFormat
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
    {
        fn format_event(
            &self,
            ctx: &FmtContext<'_, S, N>,
            mut writer: Writer<'_>,
            event: &Event<'_>,
        ) -> fmt::Result {
            self.time_formatter.format_time(&mut writer)?;
            write!(writer, " {JEMALLOC_PROFILER_TARGET} ")?;
            if let Some(scope) = ctx.event_scope() {
                let mut seen = false;

                for span in scope.from_root() {
                    write!(writer, "{}", span.metadata().name())?;
                    seen = true;

                    let ext = span.extensions();
                    if let Some(fields) = &ext.get::<FormattedFields<N>>()
                        && !fields.is_empty()
                    {
                        write!(writer, "{{{fields}}}:")?;
                    }
                }

                if seen {
                    writer.write_char(' ')?;
                }
            };

            ctx.format_fields(writer.by_ref(), event)?;
            writeln!(writer)?;

            // Print a backtrace to help identify the callsite
            backtrace::trace(|frame| {
                backtrace::resolve_frame(frame, |symbol| {
                    if let Some(symbole_name) = symbol.name() {
                        let _ = writeln!(writer, "{symbole_name}");
                    } else {
                        let _ = writeln!(writer, "symb failed");
                    }
                });
                true
            });
            Ok(())
        }
    }

    fn profiler_tracing_filter(metadata: &Metadata) -> bool {
        metadata.is_span() || (metadata.is_event() && metadata.target() == JEMALLOC_PROFILER_TARGET)
    }

    /// Configures the regular logging layer and a specific layer that gathers
    /// extra debug information for the jemalloc profiler.
    ///
    /// The the jemalloc profiler formatter disables the env filter reloading
    /// because the [tracing_subscriber::reload::Layer] seems to overwrite the
    /// filter configured by [profiler_tracing_filter()] even though it is
    /// applied to a separate layer.
    pub(super) fn configure_registry<S>(
        registry: S,
        event_format: EventFormat<'static>,
        fmt_fields: FieldFormat,
        ansi_colors: bool,
        level: Level,
        _reloadable_env_filter: ReloadLayer,
    ) -> anyhow::Result<impl Subscriber + for<'span> LookupSpan<'span>>
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        Ok(registry
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(ProfilingFormat::default())
                    .fmt_fields(DefaultFields::new())
                    .with_ansi(ansi_colors)
                    .with_filter(filter_fn(profiler_tracing_filter)),
            )
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(event_format)
                    .fmt_fields(fmt_fields)
                    .with_ansi(ansi_colors)
                    .with_filter(startup_env_filter(level)?),
            ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    #[test]
    fn test_otlp_protocol_from_str() {
        assert_eq!(OtlpProtocol::from_str("grpc").unwrap(), OtlpProtocol::Grpc);
        assert_eq!(
            OtlpProtocol::from_str("http/protobuf").unwrap(),
            OtlpProtocol::HttpProtobuf
        );
        assert_eq!(
            OtlpProtocol::from_str("http/json").unwrap(),
            OtlpProtocol::HttpJson
        );
        assert!(OtlpProtocol::from_str("http/xml").is_err());
    }

    /// A shared buffer writer for capturing log output in tests.
    #[derive(Clone, Default)]
    struct TestMakeWriter(Arc<Mutex<Vec<u8>>>);

    impl TestMakeWriter {
        fn get_string(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TestMakeWriter {
        type Writer = TestWriter;

        fn make_writer(&'a self) -> Self::Writer {
            TestWriter(self.0.clone())
        }
    }

    struct TestWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().write_all(buf)?;
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Sets up a subscriber with `DdgFormat` and captures the output.
    fn capture_ddg_log<F: FnOnce()>(f: F) -> serde_json::Value {
        let writer = TestMakeWriter::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .event_format(DdgFormat::new())
                .fmt_fields(FieldFormat::Default(DefaultFields::new()))
                .with_ansi(false)
                .with_writer(writer.clone()),
        );
        tracing::subscriber::with_default(subscriber, f);
        let output = writer.get_string();
        serde_json::from_str(&output).expect("output should be valid JSON")
    }

    const TARGET: &str = "quickwit_cli::logger::tests";

    #[test]
    fn test_ddg_format_has_expected_fields() {
        let json = capture_ddg_log(|| tracing::info!("hello"));
        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 5, "{obj:?}");
        assert!(obj.contains_key("timestamp"));
        assert!(obj.contains_key("level"));
        assert!(obj.contains_key("service"));
        assert!(obj.contains_key("ddsource"));
        assert!(obj.contains_key("message"));
    }

    #[test]
    fn test_ddg_format_basic_message() {
        let json = capture_ddg_log(|| tracing::info!("hello world"));
        assert_eq!(json["level"], "INFO");
        assert_eq!(json["service"], "quickwit");
        assert_eq!(json["ddsource"], "quickwit");
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO {TARGET}: hello world")
        );
    }

    #[test]
    fn test_ddg_format_with_fields() {
        let json = capture_ddg_log(|| {
            tracing::info!(key = "value", count = 42, "processing request");
        });
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO {TARGET}: processing request key=\"value\" count=42")
        );
    }

    #[test]
    fn test_ddg_format_with_span() {
        let json = capture_ddg_log(|| {
            let span = tracing::info_span!("my_span", id = 123);
            let _guard = span.enter();
            tracing::info!("inside span");
        });
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO my_span{{id=123}}: {TARGET}: inside span")
        );
    }

    /// Captures raw text output using the production Full formatter (with timestamp, no ANSI).
    fn capture_full_log<F: FnOnce()>(f: F) -> String {
        let writer = TestMakeWriter::default();
        let full_format = tracing_subscriber::fmt::format()
            .with_target(true)
            .with_timer(time_formatter());
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .event_format(full_format)
                .fmt_fields(DefaultFields::new())
                .with_ansi(false)
                .with_writer(writer.clone()),
        );
        tracing::subscriber::with_default(subscriber, f);
        writer.get_string().trim_end().to_string()
    }

    #[test]
    fn test_ddg_format_with_nested_spans() {
        let make_event = || {
            let outer = tracing::info_span!("outer", req_id = 42);
            let _outer_guard = outer.enter();
            let inner = tracing::info_span!("inner", step = "parse");
            let _inner_guard = inner.enter();
            tracing::info!("deep inside");
        };

        // Compare DDG message against production Full formatter output.
        // The only difference is the leading timestamp.
        let full_output = capture_full_log(make_event);
        let json = capture_ddg_log(make_event);
        let ddg_message = json["message"].as_str().unwrap();

        // Full output: "2025-03-23T14:30:45.123Z  INFO outer{...}: target: deep inside"
        // DDG message:                            "INFO outer{...}: target: deep inside"
        // The timestamp adds one extra space of padding, so we trim both and compare.
        let full_without_timestamp = full_output
            .find("  ")
            .map(|pos| &full_output[pos..])
            .unwrap_or(&full_output);
        assert_eq!(
            ddg_message.trim_start(),
            full_without_timestamp.trim_start(),
        );

        assert_eq!(
            ddg_message,
            format!("INFO outer{{req_id=42}}:inner{{step=\"parse\"}}: {TARGET}: deep inside")
        );
    }

    #[test]
    fn test_ddg_format_escapes_special_chars() {
        let json = capture_ddg_log(|| {
            tracing::info!(r#"hello "world" with\backslash"#);
        });
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!(r#"INFO {TARGET}: hello "world" with\backslash"#)
        );
    }

    #[test]
    fn test_ddg_format_escapes_newlines() {
        let json = capture_ddg_log(|| {
            tracing::info!("line1\nline2\ttab");
        });
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO {TARGET}: line1\nline2\ttab")
        );
    }

    #[test]
    fn test_ddg_format_levels() {
        for (expected_level, log_fn) in [
            (
                "WARN",
                Box::new(|| tracing::warn!("w")) as Box<dyn FnOnce()>,
            ),
            ("ERROR", Box::new(|| tracing::error!("e"))),
            ("INFO", Box::new(|| tracing::info!("i"))),
        ] {
            let json = capture_ddg_log(log_fn);
            assert_eq!(json["level"], expected_level);
        }
    }

    #[test]
    fn test_ddg_format_timestamp_is_rfc3339() {
        let json = capture_ddg_log(|| tracing::info!("hello"));
        let ts = json["timestamp"].as_str().unwrap();
        time::OffsetDateTime::parse(ts, &time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|err| panic!("timestamp is not valid RFC 3339: {ts}: {err}"));
    }

    #[test]
    fn test_ddg_format_with_bool_and_float_fields() {
        let json = capture_ddg_log(|| {
            tracing::info!(enabled = true, ratio = 0.75, "status check");
        });
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO {TARGET}: status check enabled=true ratio=0.75")
        );
    }

    #[test]
    fn test_ddg_format_fields_only() {
        let json = capture_ddg_log(|| {
            tracing::info!(action = "ping");
        });
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO {TARGET}: action=\"ping\"")
        );
    }
}
