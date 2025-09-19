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

use std::sync::Arc;
use std::{env, fmt};

use anyhow::Context;
use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::BatchConfigBuilder;
use opentelemetry_sdk::{Resource, trace};
use quickwit_common::{get_bool_from_env, get_from_env_opt};
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
) -> anyhow::Result<EnvFilterReloadFn> {
    #[cfg(feature = "tokio-console")]
    {
        if get_bool_from_env(QW_ENABLE_TOKIO_CONSOLE_ENV_KEY, false) {
            console_subscriber::init();
            return Ok(quickwit_serve::do_nothing_env_filter_reload_fn());
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
    if get_bool_from_env(QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER_ENV_KEY, false) {
        let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .build()
            .context("failed to initialize OpenTelemetry OTLP exporter")?;
        let batch_processor =
            trace::BatchSpanProcessor::builder(otlp_exporter, opentelemetry_sdk::runtime::Tokio)
                .with_batch_config(
                    BatchConfigBuilder::default()
                        // Quickwit can generate a lot of spans, especially in debug mode, and the
                        // default queue size of 2048 is too small.
                        .with_max_queue_size(32_768)
                        .build(),
                )
                .build();
        let provider = opentelemetry_sdk::trace::TracerProvider::builder()
            .with_span_processor(batch_processor)
            .with_resource(Resource::new([
                KeyValue::new("service.name", "quickwit"),
                KeyValue::new("service.version", build_info.version.clone()),
            ]))
            .build();
        let tracer = provider.tracer("quickwit");
        let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        registry
            .with(telemetry_layer)
            .try_init()
            .context("failed to register tracing subscriber")?;
    } else {
        registry
            .try_init()
            .context("failed to register tracing subscriber")?;
    }

    Ok(Arc::new(move |env_filter_def: &str| {
        let new_env_filter = EnvFilter::try_new(env_filter_def)?;
        reload_handle.reload(new_env_filter)?;
        Ok(())
    }))
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
}

impl EventFormat<'_> {
    /// Gets the log format from the environment variable `QW_LOG_FORMAT`. Returns a JSON
    /// formatter if the variable is set to `json`, otherwise returns a full formatter.
    fn get_from_env() -> Self {
        if get_from_env_opt::<String>("QW_LOG_FORMAT", false)
            .map(|log_format| log_format.eq_ignore_ascii_case("json"))
            .unwrap_or(false)
        {
            let json_format = tracing_subscriber::fmt::format().json();
            EventFormat::Json(json_format)
        } else {
            let full_format = tracing_subscriber::fmt::format()
                .with_target(true)
                .with_timer(time_formatter());

            EventFormat::Full(full_format)
        }
    }

    fn format_fields(&self) -> FieldFormat {
        match self {
            EventFormat::Full(_) => FieldFormat::Default(DefaultFields::new()),
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
        }
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
