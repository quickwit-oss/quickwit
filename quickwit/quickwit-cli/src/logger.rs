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
use once_cell::sync::Lazy;
use opentelemetry::propagation::text_map_propagator::FieldIter;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use opentelemetry::trace::{
    SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider,
};
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

const DD_SAMPLING_PRIORITY_HEADER: &str = "x-datadog-sampling-priority";
const DD_SOURCE_HEADER: &str = "x-datadog-querysource";
const DD_SPAN_PARENT_HEADER: &str = "x-datadog-parent-id";
const DD_SPAN_TRACE_HEADER: &str = "x-datadog-trace-id";

static DD_APM_HEADERS: Lazy<Vec<String>> = Lazy::new(|| {
    vec![
        DD_SAMPLING_PRIORITY_HEADER.to_string(),
        DD_SOURCE_HEADER.to_string(),
        DD_SPAN_PARENT_HEADER.to_string(),
        DD_SPAN_TRACE_HEADER.to_string(),
    ]
});

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
    let env_filter = env::var("RUST_LOG")
        .map(|_| EnvFilter::from_default_env())
        .or_else(|_| EnvFilter::try_new(format!("quickwit={level},tantivy=WARN")))
        .context("failed to set up tracing env filter")?;

    let composite_propagator = TextMapCompositePropagator::new(vec![
        Box::new(DatadogApmPropagator),
        Box::new(TraceContextPropagator::new()),
    ]);
    global::set_text_map_propagator(composite_propagator);

    let (reloadable_env_filter, reload_handle) = tracing_subscriber::reload::Layer::new(env_filter);
    let registry = tracing_subscriber::registry().with(reloadable_env_filter);
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

        let event_format = EventFormat::get_from_env();
        let fmt_fields = event_format.format_fields();

        registry
            .with(telemetry_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(event_format)
                    .fmt_fields(fmt_fields)
                    .with_ansi(ansi_colors),
            )
            .try_init()
            .context("failed to register tracing subscriber")?;
    } else {
        let event_format = EventFormat::get_from_env();
        let fmt_fields = event_format.format_fields();

        registry
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(event_format)
                    .fmt_fields(fmt_fields)
                    .with_ansi(ansi_colors),
            )
            .try_init()
            .context("failed to register tracing subscriber")?;
    }
    Ok(Arc::new(move |env_filter_def: &str| {
        let new_env_filter = EnvFilter::try_new(env_filter_def)?;
        reload_handle.reload(new_env_filter)?;
        Ok(())
    }))
}

#[derive(Debug)]
struct DatadogApmPropagator;

fn extract_span_context(
    extractor: &dyn opentelemetry::propagation::Extractor,
) -> Option<SpanContext> {
    let trace_id = extractor
        .get(DD_SPAN_TRACE_HEADER)?
        .parse::<u128>()
        .map(TraceId::from)
        .ok()?;
    let span_id = extractor
        .get(DD_SPAN_PARENT_HEADER)?
        .parse::<u64>()
        .map(SpanId::from)
        .ok()?;

    let span_context = SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::NOT_SAMPLED,
        true,
        TraceState::default(),
    );

    if span_context.is_valid() {
        return Some(span_context);
    }
    None
}

impl TextMapPropagator for DatadogApmPropagator {
    fn fields(&self) -> FieldIter<'_> {
        FieldIter::new(&DD_APM_HEADERS)
    }

    fn extract_with_context(
        &self,
        cx: &opentelemetry::Context,
        extractor: &dyn opentelemetry::propagation::Extractor,
    ) -> opentelemetry::Context {
        let Some(span_context) = extract_span_context(extractor) else {
            return cx.clone();
        };
        cx.with_remote_span_context(span_context)
    }

    fn inject_context(
        &self,
        cx: &opentelemetry::Context,
        injector: &mut dyn opentelemetry::propagation::Injector,
    ) {
        let span = cx.span();
        let span_context = span.span_context();

        if span_context.is_valid() {
            let trace_id: u128 = u128::from_be_bytes(span_context.trace_id().to_bytes());
            injector.set(DD_SPAN_TRACE_HEADER, trace_id.to_string());

            let span_id: u64 = u64::from_be_bytes(span_context.span_id().to_bytes());
            injector.set(DD_SPAN_PARENT_HEADER, span_id.to_string());

            let trace_state = span_context.trace_state();

            if let Some(source) = trace_state.get(DD_SOURCE_HEADER) {
                injector.set(DD_SOURCE_HEADER, source.to_string());
            }
            if let Some(priority) = trace_state.get(DD_SAMPLING_PRIORITY_HEADER) {
                injector.set(DD_SAMPLING_PRIORITY_HEADER, priority.to_string());
            }
        }
    }
}

enum EventFormat<'a> {
    Full(Format<Full, UtcTime<Vec<BorrowedFormatItem<'a>>>>),
    Json(Format<Json>),
}

impl EventFormat<'_> {
    /// Gets the log format from the environment variable `QW_LOG_FORMAT`. Returns a JSON
    /// formatter if the variable is set to `json`, otherwise returns a full formatter.
    fn get_from_env() -> Self {
        if get_from_env_opt::<String>("QW_LOG_FORMAT")
            .map(|log_format| log_format.eq_ignore_ascii_case("json"))
            .unwrap_or(false)
        {
            let json_format = tracing_subscriber::fmt::format().json();
            EventFormat::Json(json_format)
        } else {
            // We do not rely on the RFC3339 implementation, because it has a nanosecond precision.
            // See discussion here: https://github.com/time-rs/time/discussions/418
            let timer_format = time::format_description::parse(
                "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
            )
            .expect("time format description should be valid");
            let timer = UtcTime::new(timer_format);

            let full_format = tracing_subscriber::fmt::format()
                .with_target(true)
                .with_timer(timer);

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
