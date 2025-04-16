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

use anyhow::Context;
use once_cell::sync::OnceCell;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{BatchConfigBuilder, TracerProvider};
use opentelemetry_sdk::{Resource, trace};
use quickwit_serve::BuildInfo;
use tracing::{Level, debug};
use tracing_subscriber::fmt::format::{FmtSpan, JsonFields};
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Layer};

use crate::environment::{
    ENABLE_VERBOSE_JSON_LOGS, OPENTELEMETRY_AUTHORIZATION, OPENTELEMETRY_URL,
};

static TRACER_PROVIDER: OnceCell<TracerProvider> = OnceCell::new();
pub(crate) const RUNTIME_CONTEXT_SPAN: &str = "runtime_context";

fn fmt_env_filter(level: Level) -> EnvFilter {
    let default_directive = format!("quickwit={level}")
        .parse()
        .expect("default directive should be valid");
    EnvFilter::builder()
        .with_default_directive(default_directive)
        .from_env_lossy()
}

fn fmt_time_format() -> UtcTime<std::vec::Vec<time::format_description::FormatItem<'static>>> {
    // We do not rely on the Rfc3339 implementation, because it has a nanosecond precision.
    // See discussion here: https://github.com/time-rs/time/discussions/418
    UtcTime::new(
        time::format_description::parse(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
        )
        .expect("Time format invalid."),
    )
}

fn compact_fmt_layer<S>(level: Level) -> impl Layer<S>
where
    S: for<'a> LookupSpan<'a>,
    S: tracing::Subscriber,
{
    let event_format = tracing_subscriber::fmt::format()
        .with_target(true)
        .with_timer(fmt_time_format())
        .compact();

    tracing_subscriber::fmt::layer::<S>()
        .event_format(event_format)
        .with_ansi(false)
        .with_filter(fmt_env_filter(level))
}

fn json_fmt_layer<S>(level: Level) -> impl Layer<S>
where
    S: for<'a> LookupSpan<'a>,
    S: tracing::Subscriber,
{
    let event_format = tracing_subscriber::fmt::format()
        .with_target(true)
        .with_timer(fmt_time_format())
        .json();
    tracing_subscriber::fmt::layer::<S>()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .event_format(event_format)
        .fmt_fields(JsonFields::default())
        .with_ansi(false)
        .with_filter(fmt_env_filter(level))
}

fn fmt_layer<S>(level: Level) -> Box<dyn Layer<S> + Send + Sync + 'static>
where
    S: for<'a> LookupSpan<'a>,
    S: tracing::Subscriber,
{
    if *ENABLE_VERBOSE_JSON_LOGS {
        json_fmt_layer(level).boxed()
    } else {
        compact_fmt_layer(level).boxed()
    }
}

fn otlp_layer<S>(
    ot_url: String,
    ot_auth: String,
    level: Level,
    build_info: &BuildInfo,
) -> anyhow::Result<impl Layer<S>>
where
    S: for<'a> LookupSpan<'a>,
    S: tracing::Subscriber,
{
    let headers = std::collections::HashMap::from([("Authorization".to_string(), ot_auth)]);
    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(ot_url)
        .with_headers(headers)
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
    TRACER_PROVIDER
        .set(provider.clone())
        .expect("cell should be empty");
    let tracer = provider.tracer("quickwit");
    let env_filter = std::env::var(EnvFilter::DEFAULT_ENV)
        .map(|_| EnvFilter::from_default_env())
        .or_else(|_| {
            // record the runtime context span for trace querying
            EnvFilter::try_new(format!(
                "quickwit={level},quickwit[{RUNTIME_CONTEXT_SPAN}]=trace"
            ))
        })
        .expect("Failed to set up OTLP tracing filter.");
    Ok(tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(env_filter))
}

pub fn setup_lambda_tracer(level: Level) -> anyhow::Result<()> {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let registry = tracing_subscriber::registry();
    let build_info = BuildInfo::get();
    if let (Some(ot_url), Some(ot_auth)) = (
        OPENTELEMETRY_URL.clone(),
        OPENTELEMETRY_AUTHORIZATION.clone(),
    ) {
        registry
            .with(fmt_layer(level))
            .with(otlp_layer(ot_url, ot_auth, level, build_info)?)
            .try_init()
            .context("failed to register tracing subscriber")?;
    } else {
        registry
            .with(fmt_layer(level))
            .try_init()
            .context("failed to register tracing subscriber")?;
    }
    Ok(())
}

pub fn flush_tracer() {
    if let Some(tracer_provider) = TRACER_PROVIDER.get() {
        debug!("flush tracers");
        for res in tracer_provider.force_flush() {
            if let Err(err) = res {
                debug!(?err, "failed to flush tracer");
            }
        }
    }
}
