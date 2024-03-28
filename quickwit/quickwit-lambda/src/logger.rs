// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use anyhow::Context;
use once_cell::sync::OnceCell;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::trace::{BatchConfig, TracerProvider};
use opentelemetry::sdk::{trace, Resource};
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use quickwit_serve::BuildInfo;
use tracing::{debug, Level};
use tracing_subscriber::fmt::format::{FmtSpan, JsonFields};
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Layer};

use crate::environment::{
    ENABLE_VERBOSE_JSON_LOGS, OPENTELEMETRY_AUTHORIZATION, OPENTELEMETRY_URL,
};

static TRACER_PROVIDER: OnceCell<Option<TracerProvider>> = OnceCell::new();
pub(crate) const RUNTIME_CONTEXT_SPAN: &str = "runtime_context";

fn fmt_env_filter(level: Level) -> EnvFilter {
    let default_filter = format!("quickwit={level}")
        .parse()
        .expect("Invalid default filter");
    EnvFilter::builder()
        .with_default_directive(default_filter)
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
) -> impl Layer<S>
where
    S: for<'a> LookupSpan<'a>,
    S: tracing::Subscriber,
{
    let headers = std::collections::HashMap::from([("Authorization".into(), ot_auth)]);
    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .http()
        .with_endpoint(ot_url)
        .with_headers(headers);
    // In debug mode, Quickwit can generate a lot of spans, and the default queue size of 2048
    // is too small.
    let batch_config = BatchConfig::default().with_max_queue_size(32768);
    let trace_config = trace::config().with_resource(Resource::new([
        KeyValue::new("service.name", "quickwit"),
        KeyValue::new("service.version", build_info.version.clone()),
    ]));
    let env_filter = std::env::var(EnvFilter::DEFAULT_ENV)
        .map(|_| EnvFilter::from_default_env())
        .or_else(|_| {
            // record the runtime context span for trace querying
            EnvFilter::try_new(format!(
                "quickwit={level},quickwit[{RUNTIME_CONTEXT_SPAN}]=trace"
            ))
        })
        .expect("Failed to set up OTLP tracing filter.");
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
        .with_trace_config(trace_config)
        .with_batch_config(batch_config)
        .install_batch(opentelemetry::runtime::Tokio)
        .expect("Failed to initialize OpenTelemetry OTLP exporter.");
    TRACER_PROVIDER.set(tracer.provider()).unwrap();
    tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(env_filter)
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
            .with(otlp_layer(ot_url, ot_auth, level, build_info))
            .try_init()
            .context("Failed to set up tracing.")?;
    } else {
        registry
            .with(fmt_layer(level))
            .try_init()
            .context("Failed to set up tracing.")?;
    }
    Ok(())
}

pub fn flush_tracer() {
    if let Some(Some(tracer_provider)) = TRACER_PROVIDER.get() {
        debug!("flush tracers");
        for res in tracer_provider.force_flush() {
            if let Err(err) = res {
                debug!(err=?err, "Failed to flush tracer");
            }
        }
    }
}
