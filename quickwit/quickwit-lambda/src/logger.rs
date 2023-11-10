// Copyright (C) 2023 Quickwit, Inc.
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

use std::env;

use anyhow::Context;
use once_cell::sync::OnceCell;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::trace::{BatchConfig, TracerProvider};
use opentelemetry::sdk::{trace, Resource};
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use quickwit_serve::BuildInfo;
use tracing::{debug, Level};
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

static TRACER_PROVIDER: OnceCell<Option<TracerProvider>> = OnceCell::new();

fn setup_logging_and_tracing(
    level: Level,
    ansi: bool,
    build_info: &BuildInfo,
) -> anyhow::Result<()> {
    let env_filter = env::var("RUST_LOG")
        .map(|_| EnvFilter::from_default_env())
        .or_else(|_| EnvFilter::try_new(format!("quickwit={level}")))
        .context("Failed to set up tracing env filter.")?;
    global::set_text_map_propagator(TraceContextPropagator::new());
    let registry = tracing_subscriber::registry().with(env_filter);
    let event_format = tracing_subscriber::fmt::format()
        .with_target(true)
        .with_timer(
            // We do not rely on the Rfc3339 implementation, because it has a nanosecond precision.
            // See discussion here: https://github.com/time-rs/time/discussions/418
            UtcTime::new(
                time::format_description::parse(
                    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
                )
                .expect("Time format invalid."),
            ),
        );
    let otlp_config = (
        std::env::var("QW_LAMBDA_OPENTELEMETRY_URL"),
        std::env::var("QW_LAMBDA_OPENTELEMETRY_AUTHORIZATION"),
    );
    if let (Ok(ot_url), Ok(ot_auth)) = otlp_config {
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
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(otlp_exporter)
            .with_trace_config(trace_config)
            .with_batch_config(batch_config)
            .install_batch(opentelemetry::runtime::Tokio)
            .context("Failed to initialize OpenTelemetry OTLP exporter.")?;
        TRACER_PROVIDER.set(tracer.provider()).unwrap();
        registry
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(event_format)
                    .with_ansi(ansi),
            )
            .try_init()
            .context("Failed to set up tracing.")?;
    } else {
        registry
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(event_format)
                    .with_ansi(ansi),
            )
            .try_init()
            .context("Failed to set up tracing.")?;
    }
    Ok(())
}

pub fn setup_lambda_tracer() -> anyhow::Result<()> {
    setup_logging_and_tracing(Level::DEBUG, false, BuildInfo::get())
}

pub fn flush_tracer() {
    if let Some(Some(tracer_provider)) = TRACER_PROVIDER.get() {
        debug!("Flush tracers");
        for res in tracer_provider.force_flush() {
            if let Err(err) = res {
                debug!(err=?err, "Failed to flush tracer");
            }
        }
    }
}
