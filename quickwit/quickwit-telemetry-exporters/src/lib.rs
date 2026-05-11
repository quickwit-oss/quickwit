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

#![deny(clippy::disallowed_methods)]

use std::sync::Arc;

use anyhow::Context;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider as SdkMetricsProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
#[cfg(feature = "tokio-console")]
use quickwit_common::get_bool_from_env;
use tracing::Level;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

mod logs;
mod metrics;
mod otlp;
pub mod prometheus;

#[cfg(feature = "tokio-console")]
const QW_ENABLE_TOKIO_CONSOLE_ENV_KEY: &str = "QW_ENABLE_TOKIO_CONSOLE";

pub type EnvFilterReloadFn = Arc<dyn Fn(&str) -> anyhow::Result<()> + Send + Sync>;

pub fn do_nothing_env_filter_reload_fn() -> EnvFilterReloadFn {
    Arc::new(|_| Ok(()))
}

pub struct TelemetryHandle {
    env_filter_reload_fn: EnvFilterReloadFn,
    tracer_provider: Option<SdkTracerProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    meter_provider: Option<SdkMetricsProvider>,
}

impl TelemetryHandle {
    pub fn env_filter_reload_fn(&self) -> EnvFilterReloadFn {
        self.env_filter_reload_fn.clone()
    }

    pub fn shutdown(self) -> anyhow::Result<()> {
        if let Some(tracer_provider) = self.tracer_provider {
            tracer_provider
                .shutdown()
                .context("failed to shutdown OpenTelemetry tracer provider")?;
        }
        if let Some(logger_provider) = self.logger_provider {
            logger_provider
                .shutdown()
                .context("failed to shutdown OpenTelemetry logger provider")?;
        }
        if let Some(meter_provider) = self.meter_provider {
            meter_provider
                .shutdown()
                .context("failed to shutdown OpenTelemetry meter provider")?;
        }
        Ok(())
    }
}

/// Load the default logging filter from the environment. The filter can later
/// be updated using the result callback of [init_telemetry].
pub(crate) fn startup_env_filter(level: Level) -> anyhow::Result<EnvFilter> {
    let env_filter = std::env::var("RUST_LOG")
        .map(|_| EnvFilter::from_default_env())
        .or_else(|_| EnvFilter::try_new(format!("quickwit={level},tantivy=WARN")))
        .context("failed to set up tracing env filter")?;
    Ok(env_filter)
}

pub(crate) type ReloadLayer =
    tracing_subscriber::reload::Layer<EnvFilter, tracing_subscriber::Registry>;

/// Initializes logging/tracing/metrics providers for the process.
///
/// NOTE: this function must be called before any metric is emitted so metric handles
/// are registered against the production recorder instead of a noop/default
/// recorder.
pub fn init_telemetry(
    service_version: &str,
    level: Level,
    ansi_colors: bool,
) -> anyhow::Result<TelemetryHandle> {
    let otlp_config = otlp::OtlpExporterConfig::load_from_env();

    let meter_provider = metrics::init_metrics_provider(service_version, &otlp_config)?;

    #[cfg(feature = "tokio-console")]
    {
        if get_bool_from_env(QW_ENABLE_TOKIO_CONSOLE_ENV_KEY, false) {
            console_subscriber::init();
            return Ok(TelemetryHandle {
                env_filter_reload_fn: do_nothing_env_filter_reload_fn(),
                tracer_provider: None,
                logger_provider: None,
                meter_provider,
            });
        }
    }
    global::set_text_map_propagator(TraceContextPropagator::new());

    let event_format = logs::EventFormat::get_from_env();
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
    let registry = logs::jemalloc_profiled::configure_registry(
        registry,
        event_format,
        fmt_fields,
        ansi_colors,
        level,
        reloadable_env_filter,
    )?;

    let env_filter_reload_fn: EnvFilterReloadFn = Arc::new(move |env_filter_def: &str| {
        let new_env_filter = EnvFilter::try_new(env_filter_def)?;
        reload_handle.reload(new_env_filter)?;
        Ok(())
    });

    // Note on disabling ANSI characters: setting the ansi boolean on event format is insufficient.
    // It is thus set on layers, see https://github.com/tokio-rs/tracing/issues/1817
    let telemetry_handle = if otlp_config.is_enabled() {
        let resource = otlp::quickwit_resource(service_version);

        let tracer_provider = otlp::traces::init_tracer_provider(&otlp_config, resource.clone())?;
        let logger_provider = otlp::logs::init_logger_provider(&otlp_config, resource)?;

        let tracer = tracer_provider.tracer("quickwit");
        let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        // Bridge between tracing logs and otel tracing events
        let logs_otel_layer = OpenTelemetryTracingBridge::new(&logger_provider);

        registry
            .with(telemetry_layer)
            .with(logs_otel_layer)
            .try_init()
            .context("failed to register tracing subscriber")?;

        TelemetryHandle {
            env_filter_reload_fn,
            tracer_provider: Some(tracer_provider),
            logger_provider: Some(logger_provider),
            meter_provider,
        }
    } else {
        registry
            .try_init()
            .context("failed to register tracing subscriber")?;
        TelemetryHandle {
            env_filter_reload_fn,
            tracer_provider: None,
            logger_provider: None,
            meter_provider,
        }
    };

    Ok(telemetry_handle)
}
