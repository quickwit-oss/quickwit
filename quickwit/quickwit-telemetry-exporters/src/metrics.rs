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

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Context;
use metrics_exporter_otel::OpenTelemetryRecorder;
use metrics_exporter_prometheus::{
    Matcher, PrometheusBuilder, PrometheusHandle, PrometheusRecorder,
};
use metrics_util::layers::FanoutBuilder;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::{MetricExporter, Protocol as OtlpWireProtocol, WithExportConfig};
use opentelemetry_sdk::metrics::{SdkMeterProvider, Temporality};

use crate::config::{OtlpExporterConfig, OtlpProtocol, quickwit_resource};

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

impl OtlpProtocol {
    pub(crate) fn metric_exporter(
        &self,
        temporality: Temporality,
    ) -> anyhow::Result<MetricExporter> {
        match self {
            OtlpProtocol::Grpc => MetricExporter::builder()
                .with_tonic()
                .with_temporality(temporality)
                .build(),
            OtlpProtocol::HttpProtobuf => MetricExporter::builder()
                .with_http()
                .with_temporality(temporality)
                .with_protocol(OtlpWireProtocol::HttpBinary)
                .build(),
            OtlpProtocol::HttpJson => MetricExporter::builder()
                .with_http()
                .with_temporality(temporality)
                .with_protocol(OtlpWireProtocol::HttpJson)
                .build(),
        }
        .context("failed to initialize OTLP metrics exporter")
    }
}

/// Sets up the global metrics recorder.
pub(crate) fn init_metrics_provider(
    service_version: &str,
    otlp_config: &OtlpExporterConfig,
) -> anyhow::Result<Option<SdkMeterProvider>> {
    let prometheus_recorder = build_prometheus_recorder()?;

    let (recorder, meter_provider) = if otlp_config.is_enabled() {
        let (otlp_recorder, meter_provider) = build_otlp_recorder(service_version, otlp_config)?;
        let recorder = FanoutBuilder::default()
            .add_recorder(prometheus_recorder)
            .add_recorder(otlp_recorder)
            .build();
        (recorder, Some(meter_provider))
    } else {
        let recorder = FanoutBuilder::default()
            .add_recorder(prometheus_recorder)
            .build();
        (recorder, None)
    };

    metrics::set_global_recorder(recorder)
        .map_err(|_| anyhow::anyhow!("failed to install global metrics recorder"))?;
    quickwit_metrics::describe_metrics();

    Ok(meter_provider)
}

fn build_prometheus_recorder() -> anyhow::Result<PrometheusRecorder> {
    let mut prometheus_builder = PrometheusBuilder::new();
    for (name, buckets) in quickwit_metrics::histogram_buckets() {
        prometheus_builder = prometheus_builder
            .set_buckets_for_metric(Matcher::Full(name.to_string()), &buckets)
            .with_context(|| {
                format!("failed to configure Prometheus histogram buckets for `{name}`")
            })?;
    }
    let prometheus_recorder = prometheus_builder.build_recorder();
    let prometheus_handle = prometheus_recorder.handle();
    PROMETHEUS_HANDLE
        .set(prometheus_handle.clone())
        .map_err(|_| anyhow::anyhow!("Prometheus metrics renderer is already installed"))?;
    spawn_prometheus_upkeep(prometheus_handle).map_err(anyhow::Error::msg)?;
    Ok(prometheus_recorder)
}

pub fn metrics_text_payload() -> Result<String, String> {
    let handle = PROMETHEUS_HANDLE
        .get()
        .ok_or_else(|| "Prometheus metrics rendering is not installed yet".to_string())?;
    Ok(handle.render())
}

fn spawn_prometheus_upkeep(handle: PrometheusHandle) -> Result<(), String> {
    // Quickwit serves the existing `/metrics` route itself, so we build only the
    // Prometheus recorder instead of using the exporter's HTTP listener. That lower-level
    // API does not spawn the upkeep task that periodically drains histogram buffers.
    std::thread::Builder::new()
        .name("telemetry-exporter-prometheus-upkeep".to_string())
        .spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(5));
                handle.run_upkeep();
            }
        })
        .map(|_| ())
        .map_err(|error| format!("failed to spawn Prometheus metrics upkeep thread: {error}"))
}

fn build_otlp_recorder(
    service_version: &str,
    otlp_config: &OtlpExporterConfig,
) -> anyhow::Result<(OpenTelemetryRecorder, SdkMeterProvider)> {
    let metrics_protocol = otlp_config.metrics_protocol()?;
    let temporality = otlp_config.metrics_temporality()?;
    let metric_exporter = metrics_protocol.metric_exporter(temporality)?;
    let metrics_provider = SdkMeterProvider::builder()
        .with_resource(quickwit_resource(service_version))
        .with_periodic_exporter(metric_exporter)
        .build();
    let meter = metrics_provider.meter("quickwit");

    let recorder = OpenTelemetryRecorder::new(meter);
    for (name, buckets) in quickwit_metrics::histogram_buckets() {
        recorder.set_histogram_bounds(&metrics::KeyName::from(name), buckets);
    }
    Ok((recorder, metrics_provider))
}

#[cfg(test)]
mod tests {
    use metrics::with_local_recorder;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use quickwit_metrics::{gauge, labels};

    use super::*;

    #[test]
    fn metrics_text_payload_renders_prometheus_handle() {
        let recorder = PrometheusBuilder::new().build_recorder();
        PROMETHEUS_HANDLE
            .set(recorder.handle())
            .expect("Prometheus handle should be set once");

        with_local_recorder(&recorder, || {
            let info_metric = gauge!(
                name: "prometheus_payload_info",
                description: "prometheus payload info",
                subsystem: "",
            );
            quickwit_metrics::describe_metrics();
            gauge!(parent: info_metric, labels: [labels!("version" => "test")]).set(1.0);
        });

        let payload = metrics_text_payload().expect("Prometheus payload should render");
        assert!(payload.contains("# HELP quickwit_prometheus_payload_info"));
        assert!(payload.contains(r#"quickwit_prometheus_payload_info{version="test"} 1"#));
    }
}
