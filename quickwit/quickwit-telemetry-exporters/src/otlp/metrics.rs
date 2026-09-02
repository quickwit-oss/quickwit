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
use metrics_opentelemetry::{OpenTelemetryMetrics, OpenTelemetryRecorder};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::{
    MetricExporter, Protocol as OtlpWireProtocol, WithExportConfig, WithHttpConfig, WithTonicConfig,
};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::periodic_reader_with_async_runtime::PeriodicReader;
use opentelemetry_sdk::runtime;

use crate::otlp::{OtlpExporterConfig, OtlpProtocol, quickwit_resource};

impl OtlpProtocol {
    pub(crate) fn metric_exporter(&self) -> anyhow::Result<MetricExporter> {
        match self {
            OtlpProtocol::Grpc => MetricExporter::builder()
                .with_tonic()
                .with_retry_policy(super::RETRY_POLICY)
                .build(),
            OtlpProtocol::HttpProtobuf => MetricExporter::builder()
                .with_http()
                .with_retry_policy(super::RETRY_POLICY)
                .with_protocol(OtlpWireProtocol::HttpBinary)
                .build(),
            OtlpProtocol::HttpJson => MetricExporter::builder()
                .with_http()
                .with_retry_policy(super::RETRY_POLICY)
                .with_protocol(OtlpWireProtocol::HttpJson)
                .build(),
        }
        .context("failed to initialize OTLP metrics exporter")
    }
}

/// Builds the OTLP metrics recorder and its meter provider.
pub(crate) fn build_recorder(
    service_version: &str,
    otlp_config: &OtlpExporterConfig,
) -> anyhow::Result<(OpenTelemetryRecorder, SdkMeterProvider)> {
    let metrics_protocol = otlp_config.metrics_protocol()?;
    let metric_exporter = metrics_protocol.metric_exporter()?;
    let metric_reader = PeriodicReader::builder(metric_exporter, runtime::Tokio).build();
    let metrics_provider = SdkMeterProvider::builder()
        .with_resource(quickwit_resource(service_version))
        .with_reader(metric_reader)
        .build();
    let meter = metrics_provider.meter("quickwit");

    let otel_metrics = OpenTelemetryMetrics::new(meter);
    let recorder = OpenTelemetryRecorder::new(otel_metrics);
    for (name, buckets) in quickwit_metrics::histogram_buckets() {
        recorder.set_histogram_bounds(std::iter::once(metrics::KeyName::from(name)), &buckets);
    }
    Ok((recorder, metrics_provider))
}
