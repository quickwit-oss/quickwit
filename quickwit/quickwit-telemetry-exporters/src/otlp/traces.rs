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
use opentelemetry_otlp::{
    Protocol as OtlpWireProtocol, SpanExporter, WithExportConfig, WithHttpConfig, WithTonicConfig,
};
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::trace::{BatchConfigBuilder, SdkTracerProvider};
use opentelemetry_sdk::{Resource, runtime};

use crate::otlp::{OtlpExporterConfig, OtlpProtocol};

impl OtlpProtocol {
    pub(crate) fn span_exporter(&self) -> anyhow::Result<SpanExporter> {
        match self {
            OtlpProtocol::Grpc => SpanExporter::builder()
                .with_tonic()
                .with_retry_policy(super::RETRY_POLICY)
                .build(),
            OtlpProtocol::HttpProtobuf => SpanExporter::builder()
                .with_http()
                .with_retry_policy(super::RETRY_POLICY)
                .with_protocol(OtlpWireProtocol::HttpBinary)
                .build(),
            OtlpProtocol::HttpJson => SpanExporter::builder()
                .with_http()
                .with_retry_policy(super::RETRY_POLICY)
                .with_protocol(OtlpWireProtocol::HttpJson)
                .build(),
        }
        .context("failed to initialize OTLP traces exporter")
    }
}

/// Builds the OTLP tracer provider.
pub(crate) fn init_tracer_provider(
    otlp_config: &OtlpExporterConfig,
    resource: Resource,
) -> anyhow::Result<SdkTracerProvider> {
    let traces_protocol = otlp_config.traces_protocol()?;
    let span_exporter = traces_protocol.span_exporter()?;
    let span_processor = BatchSpanProcessor::builder(span_exporter, runtime::Tokio)
        .with_batch_config(
            BatchConfigBuilder::default()
                // Quickwit can generate a lot of spans, especially in debug mode, and the
                // default queue size of 2048 is too small.
                .with_max_queue_size(32_768)
                .build(),
        )
        .build();

    Ok(SdkTracerProvider::builder()
        .with_span_processor(span_processor)
        .with_resource(resource)
        .build())
}
