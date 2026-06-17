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
    LogExporter, Protocol as OtlpWireProtocol, WithExportConfig, WithHttpConfig, WithTonicConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;

use crate::otlp::{OtlpExporterConfig, OtlpProtocol};

impl OtlpProtocol {
    pub(crate) fn log_exporter(&self) -> anyhow::Result<LogExporter> {
        match self {
            OtlpProtocol::Grpc => LogExporter::builder()
                .with_tonic()
                .with_retry_policy(super::RETRY_POLICY)
                .build(),
            OtlpProtocol::HttpProtobuf => LogExporter::builder()
                .with_http()
                .with_retry_policy(super::RETRY_POLICY)
                .with_protocol(OtlpWireProtocol::HttpBinary)
                .build(),
            OtlpProtocol::HttpJson => LogExporter::builder()
                .with_http()
                .with_retry_policy(super::RETRY_POLICY)
                .with_protocol(OtlpWireProtocol::HttpJson)
                .build(),
        }
        .context("failed to initialize OTLP logs exporter")
    }
}

pub(crate) fn init_logger_provider(
    otlp_config: &OtlpExporterConfig,
    resource: Resource,
) -> anyhow::Result<SdkLoggerProvider> {
    let logs_protocol = otlp_config.logs_protocol()?;
    let log_exporter = logs_protocol.log_exporter()?;
    Ok(SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(log_exporter)
        .build())
}
