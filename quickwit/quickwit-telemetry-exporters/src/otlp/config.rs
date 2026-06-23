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

use std::collections::HashMap;
use std::str::FromStr;

use anyhow::Context;
use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::Temporality;
use quickwit_common::datadog_api_key::resolve_dd_api_key_from_env;
use quickwit_common::{get_bool_from_env, get_from_env, get_from_env_opt};
use secrecy::{ExposeSecret, SecretString};
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};

pub const QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER_ENV_KEY: &str =
    "QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER";

const OTEL_EXPORTER_OTLP_PROTOCOL_ENV_KEY: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";
const OTEL_EXPORTER_OTLP_TRACES_PROTOCOL_ENV_KEY: &str = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL";
const OTEL_EXPORTER_OTLP_LOGS_PROTOCOL_ENV_KEY: &str = "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL";
const OTEL_EXPORTER_OTLP_METRICS_PROTOCOL_ENV_KEY: &str = "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL";
const OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE_ENV_KEY: &str =
    "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE";
const BYOC_TELEMETRY_ENABLED_ENV_KEY: &str = "BYOC_TELEMETRY_ENABLED";
const DD_API_KEY_HTTP_HEADER_NAME: &str = "DD-API-KEY";
const DD_API_KEY_GRPC_METADATA_KEY: &str = "dd-api-key";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OtlpProtocol {
    Grpc,
    HttpProtobuf,
    HttpJson,
}

impl FromStr for OtlpProtocol {
    type Err = anyhow::Error;

    fn from_str(protocol_str: &str) -> anyhow::Result<Self> {
        const OTLP_PROTOCOL_GRPC: &str = "grpc";
        const OTLP_PROTOCOL_HTTP_PROTOBUF: &str = "http/protobuf";
        const OTLP_PROTOCOL_HTTP_JSON: &str = "http/json";

        match protocol_str {
            OTLP_PROTOCOL_GRPC => Ok(OtlpProtocol::Grpc),
            OTLP_PROTOCOL_HTTP_PROTOBUF => Ok(OtlpProtocol::HttpProtobuf),
            OTLP_PROTOCOL_HTTP_JSON => Ok(OtlpProtocol::HttpJson),
            other => anyhow::bail!(
                "unsupported OTLP protocol `{other}`, supported values are \
                 `{OTLP_PROTOCOL_GRPC}`, `{OTLP_PROTOCOL_HTTP_PROTOBUF}` and \
                 `{OTLP_PROTOCOL_HTTP_JSON}`"
            ),
        }
    }
}

pub(crate) struct OtlpExporterConfig {
    enabled: bool,
    default_protocol: String,
    headers: OtlpHeaders,
}

#[derive(Default)]
pub(crate) struct OtlpHeaders {
    dd_api_key: Option<SecretString>,
}

impl OtlpExporterConfig {
    pub(crate) fn load_from_env() -> Self {
        OtlpExporterConfig {
            enabled: get_bool_from_env(QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER_ENV_KEY, false),
            default_protocol: get_from_env(
                OTEL_EXPORTER_OTLP_PROTOCOL_ENV_KEY,
                "grpc".to_string(),
                false,
            ),
            headers: OtlpHeaders::load_from_env(),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn traces_protocol(&self) -> anyhow::Result<OtlpProtocol> {
        self.resolve_protocol_from_env(OTEL_EXPORTER_OTLP_TRACES_PROTOCOL_ENV_KEY)
    }

    pub(crate) fn logs_protocol(&self) -> anyhow::Result<OtlpProtocol> {
        self.resolve_protocol_from_env(OTEL_EXPORTER_OTLP_LOGS_PROTOCOL_ENV_KEY)
    }

    pub(crate) fn metrics_protocol(&self) -> anyhow::Result<OtlpProtocol> {
        self.resolve_protocol_from_env(OTEL_EXPORTER_OTLP_METRICS_PROTOCOL_ENV_KEY)
    }

    pub(crate) fn metrics_temporality(&self) -> anyhow::Result<Temporality> {
        let temporality = get_from_env_opt::<String>(
            OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE_ENV_KEY,
            false,
        );
        temporality
            .as_deref()
            .map(|temporality_str| {
                OtlpMetricsTemporality::from_str(temporality_str).with_context(|| {
                    format!(
                        "failed to parse environment variable \
                         `{OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE_ENV_KEY}`"
                    )
                })
            })
            .transpose()
            .map(|temporality| {
                temporality
                    .map(Temporality::from)
                    .unwrap_or(Temporality::Cumulative)
            })
    }

    pub(crate) fn headers(&self) -> &OtlpHeaders {
        &self.headers
    }

    fn resolve_protocol_from_env(
        &self,
        exporter_protocol_env_key: &str,
    ) -> anyhow::Result<OtlpProtocol> {
        let exporter_protocol = get_from_env_opt::<String>(exporter_protocol_env_key, false);
        self.resolve_protocol(exporter_protocol_env_key, exporter_protocol)
    }

    fn resolve_protocol(
        &self,
        exporter_protocol_env_key: &str,
        exporter_protocol: Option<String>,
    ) -> anyhow::Result<OtlpProtocol> {
        let (protocol, env_key) = if let Some(protocol) = exporter_protocol {
            (protocol, exporter_protocol_env_key)
        } else {
            (
                self.default_protocol.clone(),
                OTEL_EXPORTER_OTLP_PROTOCOL_ENV_KEY,
            )
        };

        OtlpProtocol::from_str(&protocol)
            .with_context(|| format!("failed to parse environment variable `{env_key}`"))
    }
}

impl OtlpHeaders {
    fn load_from_env() -> Self {
        let dd_api_key = get_bool_from_env(BYOC_TELEMETRY_ENABLED_ENV_KEY, false)
            .then(resolve_dd_api_key_from_env)
            .flatten();
        Self::load(dd_api_key)
    }

    fn load(dd_api_key: Option<SecretString>) -> Self {
        Self { dd_api_key }
    }

    pub(crate) fn http_headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        if let Some(dd_api_key) = &self.dd_api_key {
            headers.insert(
                DD_API_KEY_HTTP_HEADER_NAME.to_string(),
                dd_api_key.expose_secret().to_string(),
            );
        }
        headers
    }

    pub(crate) fn grpc_metadata(&self) -> anyhow::Result<MetadataMap> {
        let mut metadata = MetadataMap::new();
        if let Some(dd_api_key) = self.dd_api_key.as_ref() {
            let metadata_key = MetadataKey::from_static(DD_API_KEY_GRPC_METADATA_KEY);
            let metadata_value =
                MetadataValue::from_str(dd_api_key.expose_secret()).with_context(|| {
                    format!("failed to parse `{DD_API_KEY_GRPC_METADATA_KEY}` metadata")
                })?;
            metadata.insert(metadata_key, metadata_value);
        }
        Ok(metadata)
    }
}

struct OtlpMetricsTemporality(Temporality);

impl FromStr for OtlpMetricsTemporality {
    type Err = anyhow::Error;

    fn from_str(temporality_str: &str) -> anyhow::Result<Self> {
        const TEMPORALITY_DELTA: &str = "delta";
        const TEMPORALITY_LOWMEMORY: &str = "lowmemory";
        const TEMPORALITY_CUMULATIVE: &str = "cumulative";

        match temporality_str {
            TEMPORALITY_DELTA => Ok(Self(Temporality::Delta)),
            TEMPORALITY_LOWMEMORY => Ok(Self(Temporality::LowMemory)),
            TEMPORALITY_CUMULATIVE => Ok(Self(Temporality::Cumulative)),
            other => anyhow::bail!(
                "unsupported OTLP metrics temporality `{other}`, supported values are \
                 `{TEMPORALITY_DELTA}`, `{TEMPORALITY_LOWMEMORY}` and `{TEMPORALITY_CUMULATIVE}`"
            ),
        }
    }
}

impl From<OtlpMetricsTemporality> for Temporality {
    fn from(temporality: OtlpMetricsTemporality) -> Self {
        temporality.0
    }
}

pub(crate) fn quickwit_resource(service_version: &str) -> Resource {
    Resource::builder()
        .with_service_name("quickwit")
        .with_attribute(KeyValue::new(
            "service.version",
            service_version.to_string(),
        ))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otlp_protocol_from_str() {
        assert_eq!(OtlpProtocol::from_str("grpc").unwrap(), OtlpProtocol::Grpc);
        assert_eq!(
            OtlpProtocol::from_str("http/protobuf").unwrap(),
            OtlpProtocol::HttpProtobuf
        );
        assert_eq!(
            OtlpProtocol::from_str("http/json").unwrap(),
            OtlpProtocol::HttpJson
        );
        assert!(OtlpProtocol::from_str("http/xml").is_err());
    }
    fn otlp_exporter_config(default_protocol: &str) -> OtlpExporterConfig {
        OtlpExporterConfig {
            enabled: true,
            default_protocol: default_protocol.to_string(),
            headers: OtlpHeaders::default(),
        }
    }

    #[test]
    fn test_otlp_exporter_config_uses_signal_specific_protocol() {
        const TEST_PROTOCOL_ENV_KEY: &str = "QW_TEST_OTLP_SIGNAL_PROTOCOL";

        assert_eq!(
            otlp_exporter_config("grpc")
                .resolve_protocol(TEST_PROTOCOL_ENV_KEY, Some("http/json".to_string()))
                .unwrap(),
            OtlpProtocol::HttpJson
        );
    }

    #[test]
    fn test_otlp_exporter_config_falls_back_to_default_protocol() {
        const TEST_PROTOCOL_ENV_KEY: &str = "QW_TEST_OTLP_DEFAULT_PROTOCOL_FALLBACK";

        assert_eq!(
            otlp_exporter_config("http/protobuf")
                .resolve_protocol(TEST_PROTOCOL_ENV_KEY, None)
                .unwrap(),
            OtlpProtocol::HttpProtobuf
        );
    }

    #[test]
    fn test_otlp_headers_without_dd_api_key_are_empty() {
        let headers = OtlpHeaders::load(None);

        assert!(headers.dd_api_key.is_none());
        assert!(headers.http_headers().is_empty());
        assert!(headers.grpc_metadata().unwrap().is_empty());
    }

    #[test]
    fn test_otlp_exporter_config_signal_protocol_error_names_signal_env_var() {
        const TEST_PROTOCOL_ENV_KEY: &str = "QW_TEST_OTLP_INVALID_SIGNAL_PROTOCOL";

        let error = otlp_exporter_config("grpc")
            .resolve_protocol(TEST_PROTOCOL_ENV_KEY, Some("http/xml".to_string()))
            .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains(TEST_PROTOCOL_ENV_KEY));
        assert!(error.contains("unsupported OTLP protocol `http/xml`"));
    }

    #[test]
    fn test_otlp_exporter_config_default_protocol_error_names_default_env_var() {
        const TEST_PROTOCOL_ENV_KEY: &str = "QW_TEST_OTLP_INVALID_DEFAULT_PROTOCOL";

        let error = otlp_exporter_config("http/xml")
            .resolve_protocol(TEST_PROTOCOL_ENV_KEY, None)
            .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains(OTEL_EXPORTER_OTLP_PROTOCOL_ENV_KEY));
        assert!(error.contains("unsupported OTLP protocol `http/xml`"));
    }

    #[test]
    fn test_otlp_metrics_temporality_from_str() {
        assert_eq!(
            Temporality::from(OtlpMetricsTemporality::from_str("delta").unwrap()),
            Temporality::Delta
        );
        assert_eq!(
            Temporality::from(OtlpMetricsTemporality::from_str("lowmemory").unwrap()),
            Temporality::LowMemory
        );
        assert_eq!(
            Temporality::from(OtlpMetricsTemporality::from_str("cumulative").unwrap()),
            Temporality::Cumulative
        );
        assert!(OtlpMetricsTemporality::from_str("invalid").is_err());
    }

    #[test]
    fn test_otlp_headers_include_dd_api_key() {
        let headers = OtlpHeaders::load(Some(SecretString::from("api-key".to_string())));

        let http_headers = headers.http_headers();
        assert_eq!(
            http_headers
                .get(DD_API_KEY_HTTP_HEADER_NAME)
                .map(String::as_str),
            Some("api-key")
        );

        let grpc_metadata = headers.grpc_metadata().unwrap();
        assert_eq!(
            grpc_metadata
                .get(DD_API_KEY_GRPC_METADATA_KEY)
                .unwrap()
                .to_str()
                .unwrap(),
            "api-key"
        );
    }
}
