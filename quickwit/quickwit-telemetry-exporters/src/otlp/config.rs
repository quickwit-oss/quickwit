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

use std::str::FromStr;

use anyhow::Context;
use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use quickwit_common::{get_bool_from_env, get_from_env, get_from_env_opt};

pub const QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER_ENV_KEY: &str =
    "QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER";

const OTEL_EXPORTER_OTLP_PROTOCOL_ENV_KEY: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";
const OTEL_EXPORTER_OTLP_TRACES_PROTOCOL_ENV_KEY: &str = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL";
const OTEL_EXPORTER_OTLP_LOGS_PROTOCOL_ENV_KEY: &str = "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL";
const OTEL_EXPORTER_OTLP_METRICS_PROTOCOL_ENV_KEY: &str = "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL";

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
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn traces_protocol(&self) -> anyhow::Result<OtlpProtocol> {
        self.resolve_protocol(OTEL_EXPORTER_OTLP_TRACES_PROTOCOL_ENV_KEY)
    }

    pub(crate) fn logs_protocol(&self) -> anyhow::Result<OtlpProtocol> {
        self.resolve_protocol(OTEL_EXPORTER_OTLP_LOGS_PROTOCOL_ENV_KEY)
    }

    pub(crate) fn metrics_protocol(&self) -> anyhow::Result<OtlpProtocol> {
        self.resolve_protocol(OTEL_EXPORTER_OTLP_METRICS_PROTOCOL_ENV_KEY)
    }

    fn resolve_protocol(&self, exporter_protocol_env_key: &str) -> anyhow::Result<OtlpProtocol> {
        let exporter_protocol = get_from_env_opt::<String>(exporter_protocol_env_key, false);
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
    use std::ffi::OsString;

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
        }
    }

    struct EnvVarGuard {
        key: &'static str,
        previous_value: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let guard = Self {
                key,
                previous_value: std::env::var_os(key),
            };
            unsafe { std::env::set_var(key, value) };
            guard
        }

        fn remove(key: &'static str) -> Self {
            let guard = Self {
                key,
                previous_value: std::env::var_os(key),
            };
            unsafe { std::env::remove_var(key) };
            guard
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous_value) = &self.previous_value {
                unsafe { std::env::set_var(self.key, previous_value) };
            } else {
                unsafe { std::env::remove_var(self.key) };
            }
        }
    }

    #[test]
    fn test_otlp_exporter_config_uses_signal_specific_protocol() {
        const TEST_PROTOCOL_ENV_KEY: &str = "QW_TEST_OTLP_SIGNAL_PROTOCOL";

        let _guard = EnvVarGuard::set(TEST_PROTOCOL_ENV_KEY, "http/json");

        assert_eq!(
            otlp_exporter_config("grpc")
                .resolve_protocol(TEST_PROTOCOL_ENV_KEY)
                .unwrap(),
            OtlpProtocol::HttpJson
        );
    }

    #[test]
    fn test_otlp_exporter_config_falls_back_to_default_protocol() {
        const TEST_PROTOCOL_ENV_KEY: &str = "QW_TEST_OTLP_DEFAULT_PROTOCOL_FALLBACK";

        let _guard = EnvVarGuard::remove(TEST_PROTOCOL_ENV_KEY);

        assert_eq!(
            otlp_exporter_config("http/protobuf")
                .resolve_protocol(TEST_PROTOCOL_ENV_KEY)
                .unwrap(),
            OtlpProtocol::HttpProtobuf
        );
    }

    #[test]
    fn test_otlp_exporter_config_signal_protocol_error_names_signal_env_var() {
        const TEST_PROTOCOL_ENV_KEY: &str = "QW_TEST_OTLP_INVALID_SIGNAL_PROTOCOL";

        let _guard = EnvVarGuard::set(TEST_PROTOCOL_ENV_KEY, "http/xml");

        let error = otlp_exporter_config("grpc")
            .resolve_protocol(TEST_PROTOCOL_ENV_KEY)
            .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains(TEST_PROTOCOL_ENV_KEY));
        assert!(error.contains("unsupported OTLP protocol `http/xml`"));
    }

    #[test]
    fn test_otlp_exporter_config_default_protocol_error_names_default_env_var() {
        const TEST_PROTOCOL_ENV_KEY: &str = "QW_TEST_OTLP_INVALID_DEFAULT_PROTOCOL";

        let _guard = EnvVarGuard::remove(TEST_PROTOCOL_ENV_KEY);

        let error = otlp_exporter_config("http/xml")
            .resolve_protocol(TEST_PROTOCOL_ENV_KEY)
            .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains(OTEL_EXPORTER_OTLP_PROTOCOL_ENV_KEY));
        assert!(error.contains("unsupported OTLP protocol `http/xml`"));
    }
}
