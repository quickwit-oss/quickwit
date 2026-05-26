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
const DD_API_KEY_ENV_KEY: &str = "DD_API_KEY";
const DD_API_KEY_FILE_ENV_KEY: &str = "DD_API_KEY_FILE";
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
            headers: OtlpHeaders::load(),
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

impl OtlpHeaders {
    fn load() -> Self {
        let dd_api_key = if get_bool_from_env(BYOC_TELEMETRY_ENABLED_ENV_KEY, false) {
            get_from_env_opt::<String>(DD_API_KEY_ENV_KEY, true)
                .map(|api_key| (DD_API_KEY_ENV_KEY, api_key))
                .or_else(|| {
                    get_from_env_opt::<String>(DD_API_KEY_FILE_ENV_KEY, false).and_then(|path| {
                        std::fs::read_to_string(&path)
                            .map(|api_key| (DD_API_KEY_FILE_ENV_KEY, api_key))
                            .map_err(|error| {
                                tracing::warn!(
                                    path = %path,
                                    error = %error,
                                    "failed to read DD_API_KEY_FILE"
                                );
                                error
                            })
                            .ok()
                    })
                })
                .and_then(|(env_key, api_key)| {
                    let api_key = api_key.trim();
                    if api_key.is_empty() {
                        tracing::warn!(
                            env_key = %env_key,
                            "Datadog API key is configured but empty"
                        );
                        None
                    } else {
                        Some(SecretString::from(api_key.to_string()))
                    }
                })
        } else {
            None
        };
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
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::{Mutex, MutexGuard};

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

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

    fn lock_env() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap()
    }

    fn temp_file_path(test_name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "quickwit-telemetry-exporters-{test_name}-{}",
            std::process::id()
        ))
    }

    fn assert_dd_api_key_headers(headers: &OtlpHeaders, expected_api_key: &str) {
        let http_headers = headers.http_headers();
        assert_eq!(
            http_headers
                .get(DD_API_KEY_HTTP_HEADER_NAME)
                .map(String::as_str),
            Some(expected_api_key)
        );

        let grpc_metadata = headers.grpc_metadata().unwrap();
        assert_eq!(
            grpc_metadata
                .get(DD_API_KEY_GRPC_METADATA_KEY)
                .unwrap()
                .to_str()
                .unwrap(),
            expected_api_key
        );
    }

    fn dd_api_key(headers: &OtlpHeaders) -> Option<&str> {
        headers.dd_api_key.as_ref().map(ExposeSecret::expose_secret)
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
    fn test_otlp_exporter_config_byoc_disabled_has_no_dd_api_key() {
        let _lock = lock_env();
        let _byoc_telemetry_enabled_guard = EnvVarGuard::remove(BYOC_TELEMETRY_ENABLED_ENV_KEY);
        let _dd_api_key_guard = EnvVarGuard::set(DD_API_KEY_ENV_KEY, "env-api-key");
        let _dd_api_key_file_guard = EnvVarGuard::remove(DD_API_KEY_FILE_ENV_KEY);

        let config = OtlpExporterConfig::load_from_env();
        assert!(config.headers.dd_api_key.is_none());
        assert!(config.headers().http_headers().is_empty());
        assert!(config.headers().grpc_metadata().unwrap().is_empty());
    }

    #[test]
    fn test_otlp_exporter_config_uses_dd_api_key_env_when_byoc_enabled() {
        let _lock = lock_env();
        let _byoc_telemetry_enabled_guard =
            EnvVarGuard::set(BYOC_TELEMETRY_ENABLED_ENV_KEY, "true");
        let _dd_api_key_guard = EnvVarGuard::set(DD_API_KEY_ENV_KEY, " env-api-key\n");
        let _dd_api_key_file_guard = EnvVarGuard::remove(DD_API_KEY_FILE_ENV_KEY);

        let config = OtlpExporterConfig::load_from_env();
        assert_eq!(dd_api_key(&config.headers), Some("env-api-key"));
        assert_dd_api_key_headers(config.headers(), "env-api-key");
    }

    #[test]
    fn test_otlp_exporter_config_ignores_empty_dd_api_key_env() {
        let _lock = lock_env();
        let _byoc_telemetry_enabled_guard =
            EnvVarGuard::set(BYOC_TELEMETRY_ENABLED_ENV_KEY, "true");
        let _dd_api_key_guard = EnvVarGuard::set(DD_API_KEY_ENV_KEY, " \n");
        let _dd_api_key_file_guard = EnvVarGuard::remove(DD_API_KEY_FILE_ENV_KEY);

        let config = OtlpExporterConfig::load_from_env();
        assert!(config.headers.dd_api_key.is_none());
        assert!(config.headers().http_headers().is_empty());
        assert!(config.headers().grpc_metadata().unwrap().is_empty());
    }

    #[test]
    fn test_otlp_exporter_config_reads_dd_api_key_file_when_byoc_enabled() {
        let _lock = lock_env();
        let path = temp_file_path("dd-api-key-file");
        std::fs::remove_file(&path).ok();
        std::fs::write(&path, " file-api-key\n").unwrap();

        let _byoc_telemetry_enabled_guard =
            EnvVarGuard::set(BYOC_TELEMETRY_ENABLED_ENV_KEY, "true");
        let _dd_api_key_guard = EnvVarGuard::remove(DD_API_KEY_ENV_KEY);
        let _dd_api_key_file_guard =
            EnvVarGuard::set(DD_API_KEY_FILE_ENV_KEY, path.to_str().unwrap());

        let config = OtlpExporterConfig::load_from_env();
        assert_eq!(dd_api_key(&config.headers), Some("file-api-key"));
        assert_dd_api_key_headers(config.headers(), "file-api-key");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_otlp_exporter_config_ignores_empty_dd_api_key_file() {
        let _lock = lock_env();
        let path = temp_file_path("empty-dd-api-key-file");
        std::fs::remove_file(&path).ok();
        std::fs::write(&path, " \n").unwrap();

        let _byoc_telemetry_enabled_guard =
            EnvVarGuard::set(BYOC_TELEMETRY_ENABLED_ENV_KEY, "true");
        let _dd_api_key_guard = EnvVarGuard::remove(DD_API_KEY_ENV_KEY);
        let _dd_api_key_file_guard =
            EnvVarGuard::set(DD_API_KEY_FILE_ENV_KEY, path.to_str().unwrap());

        let config = OtlpExporterConfig::load_from_env();
        assert!(config.headers.dd_api_key.is_none());
        assert!(config.headers().http_headers().is_empty());
        assert!(config.headers().grpc_metadata().unwrap().is_empty());

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_otlp_exporter_config_prefers_dd_api_key_env_over_file() {
        let _lock = lock_env();
        let path = temp_file_path("dd-api-key-env-over-file");
        std::fs::remove_file(&path).ok();
        std::fs::write(&path, "file-api-key\n").unwrap();

        let _byoc_telemetry_enabled_guard =
            EnvVarGuard::set(BYOC_TELEMETRY_ENABLED_ENV_KEY, "true");
        let _dd_api_key_guard = EnvVarGuard::set(DD_API_KEY_ENV_KEY, "env-api-key");
        let _dd_api_key_file_guard =
            EnvVarGuard::set(DD_API_KEY_FILE_ENV_KEY, path.to_str().unwrap());

        let config = OtlpExporterConfig::load_from_env();
        assert_eq!(dd_api_key(&config.headers), Some("env-api-key"));
        assert_dd_api_key_headers(config.headers(), "env-api-key");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_otlp_exporter_config_ignores_unreadable_dd_api_key_file() {
        let _lock = lock_env();
        let path = temp_file_path("missing-dd-api-key-file");
        std::fs::remove_file(&path).ok();

        let _byoc_telemetry_enabled_guard =
            EnvVarGuard::set(BYOC_TELEMETRY_ENABLED_ENV_KEY, "true");
        let _dd_api_key_guard = EnvVarGuard::remove(DD_API_KEY_ENV_KEY);
        let _dd_api_key_file_guard =
            EnvVarGuard::set(DD_API_KEY_FILE_ENV_KEY, path.to_str().unwrap());

        let config = OtlpExporterConfig::load_from_env();
        assert!(config.headers.dd_api_key.is_none());
        assert!(config.headers().http_headers().is_empty());
        assert!(config.headers().grpc_metadata().unwrap().is_empty());
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
}
