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

use std::collections::{BTreeMap, HashSet};
use std::env;
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::machine::{available_memory_num_bytes, cpu_count_bucket, memory_bucket};
use crate::usage::ApiKind;

/// Version of the payload format. Payloads without one are version 1.
pub(crate) const TELEMETRY_SCHEMA_VERSION: u32 = 2;

/// Represents the payload of the request sent with telemetry requests.
#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryPayload {
    pub schema_version: u32,
    /// Client information. See details in `[ClientInformation]`.
    pub client_info: ClientInfo,
    /// Quickwit information. See details in `[QuickwitInfo]`.
    pub quickwit_info: QuickwitTelemetryInfo,
    pub events: Vec<EventWithTimestamp>,
    /// Represents the number of events that where drops due to the
    /// combination of the `TELEMETRY_PUSH_COOLDOWN` and `MAX_EVENT_IN_QUEUE`.
    pub num_dropped_events: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventWithTimestamp {
    /// Unix time in seconds.
    pub unixtime: u64,
    /// Telemetry event.
    #[serde(flatten)]
    pub event: TelemetryEvent,
}

/// Returns the number of seconds elapsed since UNIX_EPOCH.
///
/// If the system clock is set before 1970, returns 0.
fn unixtime() -> u64 {
    match UNIX_EPOCH.elapsed() {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0u64,
    }
}

impl From<TelemetryEvent> for EventWithTimestamp {
    fn from(event: TelemetryEvent) -> Self {
        EventWithTimestamp {
            unixtime: unixtime(),
            event,
        }
    }
}

/// Represents a telemetry event sent to the telemetry relay for usage information.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum TelemetryEvent {
    RunCommand,
    /// EndCommand (with the return code).
    EndCommand {
        return_code: i32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        api_usage: Option<BTreeMap<ApiKind, String>>,
    },
    /// Event sent every 12h to signal the server is running.
    Running {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        api_usage: Option<BTreeMap<ApiKind, String>>,
    },
    /// UI index.html was requested.
    UiIndexPageLoad,
    /// The install ID was just created.
    Install,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientInfo {
    session_uuid: uuid::Uuid,
    os: String,
    arch: String,
    hashed_host_username: String,
    kubernetes: bool,
    cpus: String,
    memory: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildTelemetryInfo {
    pub commit: String,
    pub target: String,
    pub release: bool,
}

impl Default for BuildTelemetryInfo {
    fn default() -> Self {
        Self {
            commit: "unknown".to_string(),
            target: "unknown".to_string(),
            release: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuickwitTelemetryInfo {
    pub version: String,
    pub services: HashSet<String>,
    pub features: HashSet<QuickwitFeature>,
    pub build: BuildTelemetryInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub install_uuid: Option<Uuid>,
}

impl QuickwitTelemetryInfo {
    pub fn new(services: HashSet<String>, features: HashSet<QuickwitFeature>) -> Self {
        Self {
            features,
            services,
            ..Default::default()
        }
    }
}

impl Default for QuickwitTelemetryInfo {
    fn default() -> Self {
        Self {
            features: HashSet::new(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            services: HashSet::new(),
            build: BuildTelemetryInfo::default(),
            install_uuid: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum QuickwitFeature {
    AzureStorage,
    DocsClustering,
    FileBackedMetastore,
    FileStorage,
    GoogleCloudStorage,
    IngestV1,
    IngestV2,
    Jaeger,
    LambdaLeafSearch,
    Otlp,
    PostgresqMetastore,
    S3CustomEndpoint,
    S3FlavorDigitalOcean,
    S3FlavorGarage,
    S3FlavorGcs,
    S3FlavorMinio,
    S3Storage,
    SplitCache,
    Tls,
}

fn hashed_host_username() -> String {
    let hostname = hostname::get()
        .map(|hostname| hostname.to_string_lossy().to_string())
        .unwrap_or_default();
    let username = username::get_user_name().unwrap_or_default();
    let hashed_value = format!("{hostname}:{username}");
    let digest = md5::compute(hashed_value.as_bytes());
    format!("{digest:x}")
}

impl Default for ClientInfo {
    fn default() -> ClientInfo {
        ClientInfo {
            session_uuid: Uuid::new_v4(),
            os: env::consts::OS.to_string(),
            arch: env::consts::ARCH.to_string(),
            hashed_host_username: hashed_host_username(),
            kubernetes: std::env::var_os("KUBERNETES_SERVICE_HOST").is_some(),
            cpus: cpu_count_bucket(quickwit_common::num_cpus()).to_string(),
            memory: memory_bucket(available_memory_num_bytes()).to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn test_serialize_features_as_expected() {
        // These names are part of the telemetry payload documented in `docs/telemetry.md`.
        let features = [
            QuickwitFeature::AzureStorage,
            QuickwitFeature::DocsClustering,
            QuickwitFeature::FileBackedMetastore,
            QuickwitFeature::FileStorage,
            QuickwitFeature::GoogleCloudStorage,
            QuickwitFeature::IngestV1,
            QuickwitFeature::IngestV2,
            QuickwitFeature::Jaeger,
            QuickwitFeature::LambdaLeafSearch,
            QuickwitFeature::Otlp,
            QuickwitFeature::PostgresqMetastore,
            QuickwitFeature::S3CustomEndpoint,
            QuickwitFeature::S3FlavorDigitalOcean,
            QuickwitFeature::S3FlavorGarage,
            QuickwitFeature::S3FlavorGcs,
            QuickwitFeature::S3FlavorMinio,
            QuickwitFeature::S3Storage,
            QuickwitFeature::SplitCache,
            QuickwitFeature::Tls,
        ];
        let json = serde_json::to_string(&features).unwrap();
        assert_eq!(
            json,
            r#"["azure_storage","docs_clustering","file_backed_metastore","file_storage","google_cloud_storage","ingest_v1","ingest_v2","jaeger","lambda_leaf_search","otlp","postgresq_metastore","s3_custom_endpoint","s3_flavor_digital_ocean","s3_flavor_garage","s3_flavor_gcs","s3_flavor_minio","s3_storage","split_cache","tls"]"#
        );
    }

    #[test]
    fn test_serialize_payload_as_expected() {
        let event = EventWithTimestamp {
            unixtime: 0,
            event: TelemetryEvent::EndCommand {
                return_code: 0,
                api_usage: None,
            },
        };
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(
            json,
            r#"{"unixtime":0,"type":"end_command","return_code":0}"#
        );
    }

    #[test]
    fn test_serialize_activity_as_expected() {
        let event = EventWithTimestamp {
            unixtime: 0,
            event: TelemetryEvent::Running {
                api_usage: Some(BTreeMap::from([(ApiKind::NativeSearch, "1-9".to_string())])),
            },
        };
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(
            json,
            r#"{"unixtime":0,"type":"running","api_usage":{"native_search":"1-9"}}"#
        );
    }
}
