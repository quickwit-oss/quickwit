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

use std::path::PathBuf;

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct IntakeConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "default_logs_endpoint")]
    pub logs_endpoint: String,
    #[serde(default = "default_metrics_endpoint")]
    pub metrics_endpoint: String,
    #[serde(default = "default_traces_endpoint")]
    pub traces_endpoint: String,
    /// Organization identifier passed to transforms that call external services.
    #[serde(default = "default_org_id")]
    pub org_id: String,
    /// Base URL of byoc-ingest-metadata-svc (e.g. "https://metadata.example.com").
    #[serde(default = "default_metadata_svc_url")]
    pub metadata_svc_url: String,
}

impl Default for IntakeConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            logs_endpoint: default_logs_endpoint(),
            metrics_endpoint: default_metrics_endpoint(),
            traces_endpoint: default_traces_endpoint(),
            org_id: default_org_id(),
            metadata_svc_url: default_metadata_svc_url(),
        }
    }
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("qwdata/intake")
}

fn default_logs_endpoint() -> String {
    "http://127.0.0.1:7280/api/datadog/v1/byoc/logs".to_string()
}

fn default_metrics_endpoint() -> String {
    "http://127.0.0.1:7280/api/datadog/v1/byoc/metrics".to_string()
}

fn default_traces_endpoint() -> String {
    "http://127.0.0.1:7280/api/datadog/v1/byoc/traces".to_string()
}

fn default_org_id() -> String {
    "default".to_string()
}

fn default_metadata_svc_url() -> String {
    "http://localhost:9999".to_string()
}
