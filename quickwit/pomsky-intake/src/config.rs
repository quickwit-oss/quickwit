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
use std::time::Duration;

use anyhow::ensure;
use serde::Deserialize;

const DEFAULT_DD_SITE: &str = "datadoghq.com";

const HOST_TAGS_DEFAULT_POLL_INTERVAL_SECS: u64 = 15;
const HOST_TAGS_DEFAULT_FETCH_TIMEOUT_SECS: u64 = 10;
const HOST_TAGS_DEFAULT_TTL_MIN_SECS: u64 = 900; // 15 minutes
const HOST_TAGS_DEFAULT_TTL_MAX_SECS: u64 = 3600; // 60 minutes
const HOST_TAGS_DEFAULT_STALE_THRESHOLD_HOURS: u64 = 24;

const HOST_TAGS_API_PATH: &str = "/api/unstable/byoc/ingest/metadata/host-tags";

const DUAL_SHIP_DEFAULT_POLL_INTERVAL_SECS: u64 = 15;
const DUAL_SHIP_DEFAULT_FETCH_TIMEOUT_SECS: u64 = 10;

#[derive(Clone, Debug, Deserialize)]
pub struct IntakeConfig {
    /// Datadog site (e.g. `datadoghq.com`, `datad0g.com`). Overridden by
    /// the `DD_SITE` environment variable when set. Shared across all
    /// Datadog-backed pollers (host tags, and future ones).
    #[serde(rename = "site", default = "default_dd_site")]
    pub dd_site: String,

    /// Datadog API key. Resolved via [`Self::resolve_dd_api_key`], which
    /// prefers `DD_API_KEY_FILE` (file contents) over `DD_API_KEY` (env)
    /// over this config field. Shared across all Datadog-backed pollers.
    #[serde(rename = "api_key", default)]
    pub dd_api_key: Option<String>,

    /// Path to the data directory for storing Vector's internal state.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "default_logs_endpoint")]
    pub logs_endpoint: String,
    #[serde(default = "default_metrics_endpoint")]
    pub metrics_endpoint: String,
    #[serde(default = "default_sketches_endpoint")]
    pub sketches_endpoint: String,
    #[serde(default = "default_traces_endpoint")]
    pub traces_endpoint: String,
    /// Path to the CSV file used by the `metric_metadata` transform to
    /// persist its known-metrics set across restarts.
    #[serde(default = "default_metric_metadata_persist_file_path")]
    pub metric_metadata_persist_file_path: PathBuf,

    #[serde(default)]
    pub host_tags: HostTagsConfig,

    #[serde(default)]
    pub dual_ship: DualShipConfig,
}

impl Default for IntakeConfig {
    fn default() -> Self {
        Self {
            dd_site: default_dd_site(),
            dd_api_key: None,
            data_dir: default_data_dir(),
            logs_endpoint: default_logs_endpoint(),
            metrics_endpoint: default_metrics_endpoint(),
            sketches_endpoint: default_sketches_endpoint(),
            traces_endpoint: default_traces_endpoint(),
            metric_metadata_persist_file_path: default_metric_metadata_persist_file_path(),
            host_tags: HostTagsConfig::default(),
            dual_ship: DualShipConfig::default(),
        }
    }
}

impl IntakeConfig {
    /// Resolves the DD site. The `DD_SITE` env var takes precedence over
    /// the config field.
    pub fn resolve_dd_site(&self) -> String {
        std::env::var("DD_SITE").unwrap_or_else(|_| self.dd_site.clone())
    }

    /// Resolves the DD API key. Precedence: the file pointed to by
    /// `DD_API_KEY_FILE`, then `DD_API_KEY`, then the config field.
    pub fn resolve_dd_api_key(&self) -> Option<String> {
        if let Ok(path) = std::env::var("DD_API_KEY_FILE") {
            match std::fs::read_to_string(&path) {
                Ok(contents) => return Some(contents.trim().to_string()),
                Err(error) => {
                    tracing::warn!(path, %error, "failed to read DD_API_KEY_FILE");
                }
            }
        }
        std::env::var("DD_API_KEY")
            .ok()
            .or_else(|| self.dd_api_key.clone())
    }

    /// Returns an error on the first invariant violation. Cheap presence
    /// checks only — the file behind `DD_API_KEY_FILE` is read once at
    /// resolution time in [`crate::run_intake`], not here.
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            std::env::var_os("DD_API_KEY_FILE").is_some()
                || std::env::var_os("DD_API_KEY").is_some()
                || self.dd_api_key.is_some(),
            "dd_api_key must be set via the DD_API_KEY_FILE environment variable, the DD_API_KEY \
             environment variable, or config",
        );
        self.host_tags.validate()?;
        self.dual_ship.validate()?;
        Ok(())
    }
}

fn default_dd_site() -> String {
    DEFAULT_DD_SITE.to_string()
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

fn default_sketches_endpoint() -> String {
    "http://127.0.0.1:7280/api/datadog/v1/byoc/sketches".to_string()
}

fn default_traces_endpoint() -> String {
    "http://127.0.0.1:7280/api/datadog/v1/byoc/traces".to_string()
}

fn default_metric_metadata_persist_file_path() -> PathBuf {
    PathBuf::from("qwdata/intake/metric_metadata_known.csv")
}

/// Configuration for the host-tags enrichment poller.
#[derive(Clone, Debug, Deserialize)]
pub struct HostTagsConfig {
    /// How often to poll the metadata service, in seconds.
    #[serde(default = "host_tags_default_poll_interval_secs")]
    pub poll_interval_secs: u64,

    /// HTTP request timeout for a single metadata service fetch, in seconds.
    /// Must be strictly less than `poll_interval_secs` so a slow fetch
    /// cannot overlap the next poll cycle.
    #[serde(default = "host_tags_default_fetch_timeout_secs")]
    pub fetch_timeout_secs: u64,

    /// Minimum TTL before a host's tags are re-fetched, in seconds.
    #[serde(default = "host_tags_default_ttl_min_secs")]
    pub ttl_min_secs: u64,

    /// Maximum TTL before a host's tags are re-fetched, in seconds.
    #[serde(default = "host_tags_default_ttl_max_secs")]
    pub ttl_max_secs: u64,

    /// How long past expiry before a host entry is evicted from the store,
    /// in hours. Must be strictly greater than `ttl_max_secs / 3600`.
    ///
    /// Entries expired for less than this threshold are kept so the pipeline
    /// can continue serving their stale tags while a refresh is in flight.
    /// Only entries that have been absent from traffic for this long get
    /// evicted.
    #[serde(default = "host_tags_default_stale_threshold_hours")]
    pub stale_threshold_hours: u64,

    /// Path to the NDJSON cache file for persisting host tags across
    /// restarts. When set, the poller loads entries on startup (including
    /// expired ones for stale-serving) and rewrites the file after each
    /// successful fetch or eviction.
    #[serde(default)]
    pub cache_path: Option<PathBuf>,
}

fn host_tags_default_poll_interval_secs() -> u64 {
    HOST_TAGS_DEFAULT_POLL_INTERVAL_SECS
}

fn host_tags_default_fetch_timeout_secs() -> u64 {
    HOST_TAGS_DEFAULT_FETCH_TIMEOUT_SECS
}

fn host_tags_default_ttl_min_secs() -> u64 {
    HOST_TAGS_DEFAULT_TTL_MIN_SECS
}

fn host_tags_default_ttl_max_secs() -> u64 {
    HOST_TAGS_DEFAULT_TTL_MAX_SECS
}

fn host_tags_default_stale_threshold_hours() -> u64 {
    HOST_TAGS_DEFAULT_STALE_THRESHOLD_HOURS
}

impl HostTagsConfig {
    /// Builds the full metadata service URL: `https://{dd_site}{api_path}`.
    pub fn metadata_service_url(&self, dd_site: &str) -> String {
        let site = dd_site.trim_end_matches('/');
        format!("https://{site}{HOST_TAGS_API_PATH}")
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }

    pub fn fetch_timeout(&self) -> Duration {
        Duration::from_secs(self.fetch_timeout_secs)
    }

    pub fn ttl_min(&self) -> Duration {
        Duration::from_secs(self.ttl_min_secs)
    }

    pub fn ttl_max(&self) -> Duration {
        Duration::from_secs(self.ttl_max_secs)
    }

    pub fn stale_threshold(&self) -> Duration {
        Duration::from_secs(self.stale_threshold_hours * 3600)
    }

    /// Checks that the config satisfies the invariants the poller relies
    /// on. Called from [`IntakeConfig::validate`] at startup.
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.ttl_min_secs <= self.ttl_max_secs,
            "host_tags.ttl_min_secs ({}) must be less than or equal to host_tags.ttl_max_secs ({})",
            self.ttl_min_secs,
            self.ttl_max_secs,
        );
        ensure!(
            self.fetch_timeout_secs < self.poll_interval_secs,
            "host_tags.fetch_timeout_secs ({}) must be strictly less than \
             host_tags.poll_interval_secs ({}); otherwise a slow fetch can overlap the next poll \
             cycle",
            self.fetch_timeout_secs,
            self.poll_interval_secs,
        );
        ensure!(
            self.stale_threshold_hours * 3600 > self.ttl_max_secs,
            "host_tags.stale_threshold_hours ({0}h = {1}s) must be strictly greater than \
             host_tags.ttl_max_secs ({2}s)",
            self.stale_threshold_hours,
            self.stale_threshold_hours * 3600,
            self.ttl_max_secs,
        );
        Ok(())
    }
}

impl Default for HostTagsConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: HOST_TAGS_DEFAULT_POLL_INTERVAL_SECS,
            fetch_timeout_secs: HOST_TAGS_DEFAULT_FETCH_TIMEOUT_SECS,
            ttl_min_secs: HOST_TAGS_DEFAULT_TTL_MIN_SECS,
            ttl_max_secs: HOST_TAGS_DEFAULT_TTL_MAX_SECS,
            stale_threshold_hours: HOST_TAGS_DEFAULT_STALE_THRESHOLD_HOURS,
            cache_path: None,
        }
    }
}

/// Configuration for the dual-ship metric routing component.
///
/// The poller fetches `/api/unstable/byoc/ingest/metadata/dual-shipped-metrics`
/// from the same `dd_site` used elsewhere in the intake. The same
/// `persist_file_path` is plumbed into both the `metric_dual_ship` Vector
/// transform (for the load-on-startup path) and the spawned poller (for
/// periodic writes).
#[derive(Clone, Debug, Deserialize)]
pub struct DualShipConfig {
    /// How often to poll the metadata service, in seconds.
    #[serde(default = "dual_ship_default_poll_interval_secs")]
    pub poll_interval_secs: u64,

    /// HTTP request timeout for a single metadata service fetch, in seconds.
    /// Must be strictly less than `poll_interval_secs` so a slow fetch
    /// cannot overlap the next poll cycle.
    #[serde(default = "dual_ship_default_fetch_timeout_secs")]
    pub fetch_timeout_secs: u64,

    /// Path to the CSV file used to persist the in-memory routing map
    /// between restarts. The watermark sidecar lives at
    /// `{persist_file_path}.watermark`.
    #[serde(default = "default_dual_ship_persist_file_path")]
    pub persist_file_path: PathBuf,
}

fn dual_ship_default_poll_interval_secs() -> u64 {
    DUAL_SHIP_DEFAULT_POLL_INTERVAL_SECS
}

fn dual_ship_default_fetch_timeout_secs() -> u64 {
    DUAL_SHIP_DEFAULT_FETCH_TIMEOUT_SECS
}

fn default_dual_ship_persist_file_path() -> PathBuf {
    PathBuf::from("qwdata/intake/metrics_to_saas.csv")
}

impl DualShipConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }

    pub fn fetch_timeout(&self) -> Duration {
        Duration::from_secs(self.fetch_timeout_secs)
    }

    /// Builds the metadata service base URL: `https://{dd_site}`. The
    /// dual-ship endpoint path is appended by the fetcher.
    pub fn metadata_service_url(&self, dd_site: &str) -> String {
        let site = dd_site.trim_end_matches('/');
        format!("https://{site}")
    }

    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.fetch_timeout_secs < self.poll_interval_secs,
            "dual_ship.fetch_timeout_secs ({}) must be strictly less than \
             dual_ship.poll_interval_secs ({}); otherwise a slow fetch can overlap the next poll \
             cycle",
            self.fetch_timeout_secs,
            self.poll_interval_secs,
        );
        Ok(())
    }
}

impl Default for DualShipConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: DUAL_SHIP_DEFAULT_POLL_INTERVAL_SECS,
            fetch_timeout_secs: DUAL_SHIP_DEFAULT_FETCH_TIMEOUT_SECS,
            persist_file_path: default_dual_ship_persist_file_path(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_service_url_from_site() {
        let config = HostTagsConfig::default();
        assert_eq!(
            config.metadata_service_url("datad0g.com"),
            "https://datad0g.com/api/unstable/byoc/ingest/metadata/host-tags",
        );
    }

    #[test]
    fn test_metadata_service_url_trims_trailing_slash() {
        let config = HostTagsConfig::default();
        assert_eq!(
            config.metadata_service_url("datadoghq.com/"),
            "https://datadoghq.com/api/unstable/byoc/ingest/metadata/host-tags",
        );
    }

    #[test]
    fn test_validate_rejects_inverted_ttl_bounds() {
        let config = HostTagsConfig {
            ttl_min_secs: 7200,
            ttl_max_secs: 60,
            ..HostTagsConfig::default()
        };
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("ttl_min_secs"), "got: {error}");
        assert!(error.contains("ttl_max_secs"), "got: {error}");
    }

    #[test]
    fn test_validate_accepts_valid_ttl_bounds() {
        let config = HostTagsConfig {
            ttl_min_secs: 60,
            ttl_max_secs: 120,
            ..HostTagsConfig::default()
        };
        config.validate().unwrap();
    }

    #[test]
    fn test_validate_accepts_equal_ttl_bounds() {
        let config = HostTagsConfig {
            ttl_min_secs: 300,
            ttl_max_secs: 300,
            ..HostTagsConfig::default()
        };
        config.validate().unwrap();
    }

    #[test]
    fn test_validate_rejects_fetch_timeout_exceeding_poll_interval() {
        let config = HostTagsConfig {
            poll_interval_secs: 5,
            fetch_timeout_secs: 10,
            ..HostTagsConfig::default()
        };
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("fetch_timeout_secs"), "got: {error}");
        assert!(error.contains("poll_interval_secs"), "got: {error}");
    }

    #[test]
    fn test_validate_rejects_fetch_timeout_equal_to_poll_interval() {
        let config = HostTagsConfig {
            poll_interval_secs: 15,
            fetch_timeout_secs: 15,
            ..HostTagsConfig::default()
        };
        config.validate().unwrap_err();
    }

    #[test]
    fn test_validate_accepts_fetch_timeout_less_than_poll_interval() {
        let config = HostTagsConfig {
            poll_interval_secs: 30,
            fetch_timeout_secs: 5,
            ..HostTagsConfig::default()
        };
        config.validate().unwrap();
    }

    #[test]
    fn test_dual_ship_metadata_service_url_strips_trailing_slash() {
        let config = DualShipConfig::default();
        assert_eq!(
            config.metadata_service_url("datad0g.com/"),
            "https://datad0g.com",
        );
    }

    #[test]
    fn test_dual_ship_validate_rejects_fetch_timeout_exceeding_poll_interval() {
        let config = DualShipConfig {
            poll_interval_secs: 5,
            fetch_timeout_secs: 10,
            ..DualShipConfig::default()
        };
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("fetch_timeout_secs"));
        assert!(error.contains("poll_interval_secs"));
    }

    #[test]
    fn test_dual_ship_validate_accepts_valid_intervals() {
        DualShipConfig {
            poll_interval_secs: 30,
            fetch_timeout_secs: 5,
            ..DualShipConfig::default()
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn test_dual_ship_default_passes_validation() {
        DualShipConfig::default().validate().unwrap();
    }
}
