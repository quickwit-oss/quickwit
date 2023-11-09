// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

mod serialize;

use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, ensure};
use bytesize::ByteSize;
use quickwit_common::net::HostAddr;
use quickwit_common::uri::Uri;
use quickwit_proto::indexing::CpuCapacity;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::node_config::serialize::load_node_config_with_env;
use crate::service::QuickwitService;
use crate::storage_config::StorageConfigs;
use crate::{ConfigFormat, MetastoreConfigs};

pub const DEFAULT_QW_CONFIG_PATH: &str = "config/quickwit.yaml";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerConfig {
    #[serde(default = "IndexerConfig::default_split_store_max_num_bytes")]
    pub split_store_max_num_bytes: ByteSize,
    #[serde(default = "IndexerConfig::default_split_store_max_num_splits")]
    pub split_store_max_num_splits: usize,
    #[serde(default = "IndexerConfig::default_max_concurrent_split_uploads")]
    pub max_concurrent_split_uploads: usize,
    /// Enables the OpenTelemetry exporter endpoint to ingest logs and traces via the OpenTelemetry
    /// Protocol (OTLP).
    #[serde(default = "IndexerConfig::default_enable_otlp_endpoint")]
    pub enable_otlp_endpoint: bool,
    #[serde(default = "IndexerConfig::default_enable_cooperative_indexing")]
    pub enable_cooperative_indexing: bool,
    #[serde(default = "IndexerConfig::default_cpu_capacity")]
    pub cpu_capacity: CpuCapacity,
}

impl IndexerConfig {
    fn default_enable_cooperative_indexing() -> bool {
        false
    }

    fn default_enable_otlp_endpoint() -> bool {
        #[cfg(any(test, feature = "testsuite"))]
        {
            false
        }
        #[cfg(not(any(test, feature = "testsuite")))]
        {
            true
        }
    }

    fn default_max_concurrent_split_uploads() -> usize {
        12
    }

    pub fn default_split_store_max_num_bytes() -> ByteSize {
        ByteSize::gb(100)
    }

    pub fn default_split_store_max_num_splits() -> usize {
        1_000
    }

    fn default_cpu_capacity() -> CpuCapacity {
        CpuCapacity::one_cpu_thread() * (num_cpus::get() as u32)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> anyhow::Result<Self> {
        use quickwit_proto::indexing::PIPELINE_FULL_CAPACITY;
        let indexer_config = IndexerConfig {
            enable_cooperative_indexing: false,
            enable_otlp_endpoint: true,
            split_store_max_num_bytes: ByteSize::mb(1),
            split_store_max_num_splits: 3,
            max_concurrent_split_uploads: 4,
            cpu_capacity: PIPELINE_FULL_CAPACITY * 4u32,
        };
        Ok(indexer_config)
    }
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            enable_cooperative_indexing: Self::default_enable_cooperative_indexing(),
            enable_otlp_endpoint: Self::default_enable_otlp_endpoint(),
            split_store_max_num_bytes: Self::default_split_store_max_num_bytes(),
            split_store_max_num_splits: Self::default_split_store_max_num_splits(),
            max_concurrent_split_uploads: Self::default_max_concurrent_split_uploads(),
            cpu_capacity: Self::default_cpu_capacity(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitCacheLimits {
    pub max_num_bytes: ByteSize,
    #[serde(default = "SplitCacheLimits::default_max_num_splits")]
    pub max_num_splits: NonZeroU32,
    #[serde(default = "SplitCacheLimits::default_num_concurrent_downloads")]
    pub num_concurrent_downloads: NonZeroU32,
}

impl SplitCacheLimits {
    fn default_max_num_splits() -> NonZeroU32 {
        NonZeroU32::new(10_000).unwrap()
    }

    fn default_num_concurrent_downloads() -> NonZeroU32 {
        NonZeroU32::new(1).unwrap()
    }
}

impl Default for SplitCacheLimits {
    fn default() -> SplitCacheLimits {
        SplitCacheLimits {
            max_num_bytes: ByteSize::gb(1),
            max_num_splits: NonZeroU32::new(100).unwrap(),
            num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SearcherConfig {
    pub aggregation_memory_limit: ByteSize,
    pub aggregation_bucket_limit: u32,
    pub fast_field_cache_capacity: ByteSize,
    pub split_footer_cache_capacity: ByteSize,
    pub partial_request_cache_capacity: ByteSize,
    pub max_num_concurrent_split_searches: usize,
    pub max_num_concurrent_split_streams: usize,
    // Strangely, if None, this will also have the effect of not forwarding
    // to searcher.
    // TODO document and fix if necessary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_cache: Option<SplitCacheLimits>,
}

impl Default for SearcherConfig {
    fn default() -> Self {
        Self {
            fast_field_cache_capacity: ByteSize::gb(1),
            split_footer_cache_capacity: ByteSize::mb(500),
            partial_request_cache_capacity: ByteSize::mb(64),
            max_num_concurrent_split_streams: 100,
            max_num_concurrent_split_searches: 100,
            aggregation_memory_limit: ByteSize::mb(500),
            aggregation_bucket_limit: 65000,
            split_cache: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct IngestApiConfig {
    pub max_queue_memory_usage: ByteSize,
    pub max_queue_disk_usage: ByteSize,
    pub replication_factor: usize,
    pub content_length_limit: u64,
}

impl Default for IngestApiConfig {
    fn default() -> Self {
        Self {
            max_queue_memory_usage: ByteSize::gib(2), // TODO maybe we want more?
            max_queue_disk_usage: ByteSize::gib(4),   // TODO maybe we want more?
            replication_factor: 1,
            content_length_limit: ByteSize::mib(10).as_u64(),
        }
    }
}

impl IngestApiConfig {
    pub fn replication_factor(&self) -> anyhow::Result<NonZeroUsize> {
        if let Ok(replication_factor_str) = env::var("QW_INGEST_REPLICATION_FACTOR") {
            let replication_factor = match replication_factor_str.trim() {
                "1" => 1,
                "2" => 2,
                _ => bail!(
                    "replication factor must be either 1 or 2, got `{replication_factor_str}`"
                ),
            };
            return Ok(NonZeroUsize::new(replication_factor)
                .expect("replication factor should be either 1 or 2"));
        }
        ensure!(
            self.replication_factor >= 1 && self.replication_factor <= 2,
            "replication factor must be either 1 or 2, got `{}`",
            self.replication_factor
        );
        Ok(NonZeroUsize::new(self.replication_factor)
            .expect("replication factor should be either 1 or 2"))
    }

    fn validate(&self) -> anyhow::Result<()> {
        self.replication_factor()?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JaegerConfig {
    /// Enables the gRPC endpoint that allows the Jaeger Query Service to connect and retrieve
    /// traces.
    #[serde(default = "JaegerConfig::default_enable_endpoint")]
    pub enable_endpoint: bool,
    /// How far back in time we look for spans when queries at not time-bound (`get_services`,
    /// `get_operations`, `get_trace` operations).
    #[serde(default = "JaegerConfig::default_lookback_period_hours")]
    lookback_period_hours: NonZeroU64,
    /// The assumed maximum duration of a trace in seconds.
    ///
    /// Finding a trace happens in two phases: the first phase identifies at least one span that
    /// matches the query, while the second phase retrieves the spans that belong to the trace.
    /// The `max_trace_duration_secs` parameter is used during the second phase to restrict the
    /// search time interval to [span.end_timestamp - max_trace_duration, span.start_timestamp
    /// + max_trace_duration].
    #[serde(default = "JaegerConfig::default_max_trace_duration_secs")]
    max_trace_duration_secs: NonZeroU64,
    /// The maximum number of spans that can be retrieved in a single request.
    #[serde(default = "JaegerConfig::default_max_fetch_spans")]
    pub max_fetch_spans: NonZeroU64,
}

impl JaegerConfig {
    pub fn lookback_period(&self) -> Duration {
        Duration::from_secs(self.lookback_period_hours.get() * 3600)
    }

    pub fn max_trace_duration(&self) -> Duration {
        Duration::from_secs(self.max_trace_duration_secs.get())
    }

    fn default_enable_endpoint() -> bool {
        #[cfg(any(test, feature = "testsuite"))]
        {
            false
        }
        #[cfg(not(any(test, feature = "testsuite")))]
        {
            true
        }
    }

    fn default_lookback_period_hours() -> NonZeroU64 {
        NonZeroU64::new(72).unwrap() // 3 days
    }

    fn default_max_trace_duration_secs() -> NonZeroU64 {
        NonZeroU64::new(3600).unwrap() // 1 hour
    }

    fn default_max_fetch_spans() -> NonZeroU64 {
        NonZeroU64::new(10_000).unwrap() // 10k spans
    }
}

impl Default for JaegerConfig {
    fn default() -> Self {
        Self {
            enable_endpoint: Self::default_enable_endpoint(),
            lookback_period_hours: Self::default_lookback_period_hours(),
            max_trace_duration_secs: Self::default_max_trace_duration_secs(),
            max_fetch_spans: Self::default_max_fetch_spans(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeConfig {
    pub cluster_id: String,
    pub node_id: String,
    pub enabled_services: HashSet<QuickwitService>,
    pub rest_listen_addr: SocketAddr,
    pub gossip_listen_addr: SocketAddr,
    pub grpc_listen_addr: SocketAddr,
    pub gossip_advertise_addr: SocketAddr,
    pub grpc_advertise_addr: SocketAddr,
    pub peer_seeds: Vec<String>,
    pub data_dir_path: PathBuf,
    pub metastore_uri: Uri,
    pub default_index_root_uri: Uri,
    pub rest_cors_allow_origins: Vec<String>,
    pub storage_configs: StorageConfigs,
    pub metastore_configs: MetastoreConfigs,
    pub indexer_config: IndexerConfig,
    pub searcher_config: SearcherConfig,
    pub ingest_api_config: IngestApiConfig,
    pub jaeger_config: JaegerConfig,
}

impl NodeConfig {
    pub fn is_service_enabled(&self, service: QuickwitService) -> bool {
        self.enabled_services.contains(&service)
    }

    /// Parses and validates a [`NodeConfig`] from a given URI and config content.
    pub async fn load(config_format: ConfigFormat, config_content: &[u8]) -> anyhow::Result<Self> {
        let env_vars = env::vars().collect::<HashMap<_, _>>();
        let config = load_node_config_with_env(config_format, config_content, &env_vars).await?;
        if !config.data_dir_path.try_exists()? {
            bail!(
                "data dir `{}` does not exist",
                config.data_dir_path.display()
            );
        }
        Ok(config)
    }

    /// Returns the list of peer seed addresses. The addresses MUST NOT be resolved. Otherwise, the
    /// DNS-based discovery mechanism implemented in Chitchat will not work correctly.
    pub async fn peer_seed_addrs(&self) -> anyhow::Result<Vec<String>> {
        let mut peer_seed_addrs = Vec::new();
        let default_gossip_port = self.gossip_listen_addr.port();

        // We want to pass non-resolved addresses to Chitchat but still want to resolve them for
        // validation purposes. Additionally, we need to append a default port if necessary and
        // finally return the addresses as strings, which is tricky for IPv6. We let the logic baked
        // in `HostAddr` handle this complexity.
        for peer_seed in &self.peer_seeds {
            let peer_seed_addr = HostAddr::parse_with_default_port(peer_seed, default_gossip_port)?;
            if let Err(error) = peer_seed_addr.resolve().await {
                warn!(peer_seed = %peer_seed_addr, error = ?error, "Failed to resolve peer seed address.");
                continue;
            }
            peer_seed_addrs.push(peer_seed_addr.to_string())
        }
        if !self.peer_seeds.is_empty() && peer_seed_addrs.is_empty() {
            bail!(
                "failed to resolve any of the peer seed addresses: `{}`",
                self.peer_seeds.join(", ")
            )
        }
        Ok(peer_seed_addrs)
    }

    pub fn redact(&mut self) {
        self.metastore_uri.redact();
        self.storage_configs.redact();
        self.metastore_configs.redact();
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        serialize::node_config_for_test()
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::indexing::CpuCapacity;

    use crate::IndexerConfig;

    #[test]
    fn test_index_config_serialization() {
        {
            let indexer_config: IndexerConfig = serde_json::from_str(r#"{}"#).unwrap();
            assert_eq!(&indexer_config, &IndexerConfig::default());
            assert!(indexer_config.cpu_capacity.cpu_millis() > 0);
            assert_eq!(indexer_config.cpu_capacity.cpu_millis() % 1_000, 0);
        }
        {
            let indexer_config: IndexerConfig =
                serde_yaml::from_str(r#"cpu_capacity: 1.5"#).unwrap();
            assert_eq!(
                indexer_config.cpu_capacity,
                CpuCapacity::from_cpu_millis(1500)
            );
            let indexer_config_json = serde_json::to_value(&indexer_config).unwrap();
            assert_eq!(
                indexer_config_json
                    .get("cpu_capacity")
                    .unwrap()
                    .as_str()
                    .unwrap(),
                "1500m"
            );
        }
        {
            let indexer_config: IndexerConfig =
                serde_yaml::from_str(r#"cpu_capacity: 1500m"#).unwrap();
            assert_eq!(
                indexer_config.cpu_capacity,
                CpuCapacity::from_cpu_millis(1500)
            );
            let indexer_config_json = serde_json::to_value(&indexer_config).unwrap();
            assert_eq!(
                indexer_config_json
                    .get("cpu_capacity")
                    .unwrap()
                    .as_str()
                    .unwrap(),
                "1500m"
            );
        }
    }
}
