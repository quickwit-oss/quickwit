// Copyright (C) 2024 Quickwit, Inc.
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
use http::HeaderMap;
use quickwit_common::net::HostAddr;
use quickwit_common::shared_consts::DEFAULT_SHARD_THROUGHPUT_LIMIT;
use quickwit_common::uri::Uri;
use quickwit_proto::indexing::CpuCapacity;
use quickwit_proto::types::NodeId;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::node_config::serialize::load_node_config_with_env;
use crate::service::QuickwitService;
use crate::storage_config::StorageConfigs;
use crate::{ConfigFormat, MetastoreConfigs};

pub const DEFAULT_QW_CONFIG_PATH: &str = "config/quickwit.yaml";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestConfig {
    pub listen_addr: SocketAddr,
    pub cors_allow_origins: Vec<String>,
    #[serde(with = "http_serde::header_map")]
    pub extra_headers: HeaderMap,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    #[serde(default = "GrpcConfig::default_max_message_size")]
    pub max_message_size: ByteSize,
}

impl GrpcConfig {
    fn default_max_message_size() -> ByteSize {
        ByteSize::mib(20)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.max_message_size >= ByteSize::mb(1),
            "max gRPC message size (`grpc.max_message_size`) must be at least 1MB, got `{}`",
            self.max_message_size
        );
        Ok(())
    }
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            max_message_size: Self::default_max_message_size(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerConfig {
    #[serde(default = "IndexerConfig::default_split_store_max_num_bytes")]
    pub split_store_max_num_bytes: ByteSize,
    #[serde(default = "IndexerConfig::default_split_store_max_num_splits")]
    pub split_store_max_num_splits: usize,
    #[serde(default = "IndexerConfig::default_max_concurrent_split_uploads")]
    pub max_concurrent_split_uploads: usize,
    /// Limits the IO throughput of the `SplitDownloader` and the `MergeExecutor`.
    /// On hardware where IO is constrained, it makes sure that Merges (a batch operation)
    /// does not starve indexing itself (as it is a latency sensitive operation).
    #[serde(default)]
    pub max_merge_write_throughput: Option<ByteSize>,
    /// Maximum number of merge or delete operation that can be executed concurrently.
    /// (defaults to num_cpu / 2).
    #[serde(default = "IndexerConfig::default_merge_concurrency")]
    pub merge_concurrency: NonZeroUsize,
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
            quickwit_common::get_bool_from_env("QW_ENABLE_OTLP_ENDPOINT", true)
        }
    }

    fn default_max_concurrent_split_uploads() -> usize {
        12
    }

    pub fn default_split_store_max_num_bytes() -> ByteSize {
        ByteSize::gib(100)
    }

    pub fn default_split_store_max_num_splits() -> usize {
        1_000
    }

    pub fn default_merge_concurrency() -> NonZeroUsize {
        NonZeroUsize::new(quickwit_common::num_cpus() * 2 / 3)
            .unwrap_or(NonZeroUsize::new(1).unwrap())
    }

    fn default_cpu_capacity() -> CpuCapacity {
        CpuCapacity::one_cpu_thread() * (quickwit_common::num_cpus() as u32)
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
            max_merge_write_throughput: None,
            merge_concurrency: NonZeroUsize::new(3).unwrap(),
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
            merge_concurrency: Self::default_merge_concurrency(),
            max_merge_write_throughput: None,
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
    #[serde(default = "SplitCacheLimits::default_max_file_descriptors")]
    pub max_file_descriptors: NonZeroU32,
}

impl SplitCacheLimits {
    fn default_max_num_splits() -> NonZeroU32 {
        NonZeroU32::new(10_000).unwrap()
    }

    fn default_num_concurrent_downloads() -> NonZeroU32 {
        NonZeroU32::new(1).unwrap()
    }

    fn default_max_file_descriptors() -> NonZeroU32 {
        NonZeroU32::new(100).unwrap()
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
    #[serde(default = "SearcherConfig::default_request_timeout_secs")]
    request_timeout_secs: NonZeroU64,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_timeout_policy: Option<StorageTimeoutPolicy>,
    pub warmup_memory_budget: ByteSize,
    pub warmup_single_split_initial_allocation: ByteSize,
}

/// Configuration controlling how fast a searcher should timeout a `get_slice`
/// request to retry it.
///
/// [Amazon's best practise](https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/timeouts-and-retries-for-latency-sensitive-applications.html)
/// suggests that to ensure low latency, it is best to:
/// - retry small GET request after 2s
/// - retry large GET request when the throughput is below some percentile.
///
/// This policy is inspired by this guidance. It does not track instanteneous throughput, but
/// computes an overall timeout using the following formula:
/// `timeout_offset + num_bytes_get_request / min_throughtput`
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageTimeoutPolicy {
    pub min_throughtput_bytes_per_secs: u64,
    pub timeout_millis: u64,
    // Disclaimer: this is a number of retry, so the overall max number of
    // attempts is `max_num_retries + 1``.
    pub max_num_retries: usize,
}

impl StorageTimeoutPolicy {
    pub fn compute_timeout(&self, num_bytes: usize) -> impl Iterator<Item = Duration> {
        let min_download_time_secs: f64 = if self.min_throughtput_bytes_per_secs == 0 {
            0.0f64
        } else {
            num_bytes as f64 / self.min_throughtput_bytes_per_secs as f64
        };
        let timeout = Duration::from_millis(self.timeout_millis)
            + Duration::from_secs_f64(min_download_time_secs);
        std::iter::repeat(timeout).take(self.max_num_retries + 1)
    }
}

impl Default for SearcherConfig {
    fn default() -> Self {
        SearcherConfig {
            fast_field_cache_capacity: ByteSize::gb(1),
            split_footer_cache_capacity: ByteSize::mb(500),
            partial_request_cache_capacity: ByteSize::mb(64),
            max_num_concurrent_split_streams: 100,
            max_num_concurrent_split_searches: 100,
            aggregation_memory_limit: ByteSize::mb(500),
            aggregation_bucket_limit: 65000,
            split_cache: None,
            request_timeout_secs: Self::default_request_timeout_secs(),
            storage_timeout_policy: None,
            warmup_memory_budget: ByteSize::gb(10),
            warmup_single_split_initial_allocation: ByteSize::gb(1),
        }
    }
}

impl SearcherConfig {
    /// The timeout after which a search should be cancelled
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs.get())
    }
    fn default_request_timeout_secs() -> NonZeroU64 {
        NonZeroU64::new(30).unwrap()
    }
    fn validate(&self) -> anyhow::Result<()> {
        if let Some(split_cache_limits) = self.split_cache {
            if self.max_num_concurrent_split_searches
                > split_cache_limits.max_file_descriptors.get() as usize
            {
                anyhow::bail!(
                    "max_num_concurrent_split_searches ({}) must be lower or equal to \
                     split_cache.max_file_descriptors ({})",
                    self.max_num_concurrent_split_searches,
                    split_cache_limits.max_file_descriptors
                );
            }
            if self.max_num_concurrent_split_streams
                > split_cache_limits.max_file_descriptors.get() as usize
            {
                anyhow::bail!(
                    "max_num_concurrent_split_streams ({}) must be lower or equal to \
                     split_cache.max_file_descriptors ({})",
                    self.max_num_concurrent_split_streams,
                    split_cache_limits.max_file_descriptors
                );
            }
            if self.warmup_single_split_initial_allocation > self.warmup_memory_budget {
                anyhow::bail!(
                    "warmup_single_split_initial_allocation ({}) must be lower or equal to \
                     warmup_memory_budget ({})",
                    self.warmup_single_split_initial_allocation,
                    self.warmup_memory_budget
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct IngestApiConfig {
    pub max_queue_memory_usage: ByteSize,
    pub max_queue_disk_usage: ByteSize,
    replication_factor: usize,
    pub content_length_limit: ByteSize,
    pub shard_throughput_limit: ByteSize,
}

impl Default for IngestApiConfig {
    fn default() -> Self {
        Self {
            max_queue_memory_usage: ByteSize::gib(2),
            max_queue_disk_usage: ByteSize::gib(4),
            replication_factor: 1,
            content_length_limit: ByteSize::mib(10),
            shard_throughput_limit: DEFAULT_SHARD_THROUGHPUT_LIMIT,
        }
    }
}

impl IngestApiConfig {
    /// Returns the replication factor, as defined in environment variable or in the configuration
    /// in that order (the environment variable can overrides the configuration).
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
        ensure!(
            self.max_queue_disk_usage > ByteSize::mib(256),
            "max_queue_disk_usage must be at least 256 MiB, got `{}`",
            self.max_queue_disk_usage
        );
        ensure!(
            self.max_queue_disk_usage >= self.max_queue_memory_usage,
            "max_queue_disk_usage ({}) must be at least max_queue_memory_usage ({})",
            self.max_queue_disk_usage,
            self.max_queue_memory_usage
        );
        info!(
            "ingestion shard throughput limit: {:?}",
            self.shard_throughput_limit
        );
        ensure!(
            self.shard_throughput_limit >= ByteSize::mib(1)
                && self.shard_throughput_limit <= ByteSize::mib(20),
            "shard_throughput_limit ({:?}) must be within 1mb and 20mb",
            self.shard_throughput_limit
        );
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
            quickwit_common::get_bool_from_env("QW_ENABLE_JAEGER_ENDPOINT", true)
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
    pub node_id: NodeId,
    pub enabled_services: HashSet<QuickwitService>,
    pub gossip_listen_addr: SocketAddr,
    pub grpc_listen_addr: SocketAddr,
    pub gossip_advertise_addr: SocketAddr,
    pub grpc_advertise_addr: SocketAddr,
    pub gossip_interval: Duration,
    pub peer_seeds: Vec<String>,
    pub data_dir_path: PathBuf,
    pub metastore_uri: Uri,
    pub default_index_root_uri: Uri,
    pub rest_config: RestConfig,
    pub grpc_config: GrpcConfig,
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
                warn!(peer_seed = %peer_seed_addr, error = ?error, "failed to resolve peer seed address");
                continue;
            }
            peer_seed_addrs.push(peer_seed_addr.to_string())
        }
        if !self.peer_seeds.is_empty() && peer_seed_addrs.is_empty() {
            warn!("failed to resolve all the peer seed addresses")
        }
        Ok(peer_seed_addrs)
    }

    pub fn redact(&mut self) {
        self.metastore_configs.redact();
        self.metastore_uri.redact();
        self.storage_configs.redact();
    }

    /// Creates a config with defaults suitable for testing.
    ///
    /// Uses the default ports without ensuring that they are available.
    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        serialize::node_config_for_tests_from_ports(7280, 7281)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test_from_ports(rest_listen_port: u16, grpc_listen_port: u16) -> Self {
        serialize::node_config_for_tests_from_ports(rest_listen_port, grpc_listen_port)
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::indexing::CpuCapacity;

    use super::*;
    use crate::IndexerConfig;

    #[test]
    fn test_indexer_config_serialization() {
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
                serde_yaml::from_str(r#"merge_concurrency: 5"#).unwrap();
            assert_eq!(
                indexer_config.merge_concurrency,
                NonZeroUsize::new(5).unwrap()
            );
            let indexer_config_json = serde_json::to_value(&indexer_config).unwrap();
            assert_eq!(
                indexer_config_json
                    .get("merge_concurrency")
                    .unwrap()
                    .as_u64()
                    .unwrap(),
                5
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

    #[test]
    fn test_validate_ingest_api_default() {
        let ingest_api_config: IngestApiConfig = serde_yaml::from_str("").unwrap();
        assert!(ingest_api_config.validate().is_ok());
        assert_eq!(ingest_api_config, IngestApiConfig::default());
    }

    #[test]
    fn test_validate_ingest_api_config() {
        {
            let ingest_api_config: IngestApiConfig = serde_yaml::from_str(
                r#"
                    max_queue_disk_usage: 100M
                "#,
            )
            .unwrap();
            assert_eq!(
                ingest_api_config.validate().unwrap_err().to_string(),
                "max_queue_disk_usage must be at least 256 MiB, got `100.0 MB`"
            );
        }
        {
            let ingest_api_config: IngestApiConfig = serde_yaml::from_str(
                r#"
                    max_queue_memory_usage: 600M
                    max_queue_disk_usage: 500M
                "#,
            )
            .unwrap();
            assert_eq!(
                ingest_api_config.validate().unwrap_err().to_string(),
                "max_queue_disk_usage (500.0 MB) must be at least max_queue_memory_usage (600.0 \
                 MB)"
            );
        }
        {
            let ingest_api_config: IngestApiConfig = serde_yaml::from_str(
                r#"
                    shard_throughput_limit: 21M
                "#,
            )
            .unwrap();
            assert_eq!(
                ingest_api_config.validate().unwrap_err().to_string(),
                "shard_throughput_limit (21.0 MB) must be within 1mb and 20mb"
            );
        }
    }

    #[test]
    fn test_grpc_config_serialization() {
        let grpc_config: GrpcConfig = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(
            grpc_config.max_message_size,
            GrpcConfig::default().max_message_size
        );

        let grpc_config: GrpcConfig = serde_yaml::from_str(
            r#"
                max_message_size: 4MiB
            "#,
        )
        .unwrap();
        assert_eq!(grpc_config.max_message_size, ByteSize::mib(4));
    }

    #[test]
    fn test_grpc_config_validate() {
        let grpc_config = GrpcConfig {
            max_message_size: ByteSize::mb(1),
        };
        assert!(grpc_config.validate().is_ok());

        let grpc_config = GrpcConfig {
            max_message_size: ByteSize::kb(1),
        };
        assert!(grpc_config.validate().is_err());
    }
}
