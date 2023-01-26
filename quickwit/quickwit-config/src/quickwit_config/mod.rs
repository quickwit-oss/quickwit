// Copyright (C) 2022 Quickwit, Inc.
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
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use byte_unit::Byte;
use quickwit_common::net::HostAddr;
use quickwit_common::uri::Uri;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::quickwit_config::serialize::load_quickwit_config_with_env;
use crate::service::QuickwitService;
use crate::ConfigFormat;

pub const DEFAULT_QW_CONFIG_PATH: &str = "config/quickwit.yaml";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerConfig {
    #[serde(default = "IndexerConfig::default_split_store_max_num_bytes")]
    pub split_store_max_num_bytes: Byte,
    #[serde(default = "IndexerConfig::default_split_store_max_num_splits")]
    pub split_store_max_num_splits: usize,
    #[serde(default = "IndexerConfig::default_max_concurrent_split_uploads")]
    pub max_concurrent_split_uploads: usize,
    /// Enables the OpenTelemetry exporter endpoint to ingest logs and traces via the OpenTelemetry
    /// Protocol (OTLP).
    #[serde(default = "IndexerConfig::default_enable_otlp_endpoint")]
    pub enable_otlp_endpoint: bool,
}

impl IndexerConfig {
    fn default_enable_otlp_endpoint() -> bool {
        !(cfg!(feature = "test") || cfg!(feature = "testsuite"))
    }

    fn default_max_concurrent_split_uploads() -> usize {
        12
    }

    pub fn default_split_store_max_num_bytes() -> Byte {
        Byte::from_bytes(100_000_000_000) // 100G
    }

    pub fn default_split_store_max_num_splits() -> usize {
        1_000
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> anyhow::Result<Self> {
        let indexer_config = IndexerConfig {
            enable_otlp_endpoint: true,
            split_store_max_num_bytes: Byte::from_bytes(1_000_000),
            split_store_max_num_splits: 3,
            max_concurrent_split_uploads: 4,
        };
        Ok(indexer_config)
    }
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            enable_otlp_endpoint: Self::default_enable_otlp_endpoint(),
            split_store_max_num_bytes: Self::default_split_store_max_num_bytes(),
            split_store_max_num_splits: Self::default_split_store_max_num_splits(),
            max_concurrent_split_uploads: Self::default_max_concurrent_split_uploads(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearcherConfig {
    #[serde(default = "SearcherConfig::default_fast_field_cache_capacity")]
    pub fast_field_cache_capacity: Byte,
    #[serde(default = "SearcherConfig::default_split_footer_cache_capacity")]
    pub split_footer_cache_capacity: Byte,
    #[serde(default = "SearcherConfig::default_max_num_concurrent_split_searches")]
    pub max_num_concurrent_split_searches: usize,
    #[serde(default = "SearcherConfig::default_max_num_concurrent_split_streams")]
    pub max_num_concurrent_split_streams: usize,
}

impl SearcherConfig {
    fn default_fast_field_cache_capacity() -> Byte {
        Byte::from_bytes(1_000_000_000) // 1G
    }

    fn default_split_footer_cache_capacity() -> Byte {
        Byte::from_bytes(500_000_000) // 500M
    }

    fn default_max_num_concurrent_split_searches() -> usize {
        100
    }

    fn default_max_num_concurrent_split_streams() -> usize {
        100
    }
}

impl Default for SearcherConfig {
    fn default() -> Self {
        Self {
            fast_field_cache_capacity: Self::default_fast_field_cache_capacity(),
            split_footer_cache_capacity: Self::default_split_footer_cache_capacity(),
            max_num_concurrent_split_streams: Self::default_max_num_concurrent_split_streams(),
            max_num_concurrent_split_searches: Self::default_max_num_concurrent_split_searches(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestApiConfig {
    #[serde(default = "IngestApiConfig::default_max_queue_memory_usage")]
    pub max_queue_memory_usage: usize,
    #[serde(default = "IngestApiConfig::default_max_queue_disk_usage")]
    pub max_queue_disk_usage: usize,
}

impl IngestApiConfig {
    fn default_max_queue_memory_usage() -> usize {
        2 * 1024 * 1024 * 1024 // 2 GiB // TODO maybe we want more?
    }

    fn default_max_queue_disk_usage() -> usize {
        4 * 1024 * 1024 * 1024 // 4 GiB // TODO maybe we want more?
    }
}

impl Default for IngestApiConfig {
    fn default() -> Self {
        Self {
            max_queue_memory_usage: Self::default_max_queue_memory_usage(),
            max_queue_disk_usage: Self::default_max_queue_disk_usage(),
        }
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
        !(cfg!(feature = "test") || cfg!(feature = "testsuite"))
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
pub struct QuickwitConfig {
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
    pub indexer_config: IndexerConfig,
    pub searcher_config: SearcherConfig,
    pub ingest_api_config: IngestApiConfig,
    pub jaeger_config: JaegerConfig,
}

impl QuickwitConfig {
    /// Parses and validates a [`QuickwitConfig`] from a given URI and config content.
    pub async fn load(config_format: ConfigFormat, config_content: &[u8]) -> anyhow::Result<Self> {
        let env_vars = env::vars().collect::<HashMap<_, _>>();
        let config =
            load_quickwit_config_with_env(config_format, config_content, &env_vars).await?;
        if !config.data_dir_path.try_exists()? {
            bail!(
                "Data dir `{}` does not exist.",
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
                "Failed to resolve any of the peer seed addresses: `{}`",
                self.peer_seeds.join(", ")
            )
        }
        Ok(peer_seed_addrs)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        serialize::quickwit_config_for_test()
    }
}
