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

use bytesize::ByteSize;
use quickwit_common::fs::get_disk_info;
use quickwit_config::NodeConfig;
use quickwit_config::service::QuickwitService;
use tracing::warn;

/// A list of all the known disk budgets.
///
/// External and unbounded disk usage, e.g. the indexing workbench (`indexing/`) and the delete
/// task workbench (`delete_task_service/`), are not included.
#[derive(Default, Debug, Eq, PartialEq)]
struct ExpectedDiskUsage {
    // indexer / ingester
    split_store_max_num_bytes: Option<ByteSize>,
    max_queue_disk_usage: Option<ByteSize>,
    // searcher
    split_cache: Option<ByteSize>,
}

impl ExpectedDiskUsage {
    fn from_config(node_config: &NodeConfig) -> Self {
        let mut expected = Self::default();
        if node_config.is_service_enabled(QuickwitService::Indexer) {
            expected.max_queue_disk_usage =
                Some(node_config.ingest_api_config.max_queue_disk_usage);
            expected.split_store_max_num_bytes =
                Some(node_config.indexer_config.split_store_max_num_bytes);
        }
        if node_config.is_service_enabled(QuickwitService::Searcher) {
            expected.split_cache = node_config
                .searcher_config
                .split_cache
                .map(|limits| limits.max_num_bytes);
        }
        expected
    }

    fn total(&self) -> ByteSize {
        self.split_store_max_num_bytes.unwrap_or_default()
            + self.max_queue_disk_usage.unwrap_or_default()
            + self.split_cache.unwrap_or_default()
    }
}

pub(super) fn check_data_dir_disk_usage(node_config: &NodeConfig) {
    let Some(disk_info) = get_disk_info(&node_config.data_dir_path) else {
        return;
    };
    let expected_disk_usage = ExpectedDiskUsage::from_config(node_config);
    if expected_disk_usage.total() > disk_info.total_space {
        warn!(
            data_dir = %node_config.data_dir_path.display(),
            device = %disk_info.device_name,
            mount_point = %disk_info.mount_point.display(),
            volume_size = ?disk_info.total_space,
            ?expected_disk_usage,
            "data dir volume too small"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_searcher_without_split_cache_has_no_expected_disk_usage() {
        let mut node_config = NodeConfig::for_test();
        node_config.enabled_services = HashSet::from([QuickwitService::Searcher]);
        node_config.searcher_config.split_cache = None;

        let expected_disk_usage = ExpectedDiskUsage::from_config(&node_config);

        assert_eq!(expected_disk_usage, ExpectedDiskUsage::default());
    }

    #[test]
    fn test_expected_disk_usage_aggregates_enabled_service_budgets() {
        let mut node_config = NodeConfig::for_test();
        node_config.enabled_services =
            HashSet::from([QuickwitService::Indexer, QuickwitService::Searcher]);

        let expected_disk_usage = ExpectedDiskUsage::from_config(&node_config);
        let expected_total = node_config.indexer_config.split_store_max_num_bytes
            + node_config.ingest_api_config.max_queue_disk_usage
            + node_config
                .searcher_config
                .split_cache
                .map(|limits| limits.max_num_bytes)
                .unwrap_or_default();

        assert_eq!(expected_disk_usage.total(), expected_total);
    }
}
