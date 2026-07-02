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

use std::time::Duration;

use quickwit_metrics::{
    LabelNames, Labels, LazyGauge, gauge, label_names, label_values, labels, lazy_gauge,
};
use quickwit_proto::metastore::{
    ListIndexStatsRequest, MetastoreService, MetastoreServiceClient, SplitStats,
};
use tracing::error;

// short interval for tests so metrics are emitted during the test
#[cfg(any(test, feature = "testsuite"))]
const INDEX_METRICS_POLLING_INTERVAL: Duration = Duration::from_millis(100);

#[cfg(not(any(test, feature = "testsuite")))]
const INDEX_METRICS_POLLING_INTERVAL: Duration = Duration::from_mins(5);

static DD_SPLIT_SIZE_BYTES: LazyGauge = lazy_gauge!(
    name: "split_size_bytes.gauge",
    description: "Split size bytes by index and split state for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

static DD_NUM_SPLITS: LazyGauge = lazy_gauge!(
    name: "num_splits.gauge",
    description: "Number of splits by index and split state for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

/// This loop gets index stats from the metastore and updates the corresponding gauges
async fn index_metrics_loop(metastore: MetastoreServiceClient) -> anyhow::Result<()> {
    let mut poll_interval = tokio::time::interval(INDEX_METRICS_POLLING_INTERVAL);

    loop {
        poll_interval.tick().await;
        if let Err(error) = update_index_metrics(&metastore).await {
            error!(%error, "failed to update index metrics");
        }
    }
}

const INDEX_LABEL: LabelNames<1> = label_names!("index");
const STAGE_LABEL: Labels<1> = labels!("split_state" => "staged");
const PUBLISHED_LABEL: Labels<1> = labels!("split_state" => "published");
const MARKED_FOR_DELETION_LABEL: Labels<1> = labels!("split_state" => "marked_for_deletion");
async fn update_index_metrics(metastore: &MetastoreServiceClient) -> anyhow::Result<()> {
    let response = metastore
        .list_index_stats(ListIndexStatsRequest {
            index_id_patterns: vec!["*".to_string()],
        })
        .await?;

    let total_size_bytes = |split_stats: Option<SplitStats>| {
        split_stats
            .map(|split_stats| split_stats.total_size_bytes as f64)
            .unwrap_or(0.0)
    };
    let num_splits = |split_stats: Option<SplitStats>| {
        split_stats
            .map(|split_stats| split_stats.num_splits as f64)
            .unwrap_or(0.0)
    };

    for index_stats in response.index_stats {
        let index_id = index_stats
            .index_uid
            .expect("`index_uid` should be populated")
            .index_id;
        let index_label = label_values!(INDEX_LABEL => index_id);

        gauge!(parent: &DD_SPLIT_SIZE_BYTES, labels: [index_label, STAGE_LABEL])
            .set(total_size_bytes(index_stats.staged));
        gauge!(parent: &DD_SPLIT_SIZE_BYTES, labels: [index_label, PUBLISHED_LABEL])
            .set(total_size_bytes(index_stats.published));
        gauge!(parent: &DD_SPLIT_SIZE_BYTES, labels: [index_label, MARKED_FOR_DELETION_LABEL])
            .set(total_size_bytes(index_stats.marked_for_deletion));

        gauge!(parent: &DD_NUM_SPLITS, labels: [index_label, STAGE_LABEL])
            .set(num_splits(index_stats.staged));
        gauge!(parent: &DD_NUM_SPLITS, labels: [index_label, PUBLISHED_LABEL])
            .set(num_splits(index_stats.published));
        gauge!(parent: &DD_NUM_SPLITS, labels: [index_label, MARKED_FOR_DELETION_LABEL])
            .set(num_splits(index_stats.marked_for_deletion));
    }
    Ok(())
}

pub(crate) fn start_index_metrics_loop(metastore: MetastoreServiceClient) {
    tokio::task::spawn(index_metrics_loop(metastore));
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshot};
    use ordered_float::OrderedFloat;
    use quickwit_config::IndexConfig;
    use quickwit_metastore::{
        CreateIndexRequestExt, SplitMetadata, StageSplitsRequestExt, metastore_for_test,
    };
    use quickwit_proto::metastore::{CreateIndexRequest, StageSplitsRequest};
    use quickwit_proto::types::IndexUid;

    use super::*;

    fn snapshot_as_map_for_test(snapshot: Snapshot) -> HashMap<String, DebugValue> {
        snapshot
            .into_vec()
            .into_iter()
            .map(|(composite_key, _, _, value)| (format!("{}", composite_key.key()), value))
            .collect()
    }

    #[tokio::test]
    async fn test_update_index_metrics() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _recorder_guard = metrics::set_default_local_recorder(Box::leak(Box::new(recorder)));

        let metastore = metastore_for_test();

        let index_id = "test-list-index-stats";
        let index_uid = IndexUid::new_with_random_ulid(index_id);
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(index_id, &index_uri);

        let split_id = format!("{index_id}--split-1");
        let split_metadata = SplitMetadata {
            split_id: split_id.clone().into(),
            index_uid: index_uid.clone(),
            footer_offsets: 0..2048,
            ..Default::default()
        };

        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            vec![split_metadata.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        update_index_metrics(&metastore).await.unwrap();

        let snapshot = snapshot_as_map_for_test(snapshotter.snapshot());
        assert_eq!(snapshot.len(), 8);

        assert_eq!(
            snapshot
                .get("Key(cloudprem.split_size_bytes.gauge)")
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(0.0))
        );
        assert_eq!(
            snapshot.get("Key(cloudprem.num_splits.gauge)").unwrap(),
            &DebugValue::Gauge(OrderedFloat(0.0))
        );

        assert_eq!(
            snapshot
                .get(&format!(
                    "Key(cloudprem.split_size_bytes.gauge, [index = {}, split_state = staged])",
                    index_id
                ))
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(2048.0))
        );
        assert_eq!(
            snapshot
                .get(&format!(
                    "Key(cloudprem.split_size_bytes.gauge, [index = {}, split_state = published])",
                    index_id
                ))
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(0.0))
        );
        assert_eq!(
            snapshot
                .get(&format!(
                    "Key(cloudprem.split_size_bytes.gauge, [index = {}, split_state = \
                     marked_for_deletion])",
                    index_id
                ))
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(0.0))
        );

        assert_eq!(
            snapshot
                .get(&format!(
                    "Key(cloudprem.num_splits.gauge, [index = {}, split_state = staged])",
                    index_id
                ))
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(1.0))
        );
        assert_eq!(
            snapshot
                .get(&format!(
                    "Key(cloudprem.num_splits.gauge, [index = {}, split_state = published])",
                    index_id
                ))
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(0.0))
        );
        assert_eq!(
            snapshot
                .get(&format!(
                    "Key(cloudprem.num_splits.gauge, [index = {}, split_state = \
                     marked_for_deletion])",
                    index_id
                ))
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(0.0))
        );
    }
}
