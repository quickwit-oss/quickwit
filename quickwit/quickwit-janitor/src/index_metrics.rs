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
use std::time::Duration;

use metrics::{Gauge, Label, gauge};
use quickwit_proto::metastore::{ListIndexStatsRequest, MetastoreService, MetastoreServiceClient};
use tracing::error;

// short interval for tests so metrics are emitted during the test
#[cfg(any(test, feature = "testsuite"))]
const INDEX_METRICS_POLLING_INTERVAL: Duration = Duration::from_millis(100);

#[cfg(not(any(test, feature = "testsuite")))]
const INDEX_METRICS_POLLING_INTERVAL: Duration = Duration::from_mins(5);

/// This loop gets index stats from the metastore and updates the corresponding gauges
async fn index_metrics_loop(metastore: MetastoreServiceClient) -> anyhow::Result<()> {
    let mut poll_interval = tokio::time::interval(INDEX_METRICS_POLLING_INTERVAL);
    let mut index_metrics = DDIndexMetrics::default();

    loop {
        poll_interval.tick().await;
        if let Err(error) = update_index_metrics(&metastore, &mut index_metrics).await {
            error!(%error, "failed to update index metrics");
        }
    }
}

async fn update_index_metrics(
    metastore: &MetastoreServiceClient,
    index_metrics: &mut DDIndexMetrics,
) -> anyhow::Result<()> {
    let response = metastore
        .list_index_stats(ListIndexStatsRequest {
            index_id_patterns: vec!["*".to_string()],
        })
        .await?;

    for index_stats in response.index_stats {
        let index_id = index_stats
            .index_uid
            .expect("`index_uid` should be populated")
            .index_id;

        // update total_size_bytes
        index_metrics.dd_index_size_bytes.set(
            index_id.clone(),
            index_stats
                .staged
                .map(|split_stats| split_stats.total_size_bytes as f64)
                .unwrap_or(0.0),
            index_stats
                .published
                .map(|split_stats| split_stats.total_size_bytes as f64)
                .unwrap_or(0.0),
            index_stats
                .marked_for_deletion
                .map(|split_stats| split_stats.total_size_bytes as f64)
                .unwrap_or(0.0),
        )?;

        // update num_splits
        index_metrics.dd_num_splits.set(
            index_id.clone(),
            index_stats
                .staged
                .map(|split_stats| split_stats.num_splits as f64)
                .unwrap_or(0.0),
            index_stats
                .published
                .map(|split_stats| split_stats.num_splits as f64)
                .unwrap_or(0.0),
            index_stats
                .marked_for_deletion
                .map(|split_stats| split_stats.num_splits as f64)
                .unwrap_or(0.0),
        )?;
    }
    Ok(())
}

pub(crate) fn start_index_metrics_loop(metastore: MetastoreServiceClient) {
    tokio::task::spawn(index_metrics_loop(metastore));
}

#[derive(Clone)]
pub struct DDIndexGauges {
    name: &'static str,
    gauges: HashMap<String, (Gauge, Gauge, Gauge)>,
}

impl DDIndexGauges {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            gauges: HashMap::new(),
        }
    }

    pub fn set(
        &mut self,
        index_id: String,
        staged_value: f64,
        published_value: f64,
        marked_for_deletion_value: f64,
    ) -> anyhow::Result<()> {
        // if gauges entry is not found, register a new one
        let gauges = self.gauges.entry(index_id.clone()).or_insert_with(|| {
            let staged_labels = vec![
                Label::new("index", index_id.clone()),
                Label::new("split_state", "staged".to_string()),
            ];
            let published_labels = vec![
                Label::new("index", index_id.clone()),
                Label::new("split_state", "published".to_string()),
            ];
            let marked_for_deletion_labels = vec![
                Label::new("index", index_id),
                Label::new("split_state", "marked_for_deletion".to_string()),
            ];
            (
                gauge!(self.name, staged_labels),
                gauge!(self.name, published_labels),
                gauge!(self.name, marked_for_deletion_labels),
            )
        });
        gauges.0.set(staged_value);
        gauges.1.set(published_value);
        gauges.2.set(marked_for_deletion_value);
        Ok(())
    }
}

#[derive(Clone)]
pub struct DDIndexMetrics {
    pub dd_index_size_bytes: DDIndexGauges,
    pub dd_num_splits: DDIndexGauges,
}

impl Default for DDIndexMetrics {
    fn default() -> Self {
        Self {
            dd_index_size_bytes: DDIndexGauges::new("split_size_bytes.gauge"),
            dd_num_splits: DDIndexGauges::new("num_splits.gauge"),
        }
    }
}

#[cfg(test)]
mod tests {
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
            split_id: split_id.clone(),
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

        let mut index_metrics = DDIndexMetrics::default();
        let _ = update_index_metrics(&metastore, &mut index_metrics).await;

        let snapshot = snapshot_as_map_for_test(snapshotter.snapshot());
        assert_eq!(snapshot.len(), 6);

        assert_eq!(
            snapshot
                .get(&format!(
                    "Key(split_size_bytes.gauge, [index = {}, split_state = staged])",
                    index_id
                ))
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(2048.0))
        );
        assert_eq!(
            snapshot
                .get(&format!(
                    "Key(split_size_bytes.gauge, [index = {}, split_state = published])",
                    index_id
                ))
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(0.0))
        );
        assert_eq!(
            snapshot
                .get(&format!(
                    "Key(split_size_bytes.gauge, [index = {}, split_state = marked_for_deletion])",
                    index_id
                ))
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(0.0))
        );

        assert_eq!(
            snapshot
                .get(&format!(
                    "Key(num_splits.gauge, [index = {}, split_state = staged])",
                    index_id
                ))
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(1.0))
        );
        assert_eq!(
            snapshot
                .get(&format!(
                    "Key(num_splits.gauge, [index = {}, split_state = published])",
                    index_id
                ))
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(0.0))
        );
        assert_eq!(
            snapshot
                .get(&format!(
                    "Key(num_splits.gauge, [index = {}, split_state = marked_for_deletion])",
                    index_id
                ))
                .unwrap(),
            &DebugValue::Gauge(OrderedFloat(0.0))
        );
    }
}
