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

use std::sync::Arc;
use std::time::Duration;

use quickwit_cluster::{Cluster, ClusterNode};
use quickwit_common::{rate_limited_error, rate_limited_info};
use quickwit_config::service::QuickwitService;
use quickwit_ingest::ShardReadingsBySource;
use quickwit_proto::control_plane::{
    ControlPlaneService, ControlPlaneServiceClient, IndexingTasksUpdate, ReportIndexerStateRequest,
    ShardsUpdate,
};
use quickwit_proto::indexing::IndexingTask;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, timeout};

const REPORT_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(1)
};

const REPORT_TIMEOUT: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(25)
} else {
    Duration::from_millis(500)
};

pub struct IndexerStateReporter {
    cluster: Cluster,
    local_shards_rx: watch::Receiver<Option<Arc<ShardReadingsBySource>>>,
    indexing_tasks_rx: watch::Receiver<Option<Arc<Vec<IndexingTask>>>>,
    control_plane_client: ControlPlaneServiceClient,
}

/// Every REPORT_INTERVAL, we read the most recently reported value from the ingester's shard
/// throughput snapshot, and the running indexing pipelines. We report those to the control plane,
/// even if they're unchanged from our last run. If both are empty, we don't report anything;
/// if one is empty, we report it as None.
impl IndexerStateReporter {
    pub fn start_reporting(
        cluster: Cluster,
        local_shards_rx: watch::Receiver<Option<Arc<ShardReadingsBySource>>>,
        indexing_tasks_rx: watch::Receiver<Option<Arc<Vec<IndexingTask>>>>,
        control_plane_client: ControlPlaneServiceClient,
    ) -> JoinHandle<()> {
        let reporter = Self {
            cluster,
            local_shards_rx,
            indexing_tasks_rx,
            control_plane_client,
        };
        tokio::spawn(reporter.run())
    }

    async fn run(self) {
        self.wait_for_all_indexers_to_enable_shard_scaling_v2()
            .await;

        let mut interval = tokio::time::interval(REPORT_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;

            if let Some(request) = self.observe_indexer_state() {
                self.send_report(request).await;
            }
        }
    }

    /// gRPC reporting only occurs once all indexers are migrated to shard scaling v2.
    async fn wait_for_all_indexers_to_enable_shard_scaling_v2(&self) {
        loop {
            if self
                .cluster
                .all_service_nodes_satisfy(
                    QuickwitService::Indexer,
                    ClusterNode::enable_shard_scaling_v2,
                )
                .await
            {
                return;
            }
            tokio::time::sleep(REPORT_INTERVAL).await;
        }
    }

    fn observe_indexer_state(&self) -> Option<ReportIndexerStateRequest> {
        let readings_by_source_opt = self.local_shards_rx.borrow().clone();
        let indexing_tasks_opt = self.indexing_tasks_rx.borrow().clone();
        if readings_by_source_opt.is_none() && indexing_tasks_opt.is_none() {
            // There's nothing to report.
            return None;
        }
        Some(ReportIndexerStateRequest {
            node_id: self.cluster.self_node_id().to_string(),
            generation_id: self.cluster.self_chitchat_id().generation_id,
            shards_update: readings_by_source_opt
                .map(|readings_by_source| ShardsUpdate::from(readings_by_source.as_ref())),
            indexing_tasks_update: indexing_tasks_opt.map(|indexing_tasks| IndexingTasksUpdate {
                indexing_tasks: indexing_tasks.as_ref().clone(),
            }),
        })
    }

    /// We send the update. The control plane will ack it basically immediately on success. We don't
    /// retry, and we have a short timeout; we don't need either, as the next iteration of the loop
    /// will take place soon anyway.
    async fn send_report(&self, request: ReportIndexerStateRequest) {
        let report_future = self.control_plane_client.report_indexer_state(request);

        match timeout(REPORT_TIMEOUT, report_future).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                rate_limited_error!(
                    limit_per_min = 1,
                    "failed to report indexer state to control plane: {error}"
                );
            }
            Err(_) => {
                rate_limited_info!(
                    limit_per_min = 1,
                    "reporting indexer state to control plane timed out. Trying again next tick"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytesize::ByteSize;
    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_ingest::ShardThroughputReading;
    use quickwit_proto::control_plane::{MockControlPlaneService, ReportIndexerStateResponse};
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::types::{IndexUid, PipelineUid, ShardId, SourceUid};
    use tokio::sync::mpsc;

    use super::*;

    fn task(pipeline: u128) -> IndexingTask {
        IndexingTask {
            index_uid: Some(IndexUid::for_test("test-index", 0)),
            source_id: "test-source".to_string(),
            pipeline_uid: Some(PipelineUid::for_test(pipeline)),
            ..Default::default()
        }
    }

    async fn indexer_cluster() -> Cluster {
        create_cluster_for_test(
            Vec::new(),
            &["indexer"],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_observe_indexer_state() {
        let cluster = indexer_cluster().await;
        let (local_shards_tx, local_shards_rx) = watch::channel(None);
        let (indexing_tasks_tx, indexing_tasks_rx) = watch::channel(None);
        let reporter = IndexerStateReporter {
            cluster: cluster.clone(),
            local_shards_rx,
            indexing_tasks_rx,
            control_plane_client: ControlPlaneServiceClient::mocked(),
        };
        assert!(reporter.observe_indexer_state().is_none());

        indexing_tasks_tx.send_replace(Some(Arc::new(vec![task(1)])));
        let request = reporter.observe_indexer_state().unwrap();
        assert_eq!(request.node_id, cluster.self_node_id().to_string());
        assert_eq!(
            request.generation_id,
            cluster.self_chitchat_id().generation_id
        );
        assert!(request.shards_update.is_none());
        assert_eq!(
            request.indexing_tasks_update.unwrap().indexing_tasks,
            vec![task(1)]
        );

        let source_uid = SourceUid {
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
        };
        let reading = ShardThroughputReading {
            shard_id: ShardId::from(1),
            shard_state: ShardState::Closed,
            short_term_ingestion_rate: ByteSize::b(123),
            long_term_ingestion_rate: ByteSize::b(456),
        };
        local_shards_tx.send_replace(Some(Arc::new(ShardReadingsBySource {
            readings_by_source: BTreeMap::from([(source_uid, vec![reading])]),
        })));
        let request = reporter.observe_indexer_state().unwrap();
        let shard_infos_by_source = request.shards_update.unwrap().shard_infos_by_source;
        assert_eq!(shard_infos_by_source.len(), 1);
        assert_eq!(shard_infos_by_source[0].source_id, "test-source");

        let shard_info = &shard_infos_by_source[0].shard_infos[0];
        assert_eq!(shard_info.shard_id, Some(ShardId::from(1)));
        assert_eq!(shard_info.shard_state(), ShardState::Closed);
        assert_eq!(shard_info.short_term_ingestion_rate_bytes_per_sec, 123);
        assert_eq!(shard_info.long_term_ingestion_rate_bytes_per_sec, 456);
    }

    #[tokio::test]
    async fn test_reporter_waits_for_all_indexers_to_enable_shard_scaling_v2() {
        let cluster = indexer_cluster().await;
        let (_local_shards_tx, local_shards_rx) = watch::channel(None);
        let (_indexing_tasks_tx, indexing_tasks_rx) = watch::channel(Some(Arc::new(vec![task(1)])));
        let (requests_tx, mut requests_rx) = mpsc::unbounded_channel();
        let mut mock_control_plane = MockControlPlaneService::new();
        mock_control_plane
            .expect_report_indexer_state()
            .returning(move |request| {
                requests_tx.send(request).unwrap();
                Ok(ReportIndexerStateResponse {})
            });
        let reporter_handle = IndexerStateReporter::start_reporting(
            cluster.clone(),
            local_shards_rx,
            indexing_tasks_rx,
            ControlPlaneServiceClient::from_mock(mock_control_plane),
        );

        // The only indexer hasn't enabled shard scaling v2 yet: nothing is reported.
        tokio::time::sleep(REPORT_INTERVAL * 3).await;
        assert!(requests_rx.try_recv().is_err());

        cluster.set_self_enable_shard_scaling_v2(true).await;
        let request = timeout(Duration::from_secs(5), requests_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            request.indexing_tasks_update.unwrap().indexing_tasks,
            vec![task(1)]
        );
        reporter_handle.abort();
    }
}
