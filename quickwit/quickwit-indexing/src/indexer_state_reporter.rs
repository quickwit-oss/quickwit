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

use quickwit_common::{rate_limited_info, rate_limited_error};
use quickwit_ingest::ShardThroughputReadings;
use quickwit_proto::control_plane::{
    ControlPlaneService, ControlPlaneServiceClient, IndexingTasksUpdate, ReportIndexerStateRequest,
};
use quickwit_proto::indexing::IndexingTask;
use quickwit_proto::types::NodeId;
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
    node_id: NodeId,
    generation_id: u64,
    local_shards_rx: watch::Receiver<Option<Arc<ShardThroughputReadings>>>,
    indexing_tasks_rx: watch::Receiver<Option<Arc<Vec<IndexingTask>>>>,
    control_plane_client: ControlPlaneServiceClient,
}

/// Every REPORT_INTERVAL, we read the most recently reported value from the ingester's shard
/// throughput snapshot, and the running indexing pipelines. We report those to the control plane,
/// even if they're unchanged from our last run. If both are empty, we don't report anything;
/// if one is empty, we report it as None.
impl IndexerStateReporter {
    pub fn start_reporting(
        node_id: NodeId,
        generation_id: u64,
        local_shards_rx: watch::Receiver<Option<Arc<ShardThroughputReadings>>>,
        indexing_tasks_rx: watch::Receiver<Option<Arc<Vec<IndexingTask>>>>,
        control_plane_client: ControlPlaneServiceClient,
    ) -> JoinHandle<()> {
        let reporter = Self {
            node_id,
            generation_id,
            local_shards_rx,
            indexing_tasks_rx,
            control_plane_client,
        };
        tokio::spawn(reporter.run())
    }

    async fn run(self) {
        let mut interval = tokio::time::interval(REPORT_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;

            if let Some(request) = self.observe_indexer_state() {
                self.send_report(request).await;
            }
        }
    }

    fn observe_indexer_state(&self) -> Option<ReportIndexerStateRequest> {
        let local_shards = self.local_shards_rx.borrow().clone();
        let indexing_tasks = self.indexing_tasks_rx.borrow().clone();
        if local_shards.is_none() && indexing_tasks.is_none() {
            return None;
        }

        Some(ReportIndexerStateRequest {
            node_id: self.node_id.to_string(),
            generation_id: self.generation_id,
            shards_update: local_shards.map(|snapshot| snapshot.as_ref().clone().into()),
            indexing_tasks_update: indexing_tasks.map(|tasks| IndexingTasksUpdate {
                indexing_tasks: tasks.as_ref().clone(),
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
                    "reporting indexer state to control plane timed out"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use quickwit_common::tower::DelayLayer;
    use quickwit_proto::control_plane::{
        ControlPlaneError, MockControlPlaneService, ReportIndexerStateResponse,
    };
    use quickwit_proto::types::{IndexUid, PipelineUid};
    use tokio::sync::mpsc;

    use super::*;

    fn task(pipeline: u128) -> IndexingTask {
        IndexingTask {
            index_uid: Some(IndexUid::for_test("index", 0)),
            source_id: "source".to_string(),
            pipeline_uid: Some(PipelineUid::for_test(pipeline)),
            ..Default::default()
        }
    }

    #[test]
    fn test_observe_latest_optional_snapshots() {
        let (shards_tx, shards_rx) = watch::channel(None);
        let (tasks_tx, tasks_rx) = watch::channel(None);
        let reporter = IndexerStateReporter {
            node_id: NodeId::from_str("indexer"),
            generation_id: 42,
            local_shards_rx: shards_rx,
            indexing_tasks_rx: tasks_rx,
            control_plane_client: ControlPlaneServiceClient::mocked(),
        };
        assert!(reporter.observe_indexer_state().is_none());
        shards_tx.send_replace(Some(Arc::new(ShardThroughputReadings::default())));
        let request = reporter.observe_indexer_state().unwrap();
        let request =
            ReportIndexerStateRequest::decode(request.encode_to_vec().as_slice()).unwrap();
        assert_eq!(request.node_id, "indexer");
        assert_eq!(request.generation_id, 42);
        assert!(
            request
                .shards_update
                .unwrap()
                .shard_infos_by_source
                .is_empty()
        );
        assert!(request.indexing_tasks_update.is_none());

        tasks_tx.send_replace(Some(Arc::new(vec![task(1)])));
        tasks_tx.send_replace(Some(Arc::new(vec![task(2)])));
        for _ in 0..2 {
            let request = reporter.observe_indexer_state().unwrap();
            assert!(request.shards_update.is_some());
            assert_eq!(
                request.indexing_tasks_update.unwrap().indexing_tasks,
                vec![task(2)]
            );
        }
        tasks_tx.send_replace(Some(Arc::new(Vec::new())));
        let request = reporter.observe_indexer_state().unwrap();
        let request =
            ReportIndexerStateRequest::decode(request.encode_to_vec().as_slice()).unwrap();
        assert!(
            request
                .indexing_tasks_update
                .unwrap()
                .indexing_tasks
                .is_empty()
        );
        shards_tx.send_replace(None);
        let request = reporter.observe_indexer_state().unwrap();
        assert!(request.shards_update.is_none());
        assert!(request.indexing_tasks_update.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn test_reporting_cadence_and_recovery_after_error() {
        let (_shards_tx, shards_rx) = watch::channel(None);
        let (tasks_tx, tasks_rx) = watch::channel(None);
        let (requests_tx, mut requests_rx) = mpsc::unbounded_channel();
        let mut mock = MockControlPlaneService::new();
        let mut calls = 0;
        mock.expect_report_indexer_state()
            .times(3)
            .returning(move |request| {
                requests_tx.send(request).unwrap();
                calls += 1;
                if calls == 1 {
                    Err(ControlPlaneError::Unavailable("unavailable".to_string()))
                } else {
                    Ok(ReportIndexerStateResponse {})
                }
            });
        let handle = IndexerStateReporter::start_reporting(
            NodeId::from_str("indexer"),
            42,
            shards_rx,
            tasks_rx,
            ControlPlaneServiceClient::from_mock(mock),
        );
        tokio::task::yield_now().await;
        tokio::time::advance(REPORT_INTERVAL).await;
        tokio::task::yield_now().await;
        assert!(requests_rx.try_recv().is_err());

        tasks_tx.send_replace(Some(Arc::new(vec![task(1)])));
        tokio::time::advance(REPORT_INTERVAL / 2).await;
        tokio::task::yield_now().await;
        assert!(requests_rx.try_recv().is_err());
        tokio::time::advance(REPORT_INTERVAL / 2).await;
        assert_eq!(
            timeout(REPORT_INTERVAL, requests_rx.recv())
                .await
                .unwrap()
                .unwrap()
                .indexing_tasks_update
                .unwrap()
                .indexing_tasks,
            vec![task(1)]
        );
        tasks_tx.send_replace(Some(Arc::new(vec![task(2)])));
        tasks_tx.send_replace(Some(Arc::new(vec![task(3)])));
        for _ in 0..2 {
            tokio::time::advance(REPORT_INTERVAL).await;
            assert_eq!(
                timeout(REPORT_INTERVAL, requests_rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .indexing_tasks_update
                    .unwrap()
                    .indexing_tasks,
                vec![task(3)]
            );
        }
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn test_report_timeout_allows_next_snapshot() {
        let (_shards_tx, shards_rx) = watch::channel(None);
        let (tasks_tx, tasks_rx) = watch::channel(Some(Arc::new(vec![task(1)])));
        let mut delayed_mock = MockControlPlaneService::new();
        delayed_mock.expect_report_indexer_state().never();
        let mut reporter = IndexerStateReporter {
            node_id: NodeId::from_str("indexer"),
            generation_id: 42,
            local_shards_rx: shards_rx,
            indexing_tasks_rx: tasks_rx,
            control_plane_client: ControlPlaneServiceClient::tower()
                .stack_report_indexer_state_layer(DelayLayer::new(REPORT_TIMEOUT * 2))
                .build_from_mock(delayed_mock),
        };
        let started = tokio::time::Instant::now();
        reporter
            .send_report(reporter.observe_indexer_state().unwrap())
            .await;
        assert!(started.elapsed() >= REPORT_TIMEOUT);
        assert!(started.elapsed() < REPORT_TIMEOUT * 2);

        tasks_tx.send_replace(Some(Arc::new(vec![task(2)])));
        let mut mock = MockControlPlaneService::new();
        mock.expect_report_indexer_state()
            .once()
            .returning(|request| {
                assert_eq!(
                    request.indexing_tasks_update.unwrap().indexing_tasks,
                    vec![task(2)]
                );
                Ok(ReportIndexerStateResponse {})
            });
        reporter.control_plane_client = ControlPlaneServiceClient::from_mock(mock);
        reporter
            .send_report(reporter.observe_indexer_state().unwrap())
            .await;
    }
}
