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

use quickwit_common::rate_limited_warn;
use quickwit_ingest::LocalShardsSnapshot;
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
    local_shards_rx: watch::Receiver<Option<Arc<LocalShardsSnapshot>>>,
    indexing_tasks_rx: watch::Receiver<Option<Arc<Vec<IndexingTask>>>>,
    control_plane_client: ControlPlaneServiceClient,
}

impl IndexerStateReporter {
    pub fn start_reporting(
        node_id: NodeId,
        generation_id: u64,
        local_shards_rx: watch::Receiver<Option<Arc<LocalShardsSnapshot>>>,
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

    async fn send_report(&self, request: ReportIndexerStateRequest) {
        let report_future = self.control_plane_client.report_indexer_state(request);

        match timeout(REPORT_TIMEOUT, report_future).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                rate_limited_warn!(
                    limit_per_min = 6,
                    "failed to report indexer state to control plane: {error}"
                );
            }
            Err(_) => {
                rate_limited_warn!(
                    limit_per_min = 6,
                    "reporting indexer state to control plane timed out"
                );
            }
        }
    }
}
