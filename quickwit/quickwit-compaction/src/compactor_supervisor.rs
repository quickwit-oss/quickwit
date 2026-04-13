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

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Health};
use quickwit_common::io::Limiter;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_indexing::IndexingSplitStore;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::StorageResolver;
use serde::Serialize;
use tracing::{error, info};

use crate::compaction_pipeline::CompactionPipeline;

const SUPERVISE_LOOP_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct SuperviseLoop;

#[derive(Clone, Debug, Default, Serialize)]
pub struct CompactorSupervisorState {
    pub num_pipeline_slots: usize,
    pub num_occupied_slots: usize,
    pub num_completed_tasks: usize,
    pub num_failed_tasks: usize,
}

/// Manages a pool of `CompactionPipeline`s, each executing a single merge task.
///
/// Periodically checks pipeline health, handles retries on failure, and reaps
/// completed/failed pipelines.
pub struct CompactorSupervisor {
    pipelines: Vec<Option<CompactionPipeline>>,

    // Shared resources distributed to pipelines when spawning actor chains.
    io_throughput_limiter: Option<Limiter>,
    split_store: IndexingSplitStore,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    max_concurrent_split_uploads: usize,
    event_broker: EventBroker,

    // Scratch directory root (<data_dir>/compaction/).
    compaction_root_directory: TempDirectory,

    max_local_retries: usize,

    // dummy counters until we have real state
    num_completed_tasks: usize,
    num_failed_tasks: usize,
}

impl CompactorSupervisor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        num_pipeline_slots: usize,
        io_throughput_limiter: Option<Limiter>,
        split_store: IndexingSplitStore,
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
        max_concurrent_split_uploads: usize,
        event_broker: EventBroker,
        compaction_root_directory: TempDirectory,
        max_local_retries: usize,
    ) -> Self {
        let pipelines = (0..num_pipeline_slots).map(|_| None).collect();
        CompactorSupervisor {
            pipelines,
            io_throughput_limiter,
            split_store,
            metastore,
            storage_resolver,
            max_concurrent_split_uploads,
            event_broker,
            compaction_root_directory,
            max_local_retries,
            num_completed_tasks: 0,
            num_failed_tasks: 0,
        }
    }

    async fn supervise(&mut self) {
        for slot in &mut self.pipelines {
            let Some(pipeline) = slot else {
                continue;
            };

            match pipeline.check_actor_health() {
                Health::Healthy => {}
                Health::Success => {
                    info!(task_id=%pipeline.task_id, "compaction task completed");
                    self.num_completed_tasks += 1;
                    *slot = None;
                }
                Health::FailureOrUnhealthy => {
                    if pipeline.retry_count < self.max_local_retries {
                        info!(
                            task_id=%pipeline.task_id,
                            retry_count=%pipeline.retry_count,
                            "retrying compaction pipeline"
                        );
                        pipeline.restart().await;
                    } else {
                        error!(
                            task_id=%pipeline.task_id,
                            retry_count=%pipeline.retry_count,
                            "compaction pipeline exhausted retries"
                        );
                        pipeline.terminate().await;
                        self.num_failed_tasks += 1;
                        *slot = None;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Actor for CompactorSupervisor {
    type ObservableState = ();

    fn name(&self) -> String {
        "CompactorSupervisor".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {}

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        info!(
            num_pipeline_slots=%self.pipelines.len(),
            "compactor supervisor started"
        );
        ctx.schedule_self_msg(SUPERVISE_LOOP_INTERVAL, SuperviseLoop);
        Ok(())
    }
}

#[async_trait]
impl Handler<SuperviseLoop> for CompactorSupervisor {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: SuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.supervise().await;
        ctx.schedule_self_msg(SUPERVISE_LOOP_INTERVAL, SuperviseLoop);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_proto::metastore::{MetastoreServiceClient, MockMetastoreService};
    use quickwit_storage::{RamStorage, StorageResolver};

    use super::*;
    use crate::compaction_pipeline::CompactionPipeline;

    fn test_supervisor(num_slots: usize) -> CompactorSupervisor {
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage);
        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());
        CompactorSupervisor::new(
            num_slots,
            None,
            split_store,
            metastore,
            StorageResolver::for_test(),
            2,
            EventBroker::default(),
            TempDirectory::for_test(),
            2,
        )
    }

    #[tokio::test]
    async fn test_supervisor_starts_with_empty_slots() {
        let universe = Universe::with_accelerated_time();
        let supervisor = test_supervisor(4);
        let (_mailbox, handle) = universe.spawn_builder().spawn(supervisor);
        let obs = handle.process_pending_and_observe().await;
        assert_eq!(obs.obs_type, quickwit_actors::ObservationType::Alive);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_supervisor_supervise_reaps_no_handle_pipelines() {
        // A pipeline with no handles returns Healthy, so it stays in its slot.
        let mut supervisor = test_supervisor(2);
        let pipeline = CompactionPipeline::new(
            "task-1".to_string(),
            vec!["split-1".to_string()],
            TempDirectory::for_test(),
        );
        supervisor.pipelines[0] = Some(pipeline);
        supervisor.supervise().await;
        // Pipeline has no handles → Healthy → not reaped.
        assert!(supervisor.pipelines[0].is_some());
        assert_eq!(supervisor.num_completed_tasks, 0);
        assert_eq!(supervisor.num_failed_tasks, 0);
    }
}
