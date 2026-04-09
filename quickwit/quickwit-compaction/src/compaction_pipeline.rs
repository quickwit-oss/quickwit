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

use quickwit_actors::{ActorHandle, Health, Supervisable};
use quickwit_common::KillSwitch;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_indexing::actors::{
    MergeExecutor, MergeSplitDownloader, Packager, Publisher, Uploader,
};
use tracing::{debug, error};

pub struct CompactionPipelineHandles {
    pub merge_split_downloader: ActorHandle<MergeSplitDownloader>,
    pub merge_executor: ActorHandle<MergeExecutor>,
    pub merge_packager: ActorHandle<Packager>,
    pub merge_uploader: ActorHandle<Uploader>,
    pub merge_publisher: ActorHandle<Publisher>,
}

/// A single-use merge execution pipeline. Processes one merge task and
/// terminates.
///
/// Owned by the `CompactorSupervisor`, which periodically calls
/// `check_actor_health()` and acts on the result (retry, reap, etc.).
pub struct CompactionPipeline {
    pub task_id: String,
    pub split_ids: Vec<String>,
    pub retry_count: usize,
    pub kill_switch: KillSwitch,
    pub scratch_directory: TempDirectory,
    pub handles: Option<CompactionPipelineHandles>,
}

impl CompactionPipeline {
    pub fn new(task_id: String, split_ids: Vec<String>, scratch_directory: TempDirectory) -> Self {
        CompactionPipeline {
            task_id,
            split_ids,
            retry_count: 0,
            kill_switch: KillSwitch::default(),
            scratch_directory,
            handles: None,
        }
    }

    fn supervisables(&self) -> Vec<&dyn Supervisable> {
        let Some(handles) = &self.handles else {
            return Vec::new();
        };
        vec![
            &handles.merge_split_downloader,
            &handles.merge_executor,
            &handles.merge_packager,
            &handles.merge_uploader,
            &handles.merge_publisher,
        ]
    }

    /// Checks child actor health.
    ///
    /// `check_for_progress` controls whether stall detection is performed
    /// (actors that are alive but haven't recorded progress since last check).
    /// The supervisor controls the cadence of progress checks.
    ///
    /// Returns:
    /// - `Success` when all actors have completed (merge published).
    /// - `FailureOrUnhealthy` when any actor has died or stalled.
    /// - `Healthy` when actors are running and making progress.
    pub fn check_actor_health(&self) -> Health {
        if self.handles.is_none() {
            return Health::Healthy;
        }

        let mut healthy_actors: Vec<&str> = Vec::new();
        let mut failure_or_unhealthy_actors: Vec<&str> = Vec::new();
        let mut success_actors: Vec<&str> = Vec::new();

        for supervisable in self.supervisables() {
            match supervisable.check_health(true) {
                Health::Healthy => {
                    healthy_actors.push(supervisable.name());
                }
                Health::FailureOrUnhealthy => {
                    failure_or_unhealthy_actors.push(supervisable.name());
                }
                Health::Success => {
                    success_actors.push(supervisable.name());
                }
            }
        }

        if !failure_or_unhealthy_actors.is_empty() {
            error!(
                task_id=%self.task_id,
                healthy_actors=?healthy_actors,
                failed_or_unhealthy_actors=?failure_or_unhealthy_actors,
                success_actors=?success_actors,
                "compaction pipeline actor failure detected"
            );
            return Health::FailureOrUnhealthy;
        }
        if healthy_actors.is_empty() {
            debug!(task_id=%self.task_id, "all compaction pipeline actors completed");
            return Health::Success;
        }
        Health::Healthy
    }

    pub async fn terminate(&mut self) {
        self.kill_switch.kill();
        if let Some(handles) = self.handles.take() {
            tokio::join!(
                handles.merge_split_downloader.kill(),
                handles.merge_executor.kill(),
                handles.merge_packager.kill(),
                handles.merge_uploader.kill(),
                handles.merge_publisher.kill(),
            );
        }
    }

    /// Terminates the current actor chain, increments retry count, and
    /// re-spawns. Downloaded splits remain on disk in the scratch directory.
    pub async fn restart(&mut self) {
        self.terminate().await;
        self.retry_count += 1;
        self.spawn_pipeline();
    }

    /// Spawns the actor chain. Currently a no-op stub — actor chain
    /// construction will be implemented in a later PR.
    fn spawn_pipeline(&mut self) {
        // TODO: construct MergeSplitDownloader → MergeExecutor → Packager →
        // Uploader → Publisher actor chain and set self.handles.
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::Health;
    use quickwit_common::temp_dir::TempDirectory;

    use super::CompactionPipeline;

    fn test_pipeline() -> CompactionPipeline {
        CompactionPipeline::new(
            "test-task".to_string(),
            vec!["split-1".to_string(), "split-2".to_string()],
            TempDirectory::for_test(),
        )
    }

    #[test]
    fn test_pipeline_no_handles_is_healthy() {
        let pipeline = test_pipeline();
        assert!(pipeline.handles.is_none());
        assert_eq!(pipeline.check_actor_health(), Health::Healthy);
    }

    #[tokio::test]
    async fn test_pipeline_terminate_without_handles() {
        let mut pipeline = test_pipeline();
        // Should not panic when there are no handles.
        pipeline.terminate().await;
        assert!(pipeline.handles.is_none());
    }

    #[tokio::test]
    async fn test_pipeline_restart_increments_retry_count() {
        let mut pipeline = test_pipeline();
        assert_eq!(pipeline.retry_count, 0);
        pipeline.restart().await;
        assert_eq!(pipeline.retry_count, 1);
    }
}
