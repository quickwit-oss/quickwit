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

//! Shared infrastructure for indexing pipeline supervisors (logs and metrics).

use std::time::Duration;

use tokio::sync::Semaphore;

/// Cap on the retry wait. Beyond this, the `MAX_PIPELINE_FAILURES` counter is the failsafe
/// that eventually forces a pod restart.
pub(crate) const MAX_RETRY_DELAY: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(300) // 5 min.
};

/// Base duration for the exponential-backoff retry schedule.
pub(crate) const RETRY_BASE_DURATION: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(10)
} else {
    Duration::from_secs(1)
};

/// Maximum number of failures (either spawn errors or runtime unhealthy transitions) a single
/// pipeline may accumulate before the process exits to force a pod restart. A stuck pipeline is
/// treated as a signal that the pod itself has accumulated state (e.g. dead fetch streams,
/// stale chitchat view, invalid publish tokens) that only a fresh process will clear.
pub(crate) const MAX_PIPELINE_FAILURES: usize = 10;

pub(crate) const SUPERVISE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub(crate) struct SuperviseLoop;

/// Calculates the wait time based on retry count.
// retry_count, wait_time (prod: base 1s, cap 5min)
// 0   1s
// 1   2s
// 2   4s
// 3   8s
// ...
// 8   256s
// >=9 300s (cap)
pub(crate) fn wait_duration_before_retry(retry_count: usize) -> Duration {
    // Protect against a `retry_count` that will lead to an overflow.
    let max_power = (retry_count as u32).min(31);
    (RETRY_BASE_DURATION * 2u32.pow(max_power)).min(MAX_RETRY_DELAY)
}

/// Spawning an indexing pipeline puts a lot of pressure on the file system, metastore, etc. so
/// we rely on this semaphore to limit the number of indexing pipelines that can be spawned
/// concurrently.
/// See also <https://github.com/quickwit-oss/quickwit/issues/1638>.
pub(crate) static SPAWN_PIPELINE_SEMAPHORE: Semaphore = Semaphore::const_new(10);

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Spawn {
    pub(crate) retry_count: usize,
}

// ---------------------------------------------------------------------------
// Pipeline trait — type-erased handle for any indexing pipeline actor
// ---------------------------------------------------------------------------

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorExitStatus, ActorHandle, ActorState, DeferableReplyHandler, Health, Mailbox,
    Observation, SendError, Supervisable,
};
use quickwit_proto::indexing::IndexingPipelineId;

use crate::models::IndexingStatistics;
use crate::source::AssignShards;

/// Trait that abstracts over the concrete pipeline actor type
/// (`IndexingPipeline` or `MetricsPipeline`). This allows `PipelineHandle`
/// to hold a single `Box<dyn PipelineHandle>`.
#[async_trait]
pub trait PipelineHandle: Send + Sync {
    fn indexing_pipeline_id(&self) -> &IndexingPipelineId;
    fn state(&self) -> ActorState;
    fn refresh_observe(&self);
    fn last_observation(&self) -> IndexingStatistics;
    fn check_health(&self, check_for_progress: bool) -> Health;
    async fn send_assign_shards(&self, message: AssignShards) -> Result<(), SendError>;
    async fn observe(&self) -> Observation<IndexingStatistics>;
    async fn join(self: Box<Self>) -> (ActorExitStatus, IndexingStatistics);
    async fn quit(self: Box<Self>) -> (ActorExitStatus, IndexingStatistics);
    async fn kill(self: Box<Self>);
}

/// Generic wrapper that implements `PipelineHandle` for any actor with the right
/// observable state and message handlers.
pub(crate) struct ActorPipeline<A: Actor<ObservableState = IndexingStatistics>> {
    pub pipeline_id: IndexingPipelineId,
    pub mailbox: Mailbox<A>,
    pub handle: ActorHandle<A>,
}

#[async_trait]
impl<A> PipelineHandle for ActorPipeline<A>
where A: Actor<ObservableState = IndexingStatistics> + DeferableReplyHandler<AssignShards>
{
    fn indexing_pipeline_id(&self) -> &IndexingPipelineId {
        &self.pipeline_id
    }

    fn state(&self) -> ActorState {
        self.handle.state()
    }

    fn refresh_observe(&self) {
        self.handle.refresh_observe();
    }

    fn last_observation(&self) -> IndexingStatistics {
        self.handle.last_observation().clone()
    }

    fn check_health(&self, check_for_progress: bool) -> Health {
        self.handle.check_health(check_for_progress)
    }

    async fn send_assign_shards(&self, message: AssignShards) -> Result<(), SendError> {
        self.mailbox.send_message(message).await?;
        Ok(())
    }

    async fn observe(&self) -> Observation<IndexingStatistics> {
        self.handle.observe().await
    }

    async fn join(self: Box<Self>) -> (ActorExitStatus, IndexingStatistics) {
        self.handle.join().await
    }

    async fn quit(self: Box<Self>) -> (ActorExitStatus, IndexingStatistics) {
        self.handle.quit().await
    }

    async fn kill(self: Box<Self>) {
        let _ = self.handle.kill().await;
    }
}
