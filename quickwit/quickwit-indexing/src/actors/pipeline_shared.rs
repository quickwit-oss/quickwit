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

pub(crate) const SUPERVISE_INTERVAL: Duration = Duration::from_secs(1);

const MAX_RETRY_DELAY: Duration = Duration::from_mins(10);

#[derive(Debug)]
pub(crate) struct SuperviseLoop;

/// Calculates the wait time based on retry count.
// retry_count, wait_time
// 0   1s
// 1   2s
// 2   4s
// 3   8s
// ...
// >=8   5mn
pub(crate) fn wait_duration_before_retry(retry_count: usize) -> Duration {
    // Protect against a `retry_count` that will lead to an overflow.
    let max_power = (retry_count as u32).min(31);
    Duration::from_secs(2u64.pow(max_power)).min(MAX_RETRY_DELAY)
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

/// Asks a pipeline to drain and shut itself down: the source stops emitting,
/// and once the in-flight batches are flushed, published, and settled
/// (acknowledged), the source — and with it the whole pipeline — exits with
/// success on its own. Past the drain deadline (commit timeout +
/// [`DRAIN_GRACE_MARGIN`]), the pipeline gives up, kills its actors, and
/// still exits: delivery degrades to at-least-once. A draining pipeline is
/// never respawned. Fire-and-forget: watch the pipeline actor's state to know
/// when it is done.
///
/// Only sources opting in through
/// [`crate::source::Source::should_be_drained`] actually drain: for the
/// others the pipeline kills its actors and exits immediately, keeping the
/// exact teardown semantics they had before drains existed.
#[derive(Debug)]
pub struct DrainPipeline;

/// Extra time granted to a draining pipeline on top of the commit timeout,
/// covering the upload and publication of the final splits.
pub(crate) const DRAIN_GRACE_MARGIN: Duration = Duration::from_secs(30);

/// Drain progress of a pipeline supervisor (see [`DrainPipeline`]): both
/// pipeline flavors share the deadline bookkeeping so they cannot diverge.
#[derive(Default)]
pub(crate) struct DrainState {
    deadline_opt: Option<Instant>,
}

impl DrainState {
    /// Whether a drain was initiated. A draining pipeline must never respawn.
    pub(crate) fn is_draining(&self) -> bool {
        self.deadline_opt.is_some()
    }

    /// Records the drain deadline and asks the source to drain. If the source
    /// is already dead, the pipeline health check terminates the draining
    /// pipeline instead of respawning it.
    pub(crate) async fn start(
        &mut self,
        source_mailbox: &Mailbox<SourceActor>,
        commit_timeout: Duration,
    ) {
        self.deadline_opt = Some(Instant::now() + commit_timeout + DRAIN_GRACE_MARGIN);
        let _ = source_mailbox.send_message(Drain).await;
    }

    /// Whether the draining pipeline ran out of time to settle: the caller
    /// kills whatever is left and exits.
    pub(crate) fn is_deadline_exceeded(&self) -> bool {
        self.deadline_opt
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Pipeline trait — type-erased handle for any indexing pipeline actor
// ---------------------------------------------------------------------------

use std::time::Instant;

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorExitStatus, ActorHandle, ActorState, DeferableReplyHandler, Health, Mailbox,
    Observation, SendError, Supervisable,
};
use quickwit_proto::indexing::IndexingPipelineId;

use crate::models::IndexingStatistics;
use crate::source::{AssignShards, Drain, SourceActor};

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
    /// See [`DrainPipeline`]. Fire-and-forget: the pipeline exits on its own.
    async fn start_drain(&self);
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
where A: Actor<ObservableState = IndexingStatistics>
        + DeferableReplyHandler<AssignShards>
        + DeferableReplyHandler<DrainPipeline, Reply = ()>
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

    async fn start_drain(&self) {
        // The pipeline mailbox is unbounded, so this should not block; a send
        // error means the pipeline is already dead, hence already settled.
        let _ = self.mailbox.send_message(DrainPipeline).await;
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
