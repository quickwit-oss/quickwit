// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use async_trait::async_trait;
use bytesize::ByteSize;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_proto::metastore::MetastoreServiceClient;

use super::{MergePlanner, MergeSplitDownloader, Packager, Publisher, Uploader, UploaderType};
use crate::merge_policy::{MergeOperation, MergeTask};
use crate::{IndexingSplitStore, PublisherType};

struct ScoredMergeTask {
    score: u64,
    merge_task: MergeTask,
}

fn merge_operation_score(merge_op: &MergeOperation) -> u64 {
    // A good merge operation is one that reduce the number of splits the most,
    // while not being too expensive.
    let num_bytes: u64 = merge_op
        .splits
        .iter()
        .map(|split| split.footer_offsets.end)
        .sum();
    let num_splits: u64 = (merge_op.splits.len() as u64).checked_sub(1).unwrap_or(0);
    // We prefer to return an integer, to avoid struggling with floating point non-ord silliness.
    num_splits * (1u64 << 50) / (1u64 + num_bytes)
}

impl From<MergeTask> for ScoredMergeTask {
    fn from(merge_task: MergeTask) -> Self {
        let score = merge_operation_score(&merge_task.merge_operation);
        ScoredMergeTask { score, merge_task }
    }
}

impl Eq for ScoredMergeTask {}

impl PartialEq<Self> for ScoredMergeTask {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd<Self> for ScoredMergeTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredMergeTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score.cmp(&other.score).then_with(|| {
            self.merge_task
                .merge_operation
                .merge_split_id
                .cmp(&other.merge_task.merge_operation.merge_split_id)
        })
    }
}

pub struct MergeSchedulerService {
    merge_ops_to_execute: BinaryHeap<ScoredMergeTask>,
    num_running_merge_ops: usize,
    max_running_merge_ops: usize,
}

impl Actor for MergeSchedulerService {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}
}

#[derive(Clone)]
pub struct MergePipelineParams {
    pub indexing_directory: TempDirectory,
    pub metastore: MetastoreServiceClient,
    pub split_store: IndexingSplitStore,
    pub max_concurrent_split_uploads: usize, //< TODO share with the indexing pipeline.
    pub merge_max_io_num_bytes_per_sec: Option<ByteSize>,
    pub event_broker: EventBroker,
}

fn start_merge_pipeline(
    merge_planner_mailbox: Mailbox<MergePlanner>,
    params: MergePipelineParams,
    ctx: &ActorContext<MergeSchedulerService>,
) -> Mailbox<MergeSplitDownloader> {
    // Merge publisher
    let merge_publisher = Publisher::new(
        PublisherType::MergePublisher,
        params.metastore.clone(),
        Some(merge_planner_mailbox.clone()),
        None,
    );
    let (merge_publisher_mailbox, merge_publisher_handler) = ctx
        .spawn_actor()
        // .set_kill_switch(self.kill_switch.clone())
        .spawn(merge_publisher);

    // Merge uploader
    let merge_uploader = Uploader::new(
        UploaderType::MergeUploader,
        params.metastore.clone(),
        params.split_store.clone(),
        merge_publisher_mailbox.into(),
        params.max_concurrent_split_uploads,
        params.event_broker.clone(),
    );

    let (merge_uploader_mailbox, merge_uploader_handler) = ctx
        .spawn_actor()
        // .set_kill_switch(self.kill_switch.clone())
        .spawn(merge_uploader);

    // // Merge Packager
    let merge_packager = Packager::new("MergePackager", merge_uploader_mailbox);
    let (merge_packager_mailbox, merge_packager_handler) = ctx
        .spawn_actor()
        // .set_kill_switch(self.kill_switch.clone())
        .spawn(merge_packager);

    todo!()
    // let max_merge_write_throughput: f64 = self
    //     .params
    //     .merge_max_io_num_bytes_per_sec
    //     .as_ref()
    //     .map(|bytes_per_sec| bytes_per_sec.as_u64() as f64)
    //     .unwrap_or(f64::INFINITY);

    // let split_downloader_io_controls = IoControls::default()
    //     .set_throughput_limit(max_merge_write_throughput)
    //     .set_index_and_component(
    //         self.params.pipeline_id.index_uid.index_id(),
    //         "split_downloader_merge",
    //     );

    // // The merge and split download share the same throughput limiter.
    // // This is how cloning the `IoControls` works.
    // let merge_executor_io_controls = split_downloader_io_controls
    //     .clone()
    //     .set_index_and_component(self.params.pipeline_id.index_uid.index_id(), "merger");

    // let merge_executor = MergeExecutor::new(
    //     self.params.pipeline_id.clone(),
    //     self.params.metastore.clone(),
    //     self.params.doc_mapper.clone(),
    //     merge_executor_io_controls,
    //     merge_packager_mailbox,
    // );
    // let (merge_executor_mailbox, merge_executor_handler) = ctx
    //     .spawn_actor()
    //     .set_kill_switch(self.kill_switch.clone())
    //     .spawn(merge_executor);

    // let merge_split_downloader = MergeSplitDownloader {
    //     scratch_directory: self.params.indexing_directory.clone(),
    //     split_store: self.params.split_store.clone(),
    //     executor_mailbox: merge_executor_mailbox,
    //     io_controls: split_downloader_io_controls,
    // };
    // let (merge_split_downloader_mailbox, merge_split_downloader_handler) = ctx
    //     .spawn_actor()
    //     .set_kill_switch(self.kill_switch.clone())
    //     .set_backpressure_micros_counter(
    //         crate::metrics::INDEXER_METRICS
    //             .backpressure_micros
    //             .with_label_values([
    //                 self.params.pipeline_id.index_uid.index_id(),
    //                 "MergeSplitDownloader",
    //             ]),
    //     )
    //     .spawn(merge_split_downloader);
}

#[async_trait]
impl Handler<MergeTask> for MergeSchedulerService {
    type Reply = ();

    async fn handle(
        &mut self,
        merge_task: MergeTask,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let scored_merge_op = ScoredMergeTask::from(merge_task);
        self.merge_ops_to_execute.push(scored_merge_op);
        self.consider_starting_merge_op(ctx).await?;
        Ok(())
    }
}

impl MergeSchedulerService {
    async fn consider_starting_merge_op(&mut self, ctx: &ActorContext<Self>) -> anyhow::Result<()> {
        if self.num_running_merge_ops >= self.max_running_merge_ops {
            // No room to run merges at the moment.
            return Ok(());
        }
        let Some(ScoredMergeTask { merge_task, .. }) = self.merge_ops_to_execute.pop() else {
            return Ok(());
        };

        self.num_running_merge_ops += 1;

        // run merge op
        Ok(())
    }
}

#[derive(Copy, Debug, Clone)]
struct WakeUp;

#[async_trait]
impl Handler<WakeUp> for MergeSchedulerService {
    type Reply = ();

    async fn handle(
        &mut self,
        wake_up: WakeUp,
        ctx: &ActorContext<MergeSchedulerService>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        self.consider_starting_merge_op(ctx).await?;
        Ok(())
    }
}
