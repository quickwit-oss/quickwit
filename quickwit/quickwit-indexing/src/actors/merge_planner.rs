// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_metastore::SplitMetadata;
use tracing::info;

use crate::actors::MergeSplitDownloader;
use crate::models::{IndexingPipelineId, NewSplits};
use crate::MergePolicy;

/// The merge planner decides when to start a merge task.
pub struct MergePlanner {
    pipeline_id: IndexingPipelineId,
    /// A young split is a split that has not reached maturity
    /// yet and can be candidate to merge operations.
    partitioned_young_splits: HashMap<u64, Vec<SplitMetadata>>,
    merge_policy: Arc<dyn MergePolicy>,
    merge_split_downloader_mailbox: Mailbox<MergeSplitDownloader>,
}

#[async_trait]
impl Actor for MergePlanner {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "MergePlanner".to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(0)
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        let target_partition_ids = self.partitioned_young_splits.keys().cloned().collect_vec();
        self.send_merge_ops(ctx, &target_partition_ids).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<NewSplits> for MergePlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewSplits,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let mut target_partition_ids = Vec::new();

        let partitioned_new_young_splits = message
            .new_splits
            .into_iter()
            .filter(|split| {
                let is_immature = !self.merge_policy.is_mature(split);
                if !is_immature {
                    info!(
                        split_id=%split.split_id(),
                        index_id=%self.pipeline_id.index_id,
                        source_id=%split.source_id,
                        pipeline_ord=%split.pipeline_ord,
                        num_docs=split.num_docs,
                        num_bytes=split.uncompressed_docs_size_in_bytes,
                        "Split is mature."
                    );
                }
                is_immature
            })
            .group_by(|split| split.partition_id);

        for (partition_id, new_young_splits) in &partitioned_new_young_splits {
            let young_splits = self
                .partitioned_young_splits
                .entry(partition_id)
                .or_default();
            young_splits.extend(new_young_splits);
            target_partition_ids.push(partition_id);
        }
        self.send_merge_ops(ctx, &target_partition_ids).await?;
        Ok(())
    }
}

impl MergePlanner {
    pub fn new(
        pipeline_id: IndexingPipelineId,
        published_splits: Vec<SplitMetadata>,
        merge_policy: Arc<dyn MergePolicy>,
        merge_split_downloader_mailbox: Mailbox<MergeSplitDownloader>,
    ) -> MergePlanner {
        let mut partitioned_young_splits: HashMap<u64, Vec<SplitMetadata>> = HashMap::new();
        for split in published_splits {
            if !belongs_to_pipeline(&pipeline_id, &split) || merge_policy.is_mature(&split) {
                continue;
            }
            partitioned_young_splits
                .entry(split.partition_id)
                .or_default()
                .push(split);
        }
        MergePlanner {
            pipeline_id,
            partitioned_young_splits,
            merge_policy,
            merge_split_downloader_mailbox,
        }
    }

    async fn send_merge_ops(
        &mut self,
        ctx: &ActorContext<Self>,
        target_partition_ids: &[u64],
    ) -> Result<(), ActorExitStatus> {
        for partition_id in target_partition_ids {
            if let Some(young_splits) = self.partitioned_young_splits.get_mut(partition_id) {
                let merge_operations = self.merge_policy.operations(young_splits);

                for merge_operation in merge_operations {
                    info!(merge_operation=?merge_operation, "Planned merge operation.");
                    ctx.send_message(&self.merge_split_downloader_mailbox, merge_operation)
                        .await?;
                }
            }
        }
        Ok(())
    }
}

fn belongs_to_pipeline(pipeline_id: &IndexingPipelineId, split: &SplitMetadata) -> bool {
    pipeline_id.source_id == split.source_id
        && pipeline_id.node_id == split.node_id
        && pipeline_id.pipeline_ord == split.pipeline_ord
}
