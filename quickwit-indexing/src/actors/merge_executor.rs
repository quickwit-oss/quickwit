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

use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::path::Path;
use std::time::Instant;

use anyhow::Context;
use async_trait::async_trait;
use fail::fail_point;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use quickwit_directories::UnionDirectory;
use quickwit_doc_mapper::QUICKWIT_TOKENIZER_MANAGER;
use quickwit_metastore::SplitMetadata;
use tantivy::directory::{DirectoryClone, MmapDirectory, RamDirectory};
use tantivy::{Directory, Index, IndexMeta, SegmentId};
use tokio::runtime::Handle;
use tracing::{debug, info, info_span, Span};

use crate::actors::Packager;
use crate::controlled_directory::ControlledDirectory;
use crate::models::{
    IndexedSplit, IndexedSplitBatch, IndexingPipelineId, MergeScratch, PublishLock,
    ScratchDirectory,
};

pub struct MergeExecutor {
    pipeline_id: IndexingPipelineId,
    merge_packager_mailbox: Mailbox<Packager>,
}

#[async_trait]
impl Actor for MergeExecutor {
    type ObservableState = ();

    fn runtime_handle(&self) -> Handle {
        RuntimeType::Blocking.get_runtime_handle()
    }

    fn observable_state(&self) -> Self::ObservableState {}

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(1)
    }

    fn name(&self) -> String {
        "MergeExecutor".to_string()
    }
}

#[async_trait]
impl Handler<MergeScratch> for MergeExecutor {
    type Reply = ();

    fn message_span(&self, msg_id: u64, merge_scratch: &MergeScratch) -> Span {
        let merge_op = &merge_scratch.merge_operation;
        let num_docs: usize = merge_op
            .splits_as_slice()
            .iter()
            .map(|split| split.num_docs)
            .sum();
        let in_merge_split_ids: Vec<String> = merge_op
            .splits_as_slice()
            .iter()
            .map(|split| split.split_id().to_string())
            .collect();
        info_span!("merge",
                    msg_id=&msg_id,
                    dir=%merge_scratch.merge_scratch_directory.path().display(),
                    merge_split_id=%merge_op.merge_split_id,
                    in_merge_split_ids=?in_merge_split_ids,
                    num_docs=num_docs,
                    num_splits=merge_op.splits_as_slice().len())
    }

    async fn handle(
        &mut self,
        merge_scratch: MergeScratch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let merge_op = &merge_scratch.merge_operation;
        self.process_merge(
            merge_op.merge_split_id.clone(),
            merge_op.splits.clone(),
            merge_scratch.tantivy_dirs,
            merge_scratch.merge_scratch_directory,
            ctx,
        )
        .await?;
        Ok(())
    }
}

fn combine_index_meta(mut index_metas: Vec<IndexMeta>) -> anyhow::Result<IndexMeta> {
    let mut union_index_meta = index_metas.pop().with_context(|| "Only one IndexMeta")?;
    for index_meta in index_metas {
        union_index_meta.segments.extend(index_meta.segments);
    }
    Ok(union_index_meta)
}

fn open_split_directories(
    // Directories containing the splits to merge
    tantivy_dirs: &[Box<dyn Directory>],
) -> anyhow::Result<(IndexMeta, Vec<Box<dyn Directory>>)> {
    let mut directories: Vec<Box<dyn Directory>> = Vec::new();
    let mut index_metas = Vec::new();
    for tantivy_dir in tantivy_dirs {
        directories.push(tantivy_dir.clone());

        let index_meta = open_index(tantivy_dir.clone())?.load_metas()?;
        index_metas.push(index_meta);
    }
    let union_index_meta = combine_index_meta(index_metas)?;
    Ok((union_index_meta, directories))
}

/// Creates a directory with a single `meta.json` file describe in `index_meta`
fn create_shadowing_meta_json_directory(index_meta: IndexMeta) -> anyhow::Result<RamDirectory> {
    let union_index_meta_json = serde_json::to_string_pretty(&index_meta)?;
    let ram_directory = RamDirectory::default();
    ram_directory.atomic_write(Path::new("meta.json"), union_index_meta_json.as_bytes())?;
    Ok(ram_directory)
}

fn merge_time_range(splits: &[SplitMetadata]) -> Option<RangeInclusive<i64>> {
    splits
        .iter()
        .flat_map(|split| split.time_range.clone())
        .flat_map(|time_range| vec![*time_range.start(), *time_range.end()].into_iter())
        .minmax()
        .into_option()
        .map(|(min_timestamp, max_timestamp)| min_timestamp..=max_timestamp)
}

fn sum_doc_sizes_in_bytes(splits: &[SplitMetadata]) -> u64 {
    splits
        .iter()
        .map(|split| split.uncompressed_docs_size_in_bytes)
        .sum::<u64>()
}

fn sum_num_docs(splits: &[SplitMetadata]) -> u64 {
    splits.iter().map(|split| split.num_docs as u64).sum()
}

fn merge_all_segments(index: &Index) -> anyhow::Result<()> {
    let segment_ids: Vec<SegmentId> = index
        .searchable_segment_metas()?
        .into_iter()
        .map(|segment_meta| segment_meta.id())
        .collect();
    if segment_ids.len() <= 1 {
        return Ok(());
    }
    debug!(segment_ids=?segment_ids,"merging-segments");
    let mut index_writer = index.writer_with_num_threads(1, 10_000_000)?;
    // TODO it would be nice if tantivy could let us run the merge in the current thread.
    index_writer.merge(&segment_ids).wait()?;
    Ok(())
}

/// Following Boost's hash_combine.
fn combine_two_hashes(lhs: u64, rhs: u64) -> u64 {
    let update_to_xor = rhs
        .wrapping_add(0x9e3779b9)
        .wrapping_add(lhs << 6)
        .wrapping_add(lhs >> 2);
    lhs ^ update_to_xor
}

fn combine_partition_ids_aux(partition_ids: impl Iterator<Item = u64>) -> u64 {
    let sorted_unique_partition_ids: BTreeSet<u64> = partition_ids.collect();
    let mut sorted_unique_partition_ids_it = sorted_unique_partition_ids.into_iter();
    if let Some(partition_id) = sorted_unique_partition_ids_it.next() {
        sorted_unique_partition_ids_it.fold(partition_id, |acc, partition_id| {
            combine_two_hashes(acc, partition_id)
        })
    } else {
        // This is not forbidden but this should never happen.
        0u64
    }
}

pub fn combine_partition_ids(splits: &[SplitMetadata]) -> u64 {
    combine_partition_ids_aux(splits.iter().map(|split| split.partition_id))
}

fn merge_split_directories(
    union_index_meta: IndexMeta,
    split_directories: Vec<Box<dyn Directory>>,
    output_path: &Path,
    ctx: &ActorContext<MergeExecutor>,
) -> anyhow::Result<ControlledDirectory> {
    let shadowing_meta_json_directory = create_shadowing_meta_json_directory(union_index_meta)?;
    // This directory is here to receive the merged split, as well as the final meta.json file.
    let output_directory = ControlledDirectory::new(
        Box::new(MmapDirectory::open(output_path)?),
        ctx.progress().clone(),
        ctx.kill_switch().clone(),
    );
    let mut directory_stack: Vec<Box<dyn Directory>> = vec![
        output_directory.box_clone(),
        Box::new(shadowing_meta_json_directory),
    ];
    directory_stack.extend(split_directories.into_iter());
    let union_directory = UnionDirectory::union_of(directory_stack);
    let union_index = open_index(union_directory)?;
    ctx.record_progress();
    let _protect_guard = ctx.protect_zone();
    merge_all_segments(&union_index)?;
    Ok(output_directory)
}

impl MergeExecutor {
    pub fn new(pipeline_id: IndexingPipelineId, merge_packager_mailbox: Mailbox<Packager>) -> Self {
        MergeExecutor {
            pipeline_id,
            merge_packager_mailbox,
        }
    }

    async fn process_merge(
        &mut self,
        merge_split_id: String,
        splits: Vec<SplitMetadata>,
        tantivy_dirs: Vec<Box<dyn Directory>>,
        merge_scratch_directory: ScratchDirectory,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        let pipeline_id = self.pipeline_id.clone();
        let start = Instant::now();
        info!("merge-start");
        let partition_id = combine_partition_ids_aux(splits.iter().map(|split| split.partition_id));
        let replaced_split_ids: Vec<String> = splits
            .iter()
            .map(|split| split.split_id().to_string())
            .collect();
        let (union_index_meta, split_directories) = open_split_directories(&tantivy_dirs)?;
        // TODO it would be nice if tantivy could let us run the merge in the current thread.
        fail_point!("before-merge-split");
        let controlled_directory = merge_split_directories(
            union_index_meta,
            split_directories,
            merge_scratch_directory.path(),
            ctx,
        )?;
        fail_point!("after-merge-split");
        info!(
            elapsed_secs = start.elapsed().as_secs_f32(),
            "merge-success"
        );

        // This will have the side effect of deleting the directory containing the downloaded
        // splits.
        let time_range = merge_time_range(&splits);
        let docs_size_in_bytes = sum_doc_sizes_in_bytes(&splits);
        let num_docs = sum_num_docs(&splits);

        let merged_index = open_index(controlled_directory.clone())?;
        ctx.record_progress();
        let index_writer = merged_index.writer_with_num_threads(1, 3_000_000)?;
        ctx.record_progress();

        let indexed_split = IndexedSplit {
            split_id: merge_split_id,
            partition_id,
            pipeline_id,
            replaced_split_ids,
            time_range,
            num_docs,
            docs_size_in_bytes,
            index: merged_index,
            index_writer,
            split_scratch_directory: merge_scratch_directory,
            controlled_directory_opt: Some(controlled_directory),
        };

        ctx.send_message(
            &self.merge_packager_mailbox,
            IndexedSplitBatch {
                splits: vec![indexed_split],
                checkpoint_delta: Default::default(),
                publish_lock: PublishLock::default(),
                date_of_birth: start,
            },
        )
        .await?;
        Ok(())
    }
}

fn open_index<T: Into<Box<dyn Directory>>>(directory: T) -> tantivy::Result<Index> {
    let mut index = Index::open(directory)?;
    index.set_tokenizers(QUICKWIT_TOKENIZER_MANAGER.clone());
    Ok(index)
}

#[cfg(test)]
mod tests {
    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_common::split_file;
    use quickwit_metastore::SplitMetadata;

    use super::*;
    use crate::merge_policy::MergeOperation;
    use crate::models::{IndexingPipelineId, ScratchDirectory};
    use crate::{get_tantivy_directory_from_split_bundle, TestSandbox};

    #[tokio::test]
    async fn test_merge_executor() -> anyhow::Result<()> {
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: i64
                fast: true
        "#;
        let test_sandbox =
            TestSandbox::create(&pipeline_id.index_id, doc_mapping_yaml, "{}", &["body"]).await?;
        for split_id in 0..4 {
            let docs = vec![
                serde_json::json!({"body ": format!("split{}", split_id), "ts": 1631072713u64 + split_id }),
            ];
            test_sandbox.add_documents(docs).await?;
        }
        let metastore = test_sandbox.metastore();
        let split_metas: Vec<SplitMetadata> = metastore
            .list_all_splits(&pipeline_id.index_id)
            .await?
            .into_iter()
            .map(|split| split.split_metadata)
            .collect();
        assert_eq!(split_metas.len(), 4);
        let merge_scratch_directory = ScratchDirectory::for_test()?;
        let downloaded_splits_directory =
            merge_scratch_directory.named_temp_child("downloaded-splits-")?;
        let mut tantivy_dirs: Vec<Box<dyn Directory>> = vec![];
        for split_meta in &split_metas {
            let split_filename = split_file(split_meta.split_id());
            let dest_filepath = downloaded_splits_directory.path().join(&split_filename);
            test_sandbox
                .storage()
                .copy_to_file(Path::new(&split_filename), &dest_filepath)
                .await?;

            tantivy_dirs.push(get_tantivy_directory_from_split_bundle(&dest_filepath).unwrap())
        }
        let merge_scratch = MergeScratch {
            merge_operation: MergeOperation {
                merge_split_id: crate::new_split_id(),
                splits: split_metas,
            },
            tantivy_dirs,
            merge_scratch_directory,
            downloaded_splits_directory,
        };
        let (merge_packager_mailbox, merge_packager_inbox) = create_test_mailbox();
        let merge_executor = MergeExecutor::new(pipeline_id, merge_packager_mailbox);
        let universe = Universe::new();
        let (merge_executor_mailbox, merge_executor_handle) =
            universe.spawn_actor(merge_executor).spawn();
        merge_executor_mailbox.send_message(merge_scratch).await?;
        merge_executor_handle.process_pending_and_observe().await;
        let mut packager_msgs = merge_packager_inbox.drain_for_test();
        assert_eq!(packager_msgs.len(), 1);
        let packager_msg = packager_msgs
            .pop()
            .unwrap()
            .downcast::<IndexedSplitBatch>()
            .unwrap();
        assert_eq!(packager_msg.splits[0].num_docs, 4);
        assert_eq!(packager_msg.splits[0].docs_size_in_bytes, 136);
        let reader = packager_msg.splits[0].index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        Ok(())
    }

    #[test]
    fn test_combine_partition_ids_singleton_unchanged() {
        assert_eq!(combine_partition_ids_aux([17].into_iter()), 17);
    }

    #[test]
    fn test_combine_partition_ids_zero_has_an_impact() {
        assert_ne!(
            combine_partition_ids_aux([12u64, 0u64].into_iter()),
            combine_partition_ids_aux([12u64].into_iter())
        );
    }

    #[test]
    fn test_combine_partition_ids_depends_on_partition_id_set() {
        assert_eq!(
            combine_partition_ids_aux([12, 16, 12, 13].into_iter()),
            combine_partition_ids_aux([12, 16, 13].into_iter())
        );
    }

    #[test]
    fn test_combine_partition_ids_order_does_not_matter() {
        assert_eq!(
            combine_partition_ids_aux([7, 12, 13].into_iter()),
            combine_partition_ids_aux([12, 13, 7].into_iter())
        );
    }
}
