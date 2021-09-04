// Copyright (C) 2021 Quickwit, Inc.
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

use std::ops::RangeInclusive;
use std::path::Path;
use std::time::Instant;

use anyhow::Context;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Mailbox, SyncActor};
use quickwit_common::split_file;
use quickwit_directories::{BundleDirectory, UnionDirectory};
use quickwit_metastore::checkpoint::CheckpointDelta;
use quickwit_metastore::SplitMetadata;
use tantivy::directory::{DirectoryClone, MmapDirectory, RamDirectory};
use tantivy::{Directory, Index, IndexMeta};
use tracing::info;

use crate::merge_policy::MergeOrDemux;
use crate::models::{IndexedSplit, MergeScratch};
use crate::new_split_id;

pub struct MergeExecutor {
    index_id: String,
    merge_packager_mailbox: Mailbox<IndexedSplit>,
}

impl Actor for MergeExecutor {
    type Message = MergeScratch;

    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}
}

fn combine_index_meta(mut index_metas: Vec<IndexMeta>) -> anyhow::Result<IndexMeta> {
    let mut union_index_meta = index_metas.pop().with_context(|| "Only one IndexMeta")?;
    for index_meta in index_metas {
        union_index_meta.segments.extend(index_meta.segments);
    }
    Ok(union_index_meta)
}

fn open_union_index(scratch_path: &Path, splits: &[SplitMetadata]) -> anyhow::Result<Index> {
    let mmap_directory = MmapDirectory::open(scratch_path)?;
    let mut directories: Vec<Box<dyn Directory>> = Vec::new();
    let mut index_metas = Vec::new();
    for split in splits {
        let split_filename = split_file(&split.split_id);
        let split_fileslice = mmap_directory.open_read(Path::new(&split_filename))?;
        let split_directory = BundleDirectory::open_split(split_fileslice)?;
        directories.push(split_directory.box_clone());
        let index_meta = Index::open(split_directory)?.load_metas()?;
        index_metas.push(index_meta);
    }
    let union_index_meta = combine_index_meta(index_metas)?;
    let union_index_meta_json = serde_json::to_string_pretty(&union_index_meta)?;
    let ram_directory = RamDirectory::default();
    ram_directory.atomic_write(Path::new("meta.json"), union_index_meta_json.as_bytes())?;
    directories.push(Box::new(ram_directory));
    // This directory will receive the new merge splits.
    let mmap_directory = MmapDirectory::open(scratch_path)?;
    directories.push(Box::new(mmap_directory));
    directories.reverse();
    let union_directory = UnionDirectory::union_of(directories);
    let union_index = Index::open(union_directory)?;
    Ok(union_index)
}

impl SyncActor for MergeExecutor {
    fn process_message(
        &mut self,
        merge_scratch: MergeScratch,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        match merge_scratch.merge_operation.op_type {
            MergeOrDemux::Merge => self.process_merge(merge_scratch, ctx)?,
            MergeOrDemux::Demux => {
                // ...
                unimplemented!();
            }
        }
        Ok(())
    }
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
    splits.iter().map(|split| split.size_in_bytes).sum::<u64>()
}

fn sum_num_docs(splits: &[SplitMetadata]) -> u64 {
    splits.iter().map(|split| split.num_records as u64).sum()
}

impl MergeExecutor {
    pub fn new(index_id: String, merge_packager_mailbox: Mailbox<IndexedSplit>) -> Self {
        MergeExecutor {
            index_id,
            merge_packager_mailbox,
        }
    }

    fn process_merge(
        &mut self,
        merge_scratch: MergeScratch,
        ctx: &ActorContext<MergeScratch>,
    ) -> anyhow::Result<()> {
        let union_index = open_union_index(
            merge_scratch.merge_scratch_directory.path(),
            &merge_scratch.merge_operation.splits,
        )?;
        let index_writer = union_index.writer_with_num_threads(1, 10_000_000)?;
        let split_id = new_split_id();
        let time_range = merge_time_range(&merge_scratch.merge_operation.splits);
        let docs_size_in_bytes = sum_doc_sizes_in_bytes(&merge_scratch.merge_operation.splits);
        let num_docs = sum_num_docs(&merge_scratch.merge_operation.splits);
        info!(split_id=split_id.as_str(), index_id=self.index_id.as_str(), indexed_split=?"sending-merge-split-to-package");
        let indexed_split = IndexedSplit {
            split_id,
            index_id: self.index_id.clone(),
            time_range,
            num_docs,
            docs_size_in_bytes,
            // start_time is not very interesting here.
            start_time: Instant::now(),
            checkpoint_delta: CheckpointDelta::default(), //< TODO fixme
            index: union_index,
            index_writer,
            split_scratch_directory: merge_scratch.merge_scratch_directory,
        };
        ctx.send_message_blocking(&self.merge_packager_mailbox, indexed_split)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_index_config::DefaultIndexConfigBuilder;
    use quickwit_metastore::SplitMetadata;

    use super::*;
    use crate::merge_policy::{MergeOperation, MergeOrDemux};
    use crate::models::ScratchDirectory;
    use crate::TestSandbox;

    #[tokio::test]
    async fn test_merge_executor() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_config = r#"{
            "default_search_fields": ["body"],
            "timestamp_field": "ts",
            "tag_fields": [],
            "field_mappings": [
                { "name": "body", "type": "text" },
                { "name": "ts", "type": "i64", "fast": true }
            ]
        }"#;
        let index_config =
            Arc::new(serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?.build()?);
        let index_id = "test-index";
        let test_index_builder = TestSandbox::create(index_id, index_config).await?;
        for split_id in 0..4 {
            let docs = vec![
                serde_json::json!({"body ": format!("split{}", split_id), "ts": 1631072713 + split_id }),
            ];
            test_index_builder.add_documents(docs).await?;
        }
        let metastore = test_index_builder.metastore();
        let splits_with_footer_offsets = metastore.list_all_splits(index_id).await?;
        let splits: Vec<SplitMetadata> = splits_with_footer_offsets
            .into_iter()
            .map(|split_and_footer_offsets| split_and_footer_offsets.split_metadata)
            .collect();
        assert_eq!(splits.len(), 4);
        let merge_scratch_directory = ScratchDirectory::try_new_temp()?;
        let storage = test_index_builder.index_storage(index_id)?;

        for split in &splits {
            let split_filename = split_file(&split.split_id);
            let dest_filepath = merge_scratch_directory.path().join(&split_filename);
            storage
                .copy_to_file(Path::new(&split_filename), &dest_filepath)
                .await?;
        }
        let merge_scratch = MergeScratch {
            merge_operation: MergeOperation {
                splits,
                op_type: MergeOrDemux::Merge,
            },
            merge_scratch_directory,
        };
        let (merge_packager_mailbox, merge_packager_inbox) = create_test_mailbox();
        let merge_executor = MergeExecutor::new(index_id.to_string(), merge_packager_mailbox);
        let universe = Universe::new();
        let (merge_executor_mailbox, merge_executor_handle) =
            universe.spawn_actor(merge_executor).spawn_sync();
        universe
            .send_message(&merge_executor_mailbox, merge_scratch)
            .await?;
        merge_executor_handle.process_pending_and_observe().await;
        let mut packager_msgs = merge_packager_inbox.drain_available_message_for_test();
        assert_eq!(packager_msgs.len(), 1);
        let packager_msg = packager_msgs.pop().unwrap();
        assert_eq!(packager_msg.num_docs, 4);
        assert_eq!(packager_msg.docs_size_in_bytes, 136);

        let reader = packager_msg.index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 4);
        Ok(())
    }
}
