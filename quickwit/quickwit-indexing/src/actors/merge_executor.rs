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
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use fail::fail_point;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use quickwit_config::build_doc_mapper;
use quickwit_directories::UnionDirectory;
use quickwit_doc_mapper::{DocMapper, QUICKWIT_TOKENIZER_MANAGER};
use quickwit_metastore::{Metastore, SplitMetadata};
use quickwit_proto::metastore_api::DeleteTask;
use quickwit_proto::SearchRequest;
use tantivy::directory::{DirectoryClone, MmapDirectory, RamDirectory};
use tantivy::{Directory, Index, IndexMeta, SegmentId, SegmentReader, TantivyError};
use tokio::runtime::Handle;
use tracing::{debug, info, info_span, warn, Span};

use crate::actors::Packager;
use crate::controlled_directory::ControlledDirectory;
use crate::merge_policy::MergeOperationType;
use crate::models::{
    IndexedSplit, IndexedSplitBatch, IndexingPipelineId, MergeScratch, PublishLock,
    ScratchDirectory, SplitAttrs,
};

pub struct MergeExecutor {
    pipeline_id: IndexingPipelineId,
    metastore: Arc<dyn Metastore>,
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
        match merge_op.operation_type {
            MergeOperationType::Merge => {
                self.process_merge(
                    merge_op.merge_split_id.clone(),
                    merge_op.splits.clone(),
                    merge_scratch.tantivy_dirs,
                    merge_scratch.merge_scratch_directory,
                    ctx,
                )
                .await?;
            }
            MergeOperationType::DeleteAndMerge => {
                self.process_delete_and_merge(
                    merge_op.merge_split_id.clone(),
                    merge_op.splits.clone(),
                    merge_scratch.tantivy_dirs,
                    merge_scratch.merge_scratch_directory,
                    ctx,
                )
                .await?;
            }
        }
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

async fn merge_split_directories(
    union_index_meta: IndexMeta,
    split_directories: Vec<Box<dyn Directory>>,
    delete_tasks: Vec<DeleteTask>,
    doc_mapper_opt: Option<Arc<dyn DocMapper>>,
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

    let mut index_writer = union_index.writer_with_num_threads(1, 3_000_000)?;
    let num_delete_tasks = delete_tasks.len();
    if num_delete_tasks > 0 {
        let doc_mapper = doc_mapper_opt
            .ok_or_else(|| anyhow!("Doc mapper must be present if there are delete tasks."))?;
        for delete_task in delete_tasks {
            // TODO: currently tantivy only support deletes with a term query.
            // Until tantivy supports a delete on the `Query` trait, we delete all terms
            // present in the query so we can test simple queries.
            // WARNING: this is broken by design and should not go in production.
            let delete_query = delete_task
                .delete_query
                .expect("A delete task must have a delete query.");
            let search_request = SearchRequest {
                index_id: delete_query.index_id,
                query: delete_query.query,
                start_timestamp: delete_query.start_timestamp,
                end_timestamp: delete_query.end_timestamp,
                search_fields: delete_query.search_fields,
                ..Default::default()
            };
            debug!(
                "Delete all documents matched by query `{:?}`",
                search_request
            );
            let query = doc_mapper.query(union_index.schema(), &search_request)?;
            query.query_terms(&mut |term, _need_position| {
                index_writer.delete_term(term.clone());
            });
        }
        debug!("commit-delete-operations");
        index_writer.commit()?;
    }

    let segment_ids: Vec<SegmentId> = union_index
        .searchable_segment_metas()?
        .into_iter()
        .map(|segment_meta| segment_meta.id())
        .collect();

    // A merge is useless if there is no delete and only one segment.
    if num_delete_tasks == 0 && segment_ids.len() <= 1 {
        return Ok(output_directory);
    }

    debug!(segment_ids=?segment_ids,"merging-segments");
    // TODO it would be nice if tantivy could let us run the merge in the current thread.
    index_writer.merge(&segment_ids).wait()?;

    Ok(output_directory)
}

impl MergeExecutor {
    pub fn new(
        pipeline_id: IndexingPipelineId,
        metastore: Arc<dyn Metastore>,
        merge_packager_mailbox: Mailbox<Packager>,
    ) -> Self {
        MergeExecutor {
            pipeline_id,
            metastore,
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
            Vec::new(),
            None,
            merge_scratch_directory.path(),
            ctx,
        )
        .await?;
        fail_point!("after-merge-split");
        info!(
            elapsed_secs = start.elapsed().as_secs_f32(),
            "merge-success"
        );

        // This will have the side effect of deleting the directory containing the downloaded
        // splits.
        let time_range = merge_time_range(&splits);
        let uncompressed_docs_size_in_bytes = sum_doc_sizes_in_bytes(&splits);
        let num_docs = sum_num_docs(&splits);
        let delete_opstamp = splits
            .iter()
            .map(|split| split.delete_opstamp)
            .min()
            .unwrap_or(0);

        let merged_index = open_index(controlled_directory.clone())?;
        ctx.record_progress();

        let indexed_split = IndexedSplit {
            split_attrs: SplitAttrs {
                split_id: merge_split_id,
                partition_id,
                pipeline_id,
                replaced_split_ids,
                time_range,
                num_docs,
                uncompressed_docs_size_in_bytes,
                delete_opstamp,
            },
            index: merged_index,
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

    async fn process_delete_and_merge(
        &mut self,
        merge_split_id: String,
        mut splits: Vec<SplitMetadata>,
        tantivy_dirs: Vec<Box<dyn Directory>>,
        merge_scratch_directory: ScratchDirectory,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        assert_eq!(
            splits.len(),
            1,
            "Delete tasks can be applied only on one split."
        );
        assert_eq!(tantivy_dirs.len(), 1);
        let split = splits.pop().unwrap();
        let index_metadata = self.metastore.index_metadata(&split.index_id).await?;
        let doc_mapper = build_doc_mapper(
            &index_metadata.doc_mapping,
            &index_metadata.search_settings,
            &index_metadata.indexing_settings,
        )?;
        let delete_tasks = self
            .metastore
            .list_delete_tasks(&split.index_id, split.delete_opstamp)
            .await?;
        if delete_tasks.is_empty() {
            warn!(
                "No delete task found for split `{}` with `delete_optamp` = `{}`.",
                split.split_id(),
                split.delete_opstamp
            );
            return Ok(());
        }
        let last_delete_opstamp = delete_tasks
            .iter()
            .map(|delete_task| delete_task.opstamp)
            .max()
            .expect("There is at least one delete task.");
        info!(
            delete_opstamp_start = split.delete_opstamp,
            num_delete_tasks = delete_tasks.len()
        );

        info!("delete-task-start");
        let start = Instant::now();
        let (union_index_meta, split_directories) = open_split_directories(&tantivy_dirs)?;
        let controlled_directory = merge_split_directories(
            union_index_meta,
            split_directories,
            delete_tasks,
            Some(doc_mapper.clone()),
            merge_scratch_directory.path(),
            ctx,
        )
        .await?;
        info!(
            elapsed_secs = start.elapsed().as_secs_f32(),
            "delete-task-success"
        );

        // This will have the side effect of deleting the directory containing the downloaded split.
        let mut merged_index = Index::open(controlled_directory.clone())?;
        ctx.record_progress();
        merged_index.set_tokenizers(QUICKWIT_TOKENIZER_MANAGER.clone());

        ctx.record_progress();

        // Compute merged split attributes.
        let merged_segment = merged_index
            .searchable_segments()?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Delete operation should output one segment."))?;
        let merged_segment_reader = SegmentReader::open(&merged_segment)?;
        let num_docs = merged_segment_reader.num_docs() as u64;
        let uncompressed_docs_size_in_bytes = (num_docs as f32
            * split.uncompressed_docs_size_in_bytes as f32
            / split.num_docs as f32) as u64;
        let time_range = if let Some(ref timestamp_field_name) = doc_mapper.timestamp_field_name() {
            let timestamp_field = merged_segment_reader
                .schema()
                .get_field(timestamp_field_name)
                .ok_or_else(|| {
                    TantivyError::SchemaError(format!(
                        "Timestamp field `{}` does not exist",
                        timestamp_field_name
                    ))
                })?;
            let reader = merged_segment_reader.fast_fields().i64(timestamp_field)?;
            Some(RangeInclusive::new(reader.min_value(), reader.max_value()))
        } else {
            None
        };

        let index_pipeline_id = IndexingPipelineId {
            index_id: split.index_id.clone(),
            node_id: split.node_id.clone(),
            pipeline_ord: split.pipeline_ord,
            source_id: split.source_id.clone(),
        };
        let indexed_split = IndexedSplit {
            split_attrs: SplitAttrs {
                split_id: merge_split_id,
                partition_id: split.partition_id,
                pipeline_id: index_pipeline_id,
                replaced_split_ids: vec![split.split_id.clone()],
                time_range,
                num_docs,
                uncompressed_docs_size_in_bytes,
                delete_opstamp: last_delete_opstamp,
            },
            index: merged_index,
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
    use quickwit_actors::{create_test_mailbox, SpawnContext, Universe};
    use quickwit_common::split_file;
    use quickwit_metastore::SplitMetadata;
    use quickwit_proto::metastore_api::DeleteQuery;

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
        let test_sandbox = TestSandbox::create(
            &pipeline_id.index_id,
            doc_mapping_yaml,
            "{}",
            &["body"],
            None,
        )
        .await?;
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
            merge_operation: MergeOperation::new_merge_operation(split_metas),
            tantivy_dirs,
            merge_scratch_directory,
            downloaded_splits_directory,
        };
        let (merge_packager_mailbox, merge_packager_inbox) = create_test_mailbox();
        let merge_executor = MergeExecutor::new(pipeline_id, metastore, merge_packager_mailbox);
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
        assert_eq!(packager_msg.splits[0].split_attrs.num_docs, 4);
        assert_eq!(
            packager_msg.splits[0]
                .split_attrs
                .uncompressed_docs_size_in_bytes,
            136
        );
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

    #[tokio::test]
    async fn test_delete_and_merge_executor() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_id = "test-delete-and-merge-index";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: i64
                fast: true
        "#;
        let test_sandbox =
            TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"], None).await?;
        let docs = vec![
            serde_json::json!({"body": "info", "ts": 0 }),
            serde_json::json!({"body": "info", "ts": 0 }),
            serde_json::json!({"body": "delete", "ts": 0 }),
        ];
        test_sandbox.add_documents(docs).await?;
        let metastore = test_sandbox.metastore();
        metastore
            .create_delete_task(DeleteQuery {
                index_id: index_id.to_string(),
                start_timestamp: None,
                end_timestamp: None,
                query: "body:delete".to_string(),
                search_fields: Vec::new(),
            })
            .await?;
        let split_meta: SplitMetadata = metastore
            .list_all_splits(index_id)
            .await?
            .into_iter()
            .map(|split| split.split_metadata)
            .next()
            .unwrap();
        let expected_uncompressed_docs_size_in_bytes =
            (2_f32 * split_meta.uncompressed_docs_size_in_bytes as f32 / 3_f32) as u64;
        // let mut tantivy_dirs: Vec<Box<dyn Directory>> = vec![];
        // for split_meta in &split_metas {
        //     let split_filename = split_file(split_meta.split_id());
        //     let dest_filepath = downloaded_splits_directory.path().join(&split_filename);
        //     test_sandbox
        //         .storage()
        //         .copy_to_file(Path::new(&split_filename), &dest_filepath)
        //         .await?;

        //     tantivy_dirs.push(get_tantivy_directory_from_split_bundle(&dest_filepath).unwrap())
        // }
        // let merge_scratch = MergeScratch {
        //     merge_operation: MergeOperation::new_merge_operation(split_metas),
        //     tantivy_dirs,
        //     merge_scratch_directory,
        //     downloaded_splits_directory,
        // };
        let merge_scratch_directory = ScratchDirectory::for_test()?;
        let downloaded_splits_directory =
            merge_scratch_directory.named_temp_child("downloaded-splits-")?;
        let split_filename = split_file(split_meta.split_id());
        let dest_filepath = downloaded_splits_directory.path().join(&split_filename);
        println!("dest_filepath {:?}", dest_filepath);
        test_sandbox
            .storage()
            .copy_to_file(Path::new(&split_filename), &dest_filepath)
            .await?;
        let tantivy_dir = get_tantivy_directory_from_split_bundle(&dest_filepath).unwrap();
        let merge_scratch = MergeScratch {
            merge_operation: MergeOperation::new_delete_and_merge_operation(split_meta),
            tantivy_dirs: vec![tantivy_dir],
            merge_scratch_directory,
            downloaded_splits_directory,
        };
        let (merge_packager_mailbox, merge_packager_inbox) = create_test_mailbox();
        let index_pipeline_id = IndexingPipelineId {
            index_id: index_id.to_string(),
            node_id: "unknown".to_string(),
            pipeline_ord: 0,
            source_id: "unknown".to_string(),
        };
        let delete_task_executor =
            MergeExecutor::new(index_pipeline_id, metastore, merge_packager_mailbox);
        let universe = Universe::new();
        let (delete_task_executor_mailbox, delete_task_executor_handle) =
            universe.spawn_actor(delete_task_executor).spawn();
        delete_task_executor_mailbox
            .send_message(merge_scratch)
            .await?;
        delete_task_executor_handle
            .process_pending_and_observe()
            .await;
        let mut packager_msgs = merge_packager_inbox.drain_for_test();
        assert_eq!(packager_msgs.len(), 1);
        let packager_msg = packager_msgs
            .pop()
            .unwrap()
            .downcast::<IndexedSplitBatch>()
            .unwrap();
        assert_eq!(packager_msg.splits[0].split_attrs.num_docs, 2);
        assert_eq!(packager_msg.splits[0].split_attrs.delete_opstamp, 1);
        assert_eq!(
            packager_msg.splits[0]
                .split_attrs
                .uncompressed_docs_size_in_bytes,
            expected_uncompressed_docs_size_in_bytes,
        );
        let reader = packager_msg.splits[0].index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        Ok(())
    }
}
