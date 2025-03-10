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
use quickwit_common::io::IoControls;
use quickwit_common::runtimes::RuntimeType;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_directories::UnionDirectory;
use quickwit_doc_mapper::DocMapper;
use quickwit_metastore::SplitMetadata;
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::metastore::{
    DeleteTask, ListDeleteTasksRequest, MarkSplitsForDeletionRequest, MetastoreService,
    MetastoreServiceClient,
};
use quickwit_proto::types::{NodeId, SplitId};
use quickwit_query::get_quickwit_fastfield_normalizer_manager;
use quickwit_query::query_ast::QueryAst;
use tantivy::directory::{Advice, DirectoryClone, MmapDirectory, RamDirectory};
use tantivy::index::SegmentId;
use tantivy::tokenizer::TokenizerManager;
use tantivy::{DateTime, Directory, Index, IndexMeta, IndexWriter, SegmentReader};
use tokio::runtime::Handle;
use tracing::{debug, error, info, instrument, warn};

use crate::actors::Packager;
use crate::controlled_directory::ControlledDirectory;
use crate::merge_policy::MergeOperationType;
use crate::models::{IndexedSplit, IndexedSplitBatch, MergeScratch, PublishLock, SplitAttrs};

#[derive(Clone)]
pub struct MergeExecutor {
    pipeline_id: MergePipelineId,
    metastore: MetastoreServiceClient,
    doc_mapper: Arc<DocMapper>,
    io_controls: IoControls,
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

    #[instrument(level = "info", name = "merge_executor", parent = merge_scratch.merge_task.merge_parent_span.id(), skip_all)]
    async fn handle(
        &mut self,
        merge_scratch: MergeScratch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let start = Instant::now();
        let merge_task = merge_scratch.merge_task;
        let indexed_split_opt: Option<IndexedSplit> = match merge_task.operation_type {
            MergeOperationType::Merge => {
                let merge_res = self
                    .process_merge(
                        merge_task.merge_split_id.clone(),
                        merge_task.splits.clone(),
                        merge_scratch.tantivy_dirs,
                        merge_scratch.merge_scratch_directory,
                        ctx,
                    )
                    .await;
                match merge_res {
                    Ok(indexed_split) => Some(indexed_split),
                    Err(err) => {
                        // A failure in a merge is a bit special.
                        //
                        // Instead of failing the pipeline, we just log it.
                        // The idea is to limit the risk associated with a potential split of death.
                        //
                        // Such a split is now not tracked by the merge planner and won't undergo a
                        // merge until the merge pipeline is restarted.
                        //
                        // With a merge policy that marks splits as mature after a day or so, this
                        // limits the noise associated to those failed
                        // merges.
                        error!(task=?merge_task, err=?err, "failed to merge splits");
                        return Ok(());
                    }
                }
            }
            MergeOperationType::DeleteAndMerge => {
                assert_eq!(
                    merge_task.splits.len(),
                    1,
                    "Delete tasks can be applied only on one split."
                );
                assert_eq!(merge_scratch.tantivy_dirs.len(), 1);
                let split_with_docs_to_delete = merge_task.splits[0].clone();
                self.process_delete_and_merge(
                    merge_task.merge_split_id.clone(),
                    split_with_docs_to_delete,
                    merge_scratch.tantivy_dirs,
                    merge_scratch.merge_scratch_directory,
                    ctx,
                )
                .await?
            }
        };
        if let Some(indexed_split) = indexed_split_opt {
            info!(
                merged_num_docs = %indexed_split.split_attrs.num_docs,
                elapsed_secs = %start.elapsed().as_secs_f32(),
                operation_type = %merge_task.operation_type,
                "merge-operation-success"
            );
            ctx.send_message(
                &self.merge_packager_mailbox,
                IndexedSplitBatch {
                    splits: vec![indexed_split],
                    checkpoint_delta_opt: Default::default(),
                    publish_lock: PublishLock::default(),
                    publish_token_opt: None,
                    batch_parent_span: merge_task.merge_parent_span.clone(),
                    merge_task_opt: Some(merge_task),
                },
            )
            .await?;
        } else {
            info!("no-splits-merged");
        }
        Ok(())
    }
}

fn combine_index_meta(mut index_metas: Vec<IndexMeta>) -> anyhow::Result<IndexMeta> {
    let mut union_index_meta = index_metas.pop().with_context(|| "only one IndexMeta")?;
    for index_meta in index_metas {
        union_index_meta.segments.extend(index_meta.segments);
    }
    Ok(union_index_meta)
}

fn open_split_directories(
    // Directories containing the splits to merge
    tantivy_dirs: &[Box<dyn Directory>],
    tokenizer_manager: &TokenizerManager,
) -> anyhow::Result<(IndexMeta, Vec<Box<dyn Directory>>)> {
    let mut directories: Vec<Box<dyn Directory>> = Vec::new();
    let mut index_metas = Vec::new();
    for tantivy_dir in tantivy_dirs {
        directories.push(tantivy_dir.clone());

        let index_meta = open_index(tantivy_dir.clone(), tokenizer_manager)?.load_metas()?;
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

fn merge_time_range(splits: &[SplitMetadata]) -> Option<RangeInclusive<DateTime>> {
    splits
        .iter()
        .flat_map(|split| split.time_range.clone())
        .flat_map(|time_range| vec![*time_range.start(), *time_range.end()].into_iter())
        .minmax()
        .into_option()
        .map(|(min_timestamp, max_timestamp)| {
            DateTime::from_timestamp_secs(min_timestamp)
                ..=DateTime::from_timestamp_secs(max_timestamp)
        })
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

fn combine_partition_ids_aux(partition_ids: impl IntoIterator<Item = u64>) -> u64 {
    let sorted_unique_partition_ids: BTreeSet<u64> = partition_ids.into_iter().collect();
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

pub fn merge_split_attrs(
    pipeline_id: MergePipelineId,
    merge_split_id: SplitId,
    splits: &[SplitMetadata],
) -> anyhow::Result<SplitAttrs> {
    let partition_id = combine_partition_ids_aux(splits.iter().map(|split| split.partition_id));
    let time_range: Option<RangeInclusive<DateTime>> = merge_time_range(splits);
    let uncompressed_docs_size_in_bytes = sum_doc_sizes_in_bytes(splits);
    let num_docs = sum_num_docs(splits);
    let replaced_split_ids: Vec<SplitId> = splits
        .iter()
        .map(|split| split.split_id().to_string())
        .collect();
    let delete_opstamp = splits
        .iter()
        .map(|split| split.delete_opstamp)
        .min()
        .unwrap_or(0);
    let doc_mapping_uid = splits
        .first()
        .ok_or_else(|| anyhow::anyhow!("attempted to merge zero splits"))?
        .doc_mapping_uid;
    if splits
        .iter()
        .any(|split| split.doc_mapping_uid != doc_mapping_uid)
    {
        anyhow::bail!("attempted to merge splits with different doc mapping uid");
    }
    Ok(SplitAttrs {
        node_id: pipeline_id.node_id.clone(),
        index_uid: pipeline_id.index_uid.clone(),
        source_id: pipeline_id.source_id.clone(),
        doc_mapping_uid,
        split_id: merge_split_id,
        partition_id,
        replaced_split_ids,
        time_range,
        num_docs,
        uncompressed_docs_size_in_bytes,
        delete_opstamp,
        num_merge_ops: max_merge_ops(splits) + 1,
    })
}

fn max_merge_ops(splits: &[SplitMetadata]) -> usize {
    splits
        .iter()
        .map(|split| split.num_merge_ops)
        .max()
        .unwrap_or(0)
}

impl MergeExecutor {
    pub fn new(
        pipeline_id: MergePipelineId,
        metastore: MetastoreServiceClient,
        doc_mapper: Arc<DocMapper>,
        io_controls: IoControls,
        merge_packager_mailbox: Mailbox<Packager>,
    ) -> Self {
        MergeExecutor {
            pipeline_id,
            metastore,
            doc_mapper,
            io_controls,
            merge_packager_mailbox,
        }
    }

    async fn process_merge(
        &mut self,
        merge_split_id: SplitId,
        splits: Vec<SplitMetadata>,
        tantivy_dirs: Vec<Box<dyn Directory>>,
        merge_scratch_directory: TempDirectory,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<IndexedSplit> {
        let (union_index_meta, split_directories) = open_split_directories(
            &tantivy_dirs,
            self.doc_mapper.tokenizer_manager().tantivy_manager(),
        )?;
        // TODO it would be nice if tantivy could let us run the merge in the current thread.
        fail_point!("before-merge-split");
        let controlled_directory = self
            .merge_split_directories(
                union_index_meta,
                split_directories,
                Vec::new(),
                None,
                merge_scratch_directory.path(),
                ctx,
            )
            .await?;
        fail_point!("after-merge-split");

        // This will have the side effect of deleting the directory containing the downloaded
        // splits.
        let merged_index = open_index(
            controlled_directory.clone(),
            self.doc_mapper.tokenizer_manager().tantivy_manager(),
        )?;
        ctx.record_progress();

        let split_attrs = merge_split_attrs(self.pipeline_id.clone(), merge_split_id, &splits)?;
        Ok(IndexedSplit {
            split_attrs,
            index: merged_index,
            split_scratch_directory: merge_scratch_directory,
            controlled_directory_opt: Some(controlled_directory),
        })
    }

    async fn process_delete_and_merge(
        &mut self,
        merge_split_id: SplitId,
        split: SplitMetadata,
        tantivy_dirs: Vec<Box<dyn Directory>>,
        merge_scratch_directory: TempDirectory,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<Option<IndexedSplit>> {
        let list_delete_tasks_request =
            ListDeleteTasksRequest::new(split.index_uid.clone(), split.delete_opstamp);
        let delete_tasks = ctx
            .protect_future(self.metastore.list_delete_tasks(list_delete_tasks_request))
            .await?
            .delete_tasks;
        if delete_tasks.is_empty() {
            warn!(
                "No delete task found for split `{}` with `delete_optamp` = `{}`.",
                split.split_id(),
                split.delete_opstamp
            );
            return Ok(None);
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

        let (union_index_meta, split_directories) = open_split_directories(
            &tantivy_dirs,
            self.doc_mapper.tokenizer_manager().tantivy_manager(),
        )?;
        let controlled_directory = self
            .merge_split_directories(
                union_index_meta,
                split_directories,
                delete_tasks,
                Some(self.doc_mapper.clone()),
                merge_scratch_directory.path(),
                ctx,
            )
            .await?;

        // This will have the side effect of deleting the directory containing the downloaded split.
        let mut merged_index = Index::open(controlled_directory.clone())?;
        ctx.record_progress();
        merged_index.set_tokenizers(
            self.doc_mapper
                .tokenizer_manager()
                .tantivy_manager()
                .clone(),
        );
        merged_index.set_fast_field_tokenizers(
            get_quickwit_fastfield_normalizer_manager()
                .tantivy_manager()
                .clone(),
        );

        ctx.record_progress();

        // Compute merged split attributes.
        let merged_segment =
            if let Some(segment) = merged_index.searchable_segments()?.into_iter().next() {
                segment
            } else {
                info!(
                    "All documents from split `{}` were deleted.",
                    split.split_id()
                );
                let mark_splits_for_deletion_request = MarkSplitsForDeletionRequest::new(
                    split.index_uid.clone(),
                    vec![split.split_id.clone()],
                );
                self.metastore
                    .mark_splits_for_deletion(mark_splits_for_deletion_request)
                    .await?;
                return Ok(None);
            };

        let merged_segment_reader = SegmentReader::open(&merged_segment)?;
        let num_docs = merged_segment_reader.num_docs() as u64;
        let uncompressed_docs_size_in_bytes = (num_docs as f32
            * split.uncompressed_docs_size_in_bytes as f32
            / split.num_docs as f32) as u64;
        let time_range = if let Some(timestamp_field_name) = self.doc_mapper.timestamp_field_name()
        {
            let reader = merged_segment_reader
                .fast_fields()
                .date(timestamp_field_name)?;
            Some(reader.min_value()..=reader.max_value())
        } else {
            None
        };
        let indexed_split = IndexedSplit {
            split_attrs: SplitAttrs {
                node_id: NodeId::new(split.node_id),
                index_uid: split.index_uid,
                source_id: split.source_id,
                doc_mapping_uid: split.doc_mapping_uid,
                split_id: merge_split_id,
                partition_id: split.partition_id,
                replaced_split_ids: vec![split.split_id.clone()],
                time_range,
                num_docs,
                uncompressed_docs_size_in_bytes,
                delete_opstamp: last_delete_opstamp,
                num_merge_ops: split.num_merge_ops,
            },
            index: merged_index,
            split_scratch_directory: merge_scratch_directory,
            controlled_directory_opt: Some(controlled_directory),
        };
        Ok(Some(indexed_split))
    }

    async fn merge_split_directories(
        &self,
        union_index_meta: IndexMeta,
        split_directories: Vec<Box<dyn Directory>>,
        delete_tasks: Vec<DeleteTask>,
        doc_mapper_opt: Option<Arc<DocMapper>>,
        output_path: &Path,
        ctx: &ActorContext<MergeExecutor>,
    ) -> anyhow::Result<ControlledDirectory> {
        let shadowing_meta_json_directory = create_shadowing_meta_json_directory(union_index_meta)?;

        // This directory is here to receive the merged split, as well as the final meta.json file.
        let output_directory = ControlledDirectory::new(
            Box::new(MmapDirectory::open_with_madvice(
                output_path,
                Advice::Sequential,
            )?),
            self.io_controls
                .clone()
                .set_kill_switch(ctx.kill_switch().clone())
                .set_progress(ctx.progress().clone()),
        );
        let mut directory_stack: Vec<Box<dyn Directory>> = vec![
            output_directory.box_clone(),
            Box::new(shadowing_meta_json_directory),
        ];
        directory_stack.extend(split_directories.into_iter());
        let union_directory = UnionDirectory::union_of(directory_stack);
        let union_index = open_index(
            union_directory,
            self.doc_mapper.tokenizer_manager().tantivy_manager(),
        )?;

        ctx.record_progress();
        let _protect_guard = ctx.protect_zone();

        let mut index_writer: IndexWriter = union_index.writer_with_num_threads(1, 15_000_000)?;
        let num_delete_tasks = delete_tasks.len();
        if num_delete_tasks > 0 {
            let doc_mapper = doc_mapper_opt
                .ok_or_else(|| anyhow!("doc mapper must be present if there are delete tasks"))?;
            for delete_task in delete_tasks {
                let delete_query = delete_task
                    .delete_query
                    .expect("A delete task must have a delete query.");
                let query_ast: QueryAst = serde_json::from_str(&delete_query.query_ast)
                    .context("invalid query_ast json")?;
                // We ignore the docmapper default fields when we consider delete query.
                // We reparse the query here defensively, but actually, it should already have been
                // done in the delete task rest handler.
                let parsed_query_ast = query_ast.parse_user_query(&[]).context("invalid query")?;
                debug!(
                    "Delete all documents matched by query `{:?}`",
                    parsed_query_ast
                );
                let (query, _) =
                    doc_mapper.query(union_index.schema(), &parsed_query_ast, false)?;
                index_writer.delete_query(query)?;
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

        // If after deletion there is no longer any document, don't try to merge.
        if num_delete_tasks != 0 && segment_ids.is_empty() {
            return Ok(output_directory);
        }

        debug!(segment_ids=?segment_ids,"merging-segments");
        // TODO it would be nice if tantivy could let us run the merge in the current thread.
        index_writer.merge(&segment_ids).await?;

        Ok(output_directory)
    }
}

fn open_index<T: Into<Box<dyn Directory>>>(
    directory: T,
    tokenizer_manager: &TokenizerManager,
) -> tantivy::Result<Index> {
    let mut index = Index::open(directory)?;
    index.set_tokenizers(tokenizer_manager.clone());
    index.set_fast_field_tokenizers(
        get_quickwit_fastfield_normalizer_manager()
            .tantivy_manager()
            .clone(),
    );
    Ok(index)
}

#[cfg(test)]
mod tests {
    use quickwit_actors::Universe;
    use quickwit_common::split_file;
    use quickwit_metastore::{
        ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata, StageSplitsRequestExt,
    };
    use quickwit_proto::metastore::{
        DeleteQuery, ListSplitsRequest, PublishSplitsRequest, StageSplitsRequest,
    };
    use serde_json::Value as JsonValue;
    use tantivy::{Document, ReloadPolicy, TantivyDocument};

    use super::*;
    use crate::merge_policy::{MergeOperation, MergeTask};
    use crate::{get_tantivy_directory_from_split_bundle, new_split_id, TestSandbox};

    #[tokio::test]
    async fn test_merge_executor() -> anyhow::Result<()> {
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: datetime
                input_formats:
                - unix_timestamp
                fast: true
            timestamp_field: ts
        "#;
        let test_sandbox =
            TestSandbox::create("test-index", doc_mapping_yaml, "", &["body"]).await?;
        for split_id in 0..4 {
            let single_doc = std::iter::once(
                serde_json::json!({"body ": format!("split{split_id}"), "ts": 1631072713u64 + split_id }),
            );
            test_sandbox.add_documents(single_doc).await?;
        }
        let metastore = test_sandbox.metastore();
        let index_uid = test_sandbox.index_uid();
        let list_splits_request = ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap();
        let split_metas: Vec<SplitMetadata> = metastore
            .list_splits(list_splits_request)
            .await
            .unwrap()
            .collect_splits_metadata()
            .await
            .unwrap();
        assert_eq!(split_metas.len(), 4);
        let merge_scratch_directory = TempDirectory::for_test();
        let downloaded_splits_directory =
            merge_scratch_directory.named_temp_child("downloaded-splits-")?;
        let mut tantivy_dirs: Vec<Box<dyn Directory>> = Vec::new();
        for split_meta in &split_metas {
            let split_filename = split_file(split_meta.split_id());
            let dest_filepath = downloaded_splits_directory.path().join(&split_filename);
            test_sandbox
                .storage()
                .copy_to_file(Path::new(&split_filename), &dest_filepath)
                .await?;
            tantivy_dirs.push(get_tantivy_directory_from_split_bundle(&dest_filepath).unwrap())
        }
        let merge_operation = MergeOperation::new_merge_operation(split_metas);
        let merge_task = MergeTask::from_merge_operation_for_test(merge_operation);
        let merge_scratch = MergeScratch {
            merge_task,
            tantivy_dirs,
            merge_scratch_directory,
            downloaded_splits_directory,
        };
        let pipeline_id = MergePipelineId {
            node_id: test_sandbox.node_id(),
            index_uid,
            source_id: test_sandbox.source_id(),
        };
        let (merge_packager_mailbox, merge_packager_inbox) =
            test_sandbox.universe().create_test_mailbox();
        let merge_executor = MergeExecutor::new(
            pipeline_id,
            test_sandbox.metastore(),
            test_sandbox.doc_mapper(),
            IoControls::default(),
            merge_packager_mailbox,
        );
        let (merge_executor_mailbox, merge_executor_handle) = test_sandbox
            .universe()
            .spawn_builder()
            .spawn(merge_executor);
        merge_executor_mailbox.send_message(merge_scratch).await?;
        merge_executor_handle.process_pending_and_observe().await;
        let packager_msgs: Vec<IndexedSplitBatch> = merge_packager_inbox.drain_for_test_typed();
        assert_eq!(packager_msgs.len(), 1);
        let split_attrs_after_merge = &packager_msgs[0].splits[0].split_attrs;
        assert_eq!(split_attrs_after_merge.num_docs, 4);
        assert_eq!(split_attrs_after_merge.uncompressed_docs_size_in_bytes, 136);
        assert_eq!(split_attrs_after_merge.num_merge_ops, 1);
        let reader = packager_msgs[0].splits[0]
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        test_sandbox.assert_quit().await;
        Ok(())
    }

    #[test]
    fn test_combine_partition_ids_singleton_unchanged() {
        assert_eq!(combine_partition_ids_aux([17]), 17);
    }

    #[test]
    fn test_combine_partition_ids_zero_has_an_impact() {
        assert_ne!(
            combine_partition_ids_aux([12u64, 0u64]),
            combine_partition_ids_aux([12u64])
        );
    }

    #[test]
    fn test_combine_partition_ids_depends_on_partition_id_set() {
        assert_eq!(
            combine_partition_ids_aux([12, 16, 12, 13]),
            combine_partition_ids_aux([12, 16, 13])
        );
    }

    #[test]
    fn test_combine_partition_ids_order_does_not_matter() {
        assert_eq!(
            combine_partition_ids_aux([7, 12, 13]),
            combine_partition_ids_aux([12, 13, 7])
        );
    }

    async fn aux_test_delete_and_merge_executor(
        index_id: &str,
        docs: Vec<JsonValue>,
        delete_query: &str,
        result_docs: Vec<JsonValue>,
    ) -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: datetime
                input_formats:
                - unix_timestamp
                fast: true
            timestamp_field: ts
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "", &["body"]).await?;
        test_sandbox.add_documents(docs).await?;
        let metastore = test_sandbox.metastore();
        let index_uid = test_sandbox.index_uid();
        metastore
            .create_delete_task(DeleteQuery {
                index_uid: Some(index_uid.clone()),
                start_timestamp: None,
                end_timestamp: None,
                query_ast: quickwit_query::query_ast::qast_json_helper(delete_query, &["body"]),
            })
            .await?;
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();

        // We want to test a delete on a split with num_merge_ops > 0.
        let mut new_split_metadata = splits[0].split_metadata.clone();
        new_split_metadata.split_id = new_split_id();
        new_split_metadata.num_merge_ops = 1;
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &new_split_metadata)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![new_split_metadata.split_id.to_string()],
            replaced_split_ids: vec![splits[0].split_metadata.split_id.to_string()],
            index_checkpoint_delta_json_opt: None,
            publish_token_opt: None,
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap();
        let expected_uncompressed_docs_size_in_bytes =
            (new_split_metadata.uncompressed_docs_size_in_bytes as f32 / 2_f32) as u64;
        let merge_scratch_directory = TempDirectory::for_test();
        let downloaded_splits_directory =
            merge_scratch_directory.named_temp_child("downloaded-splits-")?;
        let split_filename = split_file(splits[0].split_metadata.split_id());
        let new_split_filename = split_file(new_split_metadata.split_id());
        let dest_filepath = downloaded_splits_directory.path().join(&new_split_filename);
        test_sandbox
            .storage()
            .copy_to_file(Path::new(&split_filename), &dest_filepath)
            .await?;
        let tantivy_dir = get_tantivy_directory_from_split_bundle(&dest_filepath).unwrap();
        let merge_operation = MergeOperation::new_delete_and_merge_operation(new_split_metadata);
        let merge_task = MergeTask::from_merge_operation_for_test(merge_operation);
        let merge_scratch = MergeScratch {
            merge_task,
            tantivy_dirs: vec![tantivy_dir],
            merge_scratch_directory,
            downloaded_splits_directory,
        };
        let pipeline_id = MergePipelineId {
            node_id: test_sandbox.node_id(),
            index_uid: test_sandbox.index_uid(),
            source_id: test_sandbox.source_id(),
        };
        let universe = Universe::with_accelerated_time();
        let (merge_packager_mailbox, merge_packager_inbox) = universe.create_test_mailbox();
        let delete_task_executor = MergeExecutor::new(
            pipeline_id,
            metastore,
            test_sandbox.doc_mapper(),
            IoControls::default(),
            merge_packager_mailbox,
        );
        let (delete_task_executor_mailbox, delete_task_executor_handle) =
            universe.spawn_builder().spawn(delete_task_executor);
        delete_task_executor_mailbox
            .send_message(merge_scratch)
            .await?;
        delete_task_executor_handle
            .process_pending_and_observe()
            .await;

        let packager_msgs: Vec<IndexedSplitBatch> = merge_packager_inbox.drain_for_test_typed();
        if !result_docs.is_empty() {
            assert_eq!(packager_msgs.len(), 1);
            let split = &packager_msgs[0].splits[0];
            assert_eq!(split.split_attrs.num_docs, result_docs.len() as u64);
            assert_eq!(split.split_attrs.delete_opstamp, 1);
            // Delete operations do not update the num_merge_ops value.
            assert_eq!(split.split_attrs.num_merge_ops, 1);
            assert_eq!(
                split.split_attrs.uncompressed_docs_size_in_bytes,
                expected_uncompressed_docs_size_in_bytes,
            );
            let reader = split
                .index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);

            let documents_left = searcher
                .search(
                    &tantivy::query::AllQuery,
                    &tantivy::collector::TopDocs::with_limit(result_docs.len() + 1),
                )?
                .into_iter()
                .map(|(_, doc_address)| {
                    let doc: TantivyDocument = searcher.doc(doc_address).unwrap();
                    let doc_json = doc.to_json(searcher.schema());
                    serde_json::from_str(&doc_json).unwrap()
                })
                .collect::<Vec<JsonValue>>();

            assert_eq!(documents_left.len(), result_docs.len());
            for doc in &documents_left {
                assert!(result_docs.contains(doc));
            }
            for doc in &result_docs {
                assert!(documents_left.contains(doc));
            }
        } else {
            assert!(packager_msgs.is_empty());
            let metastore = test_sandbox.metastore();
            let index_uid = test_sandbox.index_uid();
            let splits = metastore
                .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
                .await
                .unwrap()
                .collect_splits()
                .await
                .unwrap();
            assert!(splits.iter().all(
                |split| split.split_state == quickwit_metastore::SplitState::MarkedForDeletion
            ));
        }
        test_sandbox.assert_quit().await;
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_and_merge_executor() -> anyhow::Result<()> {
        aux_test_delete_and_merge_executor(
            "test-delete-and-merge-index",
            vec![
                serde_json::json!({"body": "info", "ts": 1624928208 }),
                serde_json::json!({"body": "delete", "ts": 1634928208 }),
            ],
            "body:delete",
            vec![serde_json::json!({"body": ["info"], "ts": ["2021-06-29T00:56:48Z"] })],
        )
        .await
    }

    #[tokio::test]
    async fn test_delete_termset_and_merge_executor() -> anyhow::Result<()> {
        aux_test_delete_and_merge_executor(
            "test-delete-termset-and-merge-executor",
            vec![
                serde_json::json!({"body": "info", "ts": 1624928208 }),
                serde_json::json!({"body": "info", "ts": 1624928209 }),
                serde_json::json!({"body": "delete", "ts": 1634928208 }),
                serde_json::json!({"body": "delete", "ts": 1634928209 }),
            ],
            "body: IN [delete]",
            vec![
                serde_json::json!({"body": ["info"], "ts": ["2021-06-29T00:56:48Z"] }),
                serde_json::json!({"body": ["info"], "ts": ["2021-06-29T00:56:49Z"] }),
            ],
        )
        .await
    }

    #[tokio::test]
    async fn test_delete_all() -> anyhow::Result<()> {
        aux_test_delete_and_merge_executor(
            "test-delete-all",
            vec![
                serde_json::json!({"body": "delete", "ts": 1634928208 }),
                serde_json::json!({"body": "delete", "ts": 1634928209 }),
            ],
            "body:delete",
            Vec::new(),
        )
        .await
    }
}
