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

//! Mature merge: merge fully-mature splits across all nodes of an index.
//!
//! Unlike the standard `MergePipeline`, this module:
//! - Considers only *mature* splits (i.e., splits that are past their maturation period).
//! - Has no node-id restriction — it can merge splits originally created on different nodes.
//! - Is driven by a simple one-shot batch run, not a reactive actor loop.
//!
//! Entry point: [`merge_mature_all_indexes`].

use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use quickwit_actors::{ActorExitStatus, Universe};
use quickwit_common::io::IoControls;
use quickwit_common::{KillSwitch, temp_dir};
use quickwit_config::build_doc_mapper;
use quickwit_metastore::{
    IndexMetadata, ListIndexesMetadataResponseExt, ListSplitsQuery, ListSplitsRequestExt,
    MetastoreServiceStreamSplitsExt, SplitState,
};
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::metastore::{
    ListIndexesMetadataRequest, ListSplitsRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::NodeId;
use quickwit_storage::StorageResolver;
use tantivy::Inventory;
use time::OffsetDateTime;
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::actors::{
    MergeExecutor, MergePermit, MergeSplitDownloader, Packager, Publisher, PublisherType, Uploader,
    UploaderType,
};
use crate::mature_merge_plan::plan_merge_operations_for_index;
use crate::merge_policy::{MergeOperation, MergeTask, NopMergePolicy};
use crate::split_store::{IndexingSplitCache, IndexingSplitStore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatureMergeConfig {
    /// Splits within this many days of the retention cutoff are left untouched.
    pub retention_safety_buffer_days: u64,
    /// Minimum number of splits in a group before a merge operation is emitted.
    pub min_merge_group_size: usize,
    /// Maximum number of docs in a split for it to be eligible for mature merging.
    pub input_split_max_num_docs: usize,
    /// Maximum number of splits per merge operation.
    pub max_merge_group_size: usize,
    /// Maximum total number of documents per merge operation.
    pub split_target_num_docs: usize,
    /// Number of indexes processed concurrently.
    pub index_parallelism: usize,
    /// Maximum number of merges running concurrently across all indexes.
    pub max_concurrent_merges: usize,
    /// Print planned operations without executing them.
    pub dry_run: bool,
}

impl Default for MatureMergeConfig {
    fn default() -> Self {
        Self {
            retention_safety_buffer_days: 5,
            min_merge_group_size: 5,
            input_split_max_num_docs: 10_000,
            max_merge_group_size: 100,
            split_target_num_docs: 5_000_000,
            index_parallelism: 50,
            max_concurrent_merges: 10,
            dry_run: false,
        }
    }
}

/// Statistics for the merges performed on a single index.
#[derive(Debug, Default)]
struct IndexMergeOutcome {
    num_published_merges: u64,
    num_replaced_splits: u64,
}

struct IndexMergeSummary {
    num_merges_planned: usize,
    num_input_splits: usize,
    total_input_bytes: u64,
    outcome: IndexMergeOutcome,
}

/// Fetches all published splits for the given index from the metastore (no node-id filter,
/// no immature filter) and calls [`plan_merge_operations_for_index`].
async fn fetch_splits_and_plan(
    index_metadata: &IndexMetadata,
    metastore: &MetastoreServiceClient,
    now: OffsetDateTime,
    config: &MatureMergeConfig,
) -> anyhow::Result<Vec<MergeOperation>> {
    let index_uid = index_metadata.index_uid.clone();
    let list_splits_query =
        ListSplitsQuery::for_index(index_uid).with_split_state(SplitState::Published);
    let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&list_splits_query)?;
    let splits_stream = metastore.list_splits(list_splits_request).await?;
    let splits = splits_stream.collect_splits_metadata().await?;

    info!(
        index_id = %index_metadata.index_config.index_id,
        num_splits = splits.len(),
        "fetched splits for mature merge planning"
    );

    let operations =
        plan_merge_operations_for_index(&index_metadata.index_config, splits, now, config);
    Ok(operations)
}

/// Executes the given merge operations for a single index using the standard actor pipeline:
/// `MergeSplitDownloader -> MergeExecutor -> Packager -> Uploader -> Publisher`.
async fn run_mature_merges_for_index(
    index_metadata: &IndexMetadata,
    operations: Vec<MergeOperation>,
    metastore: MetastoreServiceClient,
    split_store: IndexingSplitStore,
    semaphore: Arc<Semaphore>,
    data_dir_path: &std::path::Path,
    config: &MatureMergeConfig,
    node_id: NodeId,
) -> anyhow::Result<IndexMergeOutcome> {
    if operations.is_empty() {
        return Ok(IndexMergeOutcome {
            num_published_merges: 0,
            num_replaced_splits: 0,
        });
    }

    let index_config = &index_metadata.index_config;
    let index_uid = index_metadata.index_uid.clone();

    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
        .context("failed to build doc mapper")?;
    let tag_fields = doc_mapper.tag_named_fields()?;

    let indexing_directory = temp_dir::Builder::default()
        .join("mature-merge")
        .tempdir_in(data_dir_path)
        .context("failed to create temp directory for mature merge")?;

    let pipeline_id = MergePipelineId {
        node_id,
        index_uid,
        source_id: "_mature_merge".to_string(),
    };

    let universe = Universe::new();
    let kill_switch = KillSwitch::default();

    // Build chain from publisher inward (each actor gets the next actor's mailbox).

    let merge_publisher = Publisher::new(
        PublisherType::MergePublisher,
        metastore.clone(),
        // No feedback loop to a merge planner.
        None,
        None,
    );
    let (merge_publisher_mailbox, merge_publisher_handle) = universe
        .spawn_builder()
        .set_kill_switch(kill_switch.clone())
        .spawn(merge_publisher);

    let merge_uploader = Uploader::new(
        UploaderType::MergeUploader,
        metastore.clone(),
        Arc::new(NopMergePolicy),
        index_config.retention_policy_opt.clone(),
        split_store.clone(),
        merge_publisher_mailbox.into(),
        config.max_concurrent_merges,
        Default::default(),
    );
    let (merge_uploader_mailbox, merge_uploader_handle) = universe
        .spawn_builder()
        .set_kill_switch(kill_switch.clone())
        .spawn(merge_uploader);

    let merge_packager = Packager::new("MaturePackager", tag_fields, merge_uploader_mailbox);
    let (merge_packager_mailbox, merge_packager_handle) = universe
        .spawn_builder()
        .set_kill_switch(kill_switch.clone())
        .spawn(merge_packager);

    let merge_executor = MergeExecutor::new(
        pipeline_id,
        metastore,
        doc_mapper,
        IoControls::default().set_component("mature_merger"),
        merge_packager_mailbox,
    );
    let (merge_executor_mailbox, merge_executor_handle) = universe
        .spawn_builder()
        .set_kill_switch(kill_switch.clone())
        .spawn(merge_executor);

    let merge_split_downloader = MergeSplitDownloader {
        scratch_directory: indexing_directory,
        split_store,
        executor_mailbox: merge_executor_mailbox,
        io_controls: IoControls::default().set_component("mature_split_downloader"),
    };
    let (merge_split_downloader_mailbox, merge_split_downloader_handle) = universe
        .spawn_builder()
        .set_kill_switch(kill_switch.clone())
        .spawn(merge_split_downloader);

    // Send all merge tasks to the downloader, gated by the concurrency semaphore.
    let inventory: Inventory<MergeOperation> = Inventory::default();
    for operation in operations {
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .expect("semaphore should not be closed");
        let merge_task = MergeTask {
            merge_operation: inventory.track(operation),
            _merge_permit: MergePermit::new(permit),
        };
        if merge_split_downloader_mailbox
            .send_message(merge_task)
            .await
            .is_err()
        {
            anyhow::bail!("merge split downloader actor died unexpectedly");
        }
    }

    // Dropping the downloader mailbox signals no more tasks are coming.
    // The pipeline will cascade-exit once all pending tasks are processed.
    drop(merge_split_downloader_mailbox);

    let (downloader_status, _) = merge_split_downloader_handle.join().await;
    let (executor_status, _) = merge_executor_handle.join().await;
    let (packager_status, _) = merge_packager_handle.join().await;
    let (uploader_status, _) = merge_uploader_handle.join().await;
    let (publisher_status, publisher_counters) = merge_publisher_handle.join().await;

    universe.quit().await;

    for (name, status) in [
        ("downloader", downloader_status),
        ("executor", executor_status),
        ("packager", packager_status),
        ("uploader", uploader_status),
        ("publisher", publisher_status),
    ] {
        if !matches!(status, ActorExitStatus::Success | ActorExitStatus::Quit) {
            anyhow::bail!(
                "mature merge actor `{}` exited with unexpected status: {:?}",
                name,
                status
            );
        }
    }

    Ok(IndexMergeOutcome {
        num_published_merges: publisher_counters.num_replace_operations,
        num_replaced_splits: publisher_counters.num_replaced_splits,
    })
}

/// Plans and optionally executes mature merges for a single index
async fn merge_mature_single_index(
    index_metadata: IndexMetadata,
    metastore: &MetastoreServiceClient,
    storage_resolver: &StorageResolver,
    semaphore: Arc<Semaphore>,
    data_dir_path: &std::path::Path,
    config: &MatureMergeConfig,
    node_id: NodeId,
    now: OffsetDateTime,
) -> anyhow::Result<IndexMergeSummary> {
    let index_id = index_metadata.index_config.index_id.clone();
    let operations = fetch_splits_and_plan(&index_metadata, metastore, now, config).await?;
    let num_merges_planned = operations.len();
    let num_input_splits: usize = operations.iter().map(|op| op.splits.len()).sum();
    let total_input_bytes: u64 = operations
        .iter()
        .flat_map(|op| op.splits.iter())
        .map(|s| s.uncompressed_docs_size_in_bytes)
        .sum();

    if config.dry_run {
        for op in &operations {
            info!(
                index_id = %index_id,
                num_input_splits = op.splits.len(),
                num_docs = op.splits.iter().map(|s| s.num_docs).sum::<usize>(),
                input_bytes = op.splits.iter().map(|s| s.footer_offsets.end).sum::<u64>(),
                "[dry-run] merge operation planned"
            );
        }
        return Ok(IndexMergeSummary {
            num_merges_planned,
            num_input_splits,
            total_input_bytes,
            outcome: IndexMergeOutcome::default(),
        });
    }

    if operations.is_empty() {
        return Ok(IndexMergeSummary {
            num_merges_planned: 0,
            total_input_bytes: 0,
            num_input_splits: 0,
            outcome: IndexMergeOutcome::default(),
        });
    }

    let index_uri = index_metadata.index_uri();
    let remote_storage = storage_resolver
        .resolve(index_uri)
        .await
        .context("failed to resolve index storage")?;
    let split_store =
        IndexingSplitStore::new(remote_storage, Arc::new(IndexingSplitCache::no_caching()));

    let outcome = run_mature_merges_for_index(
        &index_metadata,
        operations,
        metastore.clone(),
        split_store,
        semaphore,
        data_dir_path,
        config,
        node_id,
    )
    .await?;

    if num_merges_planned > 0 {
        info!(
            index_id = %index_id,
            planned = num_merges_planned,
            published_merges = outcome.num_published_merges,
            replaced_splits = outcome.num_replaced_splits,
            input_splits = num_input_splits,
            input_bytes = total_input_bytes,
            "mature split merges complete for index"
        );
    }

    Ok(IndexMergeSummary {
        num_merges_planned,
        num_input_splits,
        total_input_bytes,
        outcome,
    })
}

/// Aggregates per-index results, logs per-index and global summary lines, and warns on errors.
fn log_merge_results(results: Vec<anyhow::Result<IndexMergeSummary>>) {
    let mut total_planned = 0usize;
    let mut total_input_splits = 0usize;
    let mut total_input_bytes = 0u64;
    let mut total_published_merges = 0u64;
    let mut total_replaced_splits = 0u64;
    for result in results {
        match result {
            Ok(summary) => {
                total_planned += summary.num_merges_planned;
                total_input_splits += summary.num_input_splits;
                total_input_bytes += summary.total_input_bytes;
                total_published_merges += summary.outcome.num_published_merges;
                total_replaced_splits += summary.outcome.num_replaced_splits;
            }
            Err(err) => {
                warn!(err = ?err, "error processing index during mature merge");
            }
        }
    }
    info!(
        total_planned,
        total_published_merges,
        total_replaced_splits,
        total_input_splits,
        total_input_bytes,
        "mature merge complete"
    );
}

/// Processes all indexes from the metastore, discovering and running mature
/// merge opportunities.
///
/// Up to [`INDEX_PARALLELISM`] indexes are processed concurrently. This avoids
/// fetching split metadata too eagerly. The total merge concurrency is
/// protected by max_concurrent_merges.
///
/// If `dry_run` is `true`, the planned operations are printed but not executed.
pub async fn merge_mature_all_indexes(
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    data_dir_path: &std::path::Path,
    config: MatureMergeConfig,
    node_id: NodeId,
) -> anyhow::Result<()> {
    let now = OffsetDateTime::now_utc();

    let indexes_metadata = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest::all())
        .await
        .context("failed to list indexes")?
        .deserialize_indexes_metadata()
        .await
        .context("failed to deserialize indexes metadata")?;

    info!(
        num_indexes = indexes_metadata.len(),
        "starting mature merge"
    );

    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_merges));
    let metastore_ref = &metastore;
    let storage_resolver_ref = &storage_resolver;
    let config_ref = &config;

    let results: Vec<anyhow::Result<IndexMergeSummary>> = futures::stream::iter(indexes_metadata)
        .map(|index_metadata| {
            let node_id = node_id.clone();
            let semaphore = Arc::clone(&semaphore);
            async move {
                merge_mature_single_index(
                    index_metadata,
                    metastore_ref,
                    storage_resolver_ref,
                    semaphore,
                    data_dir_path,
                    config_ref,
                    node_id,
                    now,
                )
                .await
            }
        })
        .buffer_unordered(config.index_parallelism)
        .collect()
        .await;

    log_merge_results(results);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_metastore::{IndexMetadata, IndexMetadataResponseExt, SplitMetadata};
    use quickwit_proto::metastore::{
        IndexMetadataRequest, ListSplitsRequest, MetastoreService, MetastoreServiceClient,
        MockMetastoreService,
    };
    use quickwit_proto::types::NodeId;
    use quickwit_storage::RamStorage;

    use super::*;
    use crate::TestSandbox;

    /// Tests the short-circuit path: when no merge operations are planned,
    /// `run_mature_merges_for_index` returns 0 immediately without spawning any actors.
    #[tokio::test]
    async fn test_run_mature_merges_for_index_no_operations() -> anyhow::Result<()> {
        let mock_metastore = MockMetastoreService::new();
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage);
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let data_dir = TempDirectory::for_test();
        let node_id = NodeId::from("test-node");

        let semaphore = Arc::new(Semaphore::new(2));
        let outcome = run_mature_merges_for_index(
            &index_metadata,
            vec![],
            MetastoreServiceClient::from_mock(mock_metastore),
            split_store,
            semaphore,
            data_dir.path(),
            &MatureMergeConfig::default(),
            node_id,
        )
        .await?;

        assert_eq!(outcome.num_published_merges, 0);
        assert_eq!(outcome.num_replaced_splits, 0);
        Ok(())
    }

    /// Tests the full per index pipeline end-to-end with a single merge operation
    #[tokio::test]
    async fn test_run_mature_merges_for_index_merges_real_splits() -> anyhow::Result<()> {
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: datetime
                input_formats: [unix_timestamp]
                fast: true
            timestamp_field: ts
        "#;
        let test_sandbox =
            TestSandbox::create("test-index-mature2", doc_mapping_yaml, "", &["body"]).await?;

        // Each add_documents() call runs the full indexing pipeline and produces
        // exactly one Published split on the underlying RamStorage.
        for i in 0..4u64 {
            test_sandbox
                .add_documents(std::iter::once(
                    serde_json::json!({"body": format!("doc{i}"), "ts": 1_631_072_713u64 + i}),
                ))
                .await?;
        }

        let metastore = test_sandbox.metastore();
        let index_uid = test_sandbox.index_uid();

        let split_metas: Vec<SplitMetadata> = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await?
            .collect_splits_metadata()
            .await?;
        assert_eq!(split_metas.len(), 4);

        let index_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id(
                index_uid.index_id.to_string(),
            ))
            .await?
            .deserialize_index_metadata()?;

        let merge_op = MergeOperation::new_merge_operation(split_metas);
        let split_store =
            IndexingSplitStore::create_without_local_store_for_test(test_sandbox.storage());
        let data_dir = TempDirectory::for_test();
        let semaphore = Arc::new(Semaphore::new(2));

        let outcome = run_mature_merges_for_index(
            &index_metadata,
            vec![merge_op],
            metastore.clone(),
            split_store,
            semaphore,
            data_dir.path(),
            &MatureMergeConfig::default(),
            test_sandbox.node_id(),
        )
        .await?;

        assert_eq!(outcome.num_published_merges, 1);
        assert_eq!(outcome.num_replaced_splits, 4);

        // The 4 input splits are now MarkedForDeletion; 1 merged Published split should remain.
        let published_after: Vec<SplitMetadata> = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(
                &ListSplitsQuery::for_index(index_uid).with_split_state(SplitState::Published),
            )?)
            .await?
            .collect_splits_metadata()
            .await?;
        assert_eq!(published_after.len(), 1);
        assert_eq!(published_after[0].num_docs, 4);

        test_sandbox.assert_quit().await;
        Ok(())
    }
}
