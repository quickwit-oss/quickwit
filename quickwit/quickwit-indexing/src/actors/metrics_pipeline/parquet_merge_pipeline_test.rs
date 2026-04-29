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

//! Integration test for the Parquet merge pipeline.
//!
//! Tests the full actor chain: seeds splits → planner plans merge →
//! downloader downloads from storage → executor merges → uploader uploads →
//! publisher publishes with replaced_split_ids.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use quickwit_actors::Universe;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_parquet_engine::merge::policy::{
    ConstWriteAmplificationParquetMergePolicy, ParquetMergePolicyConfig,
};
use quickwit_parquet_engine::split::{ParquetSplitId, ParquetSplitMetadata, TimeRange};
use quickwit_parquet_engine::storage::{ParquetWriter, ParquetWriterConfig};
use quickwit_parquet_engine::table_config::TableConfig;
use quickwit_parquet_engine::test_helpers::create_test_batch_with_tags;
use quickwit_proto::metastore::{EmptyResponse, MetastoreServiceClient, MockMetastoreService};
use quickwit_storage::{RamStorage, Storage};

use super::parquet_merge_pipeline::{ParquetMergePipeline, ParquetMergePipelineParams};

/// Write a sorted Parquet file to the given directory using the standard
/// writer (which computes sorted_series, row_keys, zonemaps, and KV metadata).
fn write_test_parquet_file(
    dir: &Path,
    filename: &str,
    num_rows: usize,
    split_metadata: &ParquetSplitMetadata,
) -> u64 {
    let table_config = TableConfig::default();
    let writer = ParquetWriter::new(ParquetWriterConfig::default(), &table_config)
        .expect("failed to create ParquetWriter");

    let batch = create_test_batch_with_tags(num_rows, &["service", "host"]);
    let path = dir.join(filename);
    let (file_size, _write_metadata) = writer
        .write_to_file_with_metadata(&batch, &path, Some(split_metadata))
        .expect("failed to write test Parquet file");
    file_size
}

/// Create a ParquetSplitMetadata consistent with the test Parquet writer.
fn make_test_split_metadata(
    split_id: &str,
    num_rows: u64,
    size_bytes: u64,
) -> ParquetSplitMetadata {
    let table_config = TableConfig::default();
    ParquetSplitMetadata::metrics_builder()
        .split_id(ParquetSplitId::new(split_id))
        .index_uid("test-merge-index:00000000000000000000000001")
        .partition_id(0)
        .time_range(TimeRange::new(100, 100 + num_rows))
        .num_rows(num_rows)
        .size_bytes(size_bytes)
        .sort_fields(table_config.effective_sort_fields())
        .window_start_secs(0)
        .window_duration_secs(900)
        .add_metric_name("cpu.usage")
        .build()
}

/// Full integration test: seed splits → merge → verify replace publish.
///
/// Creates 2 real sorted Parquet files in RamStorage, seeds the merge
/// pipeline with their metadata, and verifies the pipeline:
/// 1. Plans a merge (merge_factor=2)
/// 2. Downloads files from storage
/// 3. Executes the merge via the k-way merge engine
/// 4. Uploads the merged output
/// 5. Publishes with replaced_split_ids matching the input splits
#[tokio::test]
async fn test_merge_pipeline_end_to_end() {
    quickwit_common::setup_logging_for_tests();

    let universe = Universe::with_accelerated_time();
    let temp_dir = tempfile::tempdir().unwrap();
    let ram_storage: Arc<dyn Storage> = Arc::new(RamStorage::default());

    // --- Step 1: Create real sorted Parquet files and upload to storage ---

    let meta_a = make_test_split_metadata("split-a", 50, 0);
    let size_a = write_test_parquet_file(temp_dir.path(), "split-a.parquet", 50, &meta_a);
    let meta_a = {
        let mut m = meta_a;
        m.size_bytes = size_a;
        m.parquet_file = "split-a.parquet".to_string();
        m
    };

    let meta_b = make_test_split_metadata("split-b", 50, 0);
    let size_b = write_test_parquet_file(temp_dir.path(), "split-b.parquet", 50, &meta_b);
    let meta_b = {
        let mut m = meta_b;
        m.size_bytes = size_b;
        m.parquet_file = "split-b.parquet".to_string();
        m
    };

    // Upload files to RamStorage.
    let content_a = std::fs::read(temp_dir.path().join("split-a.parquet")).unwrap();
    ram_storage
        .put(Path::new("split-a.parquet"), Box::new(content_a))
        .await
        .unwrap();
    let content_b = std::fs::read(temp_dir.path().join("split-b.parquet")).unwrap();
    ram_storage
        .put(Path::new("split-b.parquet"), Box::new(content_b))
        .await
        .unwrap();

    // --- Step 2: Set up mock metastore ---

    let mut mock_metastore = MockMetastoreService::new();

    // Expect staging of the merged output split.
    mock_metastore
        .expect_stage_metrics_splits()
        .returning(|_| Ok(EmptyResponse {}));

    // Capture the publish request to verify replaced_split_ids.
    let publish_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let publish_called_clone = publish_called.clone();
    let replaced_ids = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let replaced_ids_clone = replaced_ids.clone();

    mock_metastore
        .expect_publish_metrics_splits()
        .returning(move |request| {
            replaced_ids_clone
                .lock()
                .unwrap()
                .extend(request.replaced_split_ids.clone());
            publish_called_clone.store(true, Ordering::SeqCst);
            Ok(EmptyResponse {})
        });

    let metastore = MetastoreServiceClient::from_mock(mock_metastore);

    // --- Step 3: Spawn the merge pipeline ---

    let merge_policy = Arc::new(ConstWriteAmplificationParquetMergePolicy::new(
        ParquetMergePolicyConfig {
            merge_factor: 2,
            max_merge_factor: 2,
            max_merge_ops: 5,
            target_split_size_bytes: 256 * 1024 * 1024,
            maturation_period: Duration::from_secs(3600),
            max_finalize_merge_operations: 3,
        },
    ));

    let params = ParquetMergePipelineParams {
        index_uid: quickwit_proto::types::IndexUid::for_test("test-merge-index", 0),
        indexing_directory: TempDirectory::for_test(),
        metastore,
        storage: ram_storage.clone(),
        merge_policy,
        merge_scheduler_service: universe.get_or_spawn_one(),
        max_concurrent_split_uploads: 4,
        event_broker: EventBroker::default(),
        writer_config: ParquetWriterConfig::default(),
    };

    let initial_splits = vec![meta_a, meta_b];
    let pipeline = ParquetMergePipeline::new(params, Some(initial_splits), universe.spawn_ctx());
    let (_pipeline_mailbox, _pipeline_handle) = universe.spawn_builder().spawn(pipeline);

    // --- Step 4: Wait for publish with replaced_split_ids ---

    wait_until_predicate(
        || {
            let publish_called = publish_called.clone();
            async move { publish_called.load(Ordering::SeqCst) }
        },
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await
    .expect("timed out waiting for merge publish");

    // --- Step 5: Verify ---

    let mut replaced_sorted: Vec<String> = replaced_ids.lock().unwrap().clone();
    replaced_sorted.sort();
    assert_eq!(
        replaced_sorted,
        vec!["split-a".to_string(), "split-b".to_string()],
        "publish should replace both input splits"
    );

    universe.assert_quit().await;
}
