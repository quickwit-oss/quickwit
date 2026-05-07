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

//! Integration test for sketch splits through the Parquet merge pipeline.
//!
//! Verifies that the pipeline dispatches to the correct sketch-specific
//! metastore RPCs (stage_sketch_splits, publish_sketch_splits) and produces
//! correct merge output for sketch split data.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use quickwit_actors::Universe;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_metastore::StageParquetSplitsRequestExt;
use quickwit_parquet_engine::merge::policy::{
    ConstWriteAmplificationParquetMergePolicy, ParquetMergePolicyConfig,
};
use quickwit_parquet_engine::split::{ParquetSplitKind, ParquetSplitMetadata};
use quickwit_parquet_engine::storage::ParquetWriterConfig;
use quickwit_proto::metastore::{
    EmptyResponse, MetastoreServiceClient, MockMetastoreService, StageSketchSplitsRequest,
};
use quickwit_storage::{RamStorage, Storage};

use super::parquet_merge_pipeline::{ParquetMergePipeline, ParquetMergePipelineParams};
use super::parquet_merge_pipeline_test::{
    create_custom_test_batch, make_test_sketch_split_metadata, write_test_parquet_file,
};

/// Sketch variant of the end-to-end merge pipeline test.
///
/// Verifies that sketch splits use the correct metastore RPCs
/// (stage_sketch_splits, publish_sketch_splits) and that the split kind
/// is correctly propagated through the merge pipeline.
#[tokio::test]
async fn test_merge_pipeline_end_to_end_sketches() {
    quickwit_common::setup_logging_for_tests();

    let universe = Universe::with_accelerated_time();
    let temp_dir = tempfile::tempdir().unwrap();
    let ram_storage: Arc<dyn Storage> = Arc::new(RamStorage::default());

    // --- Step 1: Create sketch Parquet files ---

    // Write Parquet files using the metrics schema (the merge engine is
    // schema-agnostic — it just merges sorted Parquet files). The sketch
    // kind only affects metastore RPC routing, not the data format.
    // We write as "metrics" to avoid sketch schema validation, then set
    // the split metadata kind to Sketches for the pipeline.
    let batch_a = create_custom_test_batch("cpu.usage", 100, 50, "web", "host-1");
    let metrics_meta_a = super::parquet_merge_pipeline_test::make_test_split_metadata(
        "sketch-a",
        50,
        0,
        100,
        "cpu.usage",
    );
    let size_a = write_test_parquet_file(
        temp_dir.path(),
        "sketch-a.parquet",
        &batch_a,
        &metrics_meta_a,
    );
    let mut meta_a = make_test_sketch_split_metadata("sketch-a", 50, size_a, 100, "cpu.usage");
    meta_a.parquet_file = "sketch-a.parquet".to_string();

    let batch_b = create_custom_test_batch("mem.usage", 200, 50, "api", "host-2");
    let metrics_meta_b = super::parquet_merge_pipeline_test::make_test_split_metadata(
        "sketch-b",
        50,
        0,
        200,
        "mem.usage",
    );
    let size_b = write_test_parquet_file(
        temp_dir.path(),
        "sketch-b.parquet",
        &batch_b,
        &metrics_meta_b,
    );
    let mut meta_b = make_test_sketch_split_metadata("sketch-b", 50, size_b, 200, "mem.usage");
    meta_b.parquet_file = "sketch-b.parquet".to_string();

    // Upload files to RamStorage.
    let content_a = std::fs::read(temp_dir.path().join("sketch-a.parquet")).unwrap();
    ram_storage
        .put(Path::new("sketch-a.parquet"), Box::new(content_a))
        .await
        .unwrap();
    let content_b = std::fs::read(temp_dir.path().join("sketch-b.parquet")).unwrap();
    ram_storage
        .put(Path::new("sketch-b.parquet"), Box::new(content_b))
        .await
        .unwrap();

    // --- Step 2: Mock metastore with SKETCH-specific RPCs ---

    let mut mock_metastore = MockMetastoreService::new();

    // Capture staged sketch metadata.
    let staged_metadata: Arc<std::sync::Mutex<Vec<ParquetSplitMetadata>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let staged_metadata_clone = staged_metadata.clone();

    // Sketch RPCs — NOT metrics RPCs.
    mock_metastore.expect_stage_sketch_splits().returning(
        move |request: StageSketchSplitsRequest| {
            let splits = request
                .deserialize_splits_metadata()
                .expect("failed to deserialize staged sketch metadata");
            staged_metadata_clone.lock().unwrap().extend(splits);
            Ok(EmptyResponse {})
        },
    );

    let publish_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let publish_called_clone = publish_called.clone();
    let replaced_ids = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let replaced_ids_clone = replaced_ids.clone();

    mock_metastore
        .expect_publish_sketch_splits()
        .returning(move |request| {
            replaced_ids_clone
                .lock()
                .unwrap()
                .extend(request.replaced_split_ids.clone());
            publish_called_clone.store(true, Ordering::SeqCst);
            Ok(EmptyResponse {})
        });

    let metastore = MetastoreServiceClient::from_mock(mock_metastore);

    // --- Step 3: Spawn the merge pipeline with a sketches index ---

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

    // Use a sketches index name so is_sketches_index() returns true.
    let params = ParquetMergePipelineParams {
        index_uid: quickwit_proto::types::IndexUid::for_test("datadog-sketches-test", 0),
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

    // --- Step 4: Wait for publish ---

    wait_until_predicate(
        || {
            let publish_called = publish_called.clone();
            async move { publish_called.load(Ordering::SeqCst) }
        },
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await
    .expect("timed out waiting for sketch merge publish");

    // --- Step 5: Verify replaced_split_ids ---

    let mut replaced_sorted: Vec<String> = replaced_ids.lock().unwrap().clone();
    replaced_sorted.sort();
    assert_eq!(
        replaced_sorted,
        vec!["sketch-a".to_string(), "sketch-b".to_string()],
        "publish should replace both input sketch splits"
    );

    // --- Step 6: Verify staged metadata is for sketches ---

    let staged = staged_metadata.lock().unwrap().clone();
    assert_eq!(
        staged.len(),
        1,
        "exactly one merged sketch split should be staged"
    );
    let merged_meta = &staged[0];

    assert_eq!(
        merged_meta.kind,
        ParquetSplitKind::Sketches,
        "merged split must be a sketch split"
    );
    assert_eq!(
        merged_meta.num_rows, 100,
        "merged sketch split must have 100 rows"
    );

    let expected_metrics: HashSet<String> = ["cpu.usage", "mem.usage"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        merged_meta.metric_names, expected_metrics,
        "merged sketch split must contain both metric names"
    );

    assert_eq!(
        merged_meta.num_merge_ops, 1,
        "first merge should set num_merge_ops to 1"
    );

    universe.assert_quit().await;
}
