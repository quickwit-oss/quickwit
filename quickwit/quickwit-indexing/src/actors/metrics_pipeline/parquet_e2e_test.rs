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

//! End-to-end tests for the parquet pipeline
//!
//! These tests wire up the full metrics pipeline:
//! ParquetDocProcessor → ParquetIndexer → ParquetUploader → Publisher

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow::array::{
    ArrayRef, DictionaryArray, Float64Array, Int16Array, Int32Array, Int64Array, ListArray,
    StringArray, UInt8Array, UInt32Array, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use quickwit_actors::{ActorHandle, QueueCapacity, Universe};
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_metastore::checkpoint::SourceCheckpointDelta;
use quickwit_parquet_engine::ingest::record_batch_to_ipc;
use quickwit_parquet_engine::split::ParquetSplitKind;
use quickwit_parquet_engine::storage::{ParquetSplitWriter, ParquetWriterConfig};
use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
use quickwit_proto::types::IndexUid;
use quickwit_storage::RamStorage;

use crate::actors::sequencer::Sequencer;
use crate::actors::{
    ParquetDocProcessor, ParquetIndexer, ParquetPackager, ParquetUploader, Publisher, UploaderType,
};
use crate::models::RawDocBatch;

// =============================================================================
// Helpers
// =============================================================================

async fn wait_for_published_splits(
    publisher_handle: &ActorHandle<Publisher>,
    expected_splits: u64,
) -> anyhow::Result<()> {
    wait_until_predicate(
        || async {
            publisher_handle.process_pending_and_observe().await;
            let counters = publisher_handle.last_observation();
            counters.num_published_splits >= expected_splits
        },
        Duration::from_secs(15),
        Duration::from_millis(50),
    )
    .await
    .map_err(|_| anyhow::anyhow!("Timeout waiting for {} published splits", expected_splits))
}

fn create_test_batch(
    num_rows: usize,
    metric_name: &str,
    service: &str,
    base_timestamp: u64,
    base_value: f64,
) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(
            "metric_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("timeseries_id", DataType::Int64, false),
        Field::new(
            "service",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    let metric_name_arr: ArrayRef = {
        let keys = Int32Array::from(vec![0i32; num_rows]);
        let vals = StringArray::from(vec![metric_name]);
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(vals)).unwrap())
    };
    let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
    let timestamps: Vec<u64> = (0..num_rows).map(|i| base_timestamp + i as u64).collect();
    let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
    let values: Vec<f64> = (0..num_rows).map(|i| base_value + i as f64).collect();
    let value: ArrayRef = Arc::new(Float64Array::from(values));
    let timeseries_ids: Vec<i64> = (0..num_rows).map(|i| 1000 + i as i64).collect();
    let timeseries_id: ArrayRef = Arc::new(Int64Array::from(timeseries_ids));
    let service_arr: ArrayRef = {
        let keys = Int32Array::from(vec![0i32; num_rows]);
        let vals = StringArray::from(vec![service]);
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(vals)).unwrap())
    };

    RecordBatch::try_new(
        schema,
        vec![
            metric_name_arr,
            metric_type,
            timestamp_secs,
            value,
            timeseries_id,
            service_arr,
        ],
    )
    .unwrap()
}

fn create_raw_doc_batch(
    ipc_bytes: &[u8],
    range: std::ops::Range<u64>,
    force_commit: bool,
) -> RawDocBatch {
    let docs = vec![Bytes::from(ipc_bytes.to_vec())];
    let checkpoint_delta = SourceCheckpointDelta::from_range(range);
    RawDocBatch::new(docs, checkpoint_delta, force_commit)
}

// =============================================================================
// Metrics pipeline E2E
// =============================================================================

/// Full pipeline test: DocProcessor → Indexer → Uploader → Publisher.
///
/// Validates:
/// - Multiple batches accumulate without producing splits
/// - force_commit triggers split production
/// - Counters are correct at each stage
/// - Splits are staged and published via metastore
#[tokio::test]
async fn test_metrics_pipeline_e2e() {
    let universe = Universe::with_accelerated_time();
    let temp_dir = tempfile::tempdir().unwrap();

    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore
        .expect_stage_metrics_splits()
        .returning(|_| Ok(EmptyResponse {}));
    mock_metastore
        .expect_publish_metrics_splits()
        .returning(|_| Ok(EmptyResponse {}));

    let metastore_client =
        quickwit_proto::metastore::MetastoreServiceClient::from_mock(mock_metastore);
    let ram_storage = Arc::new(RamStorage::default());

    let publisher = Publisher::new(
        super::METRICS_PUBLISHER_NAME,
        quickwit_actors::QueueCapacity::Bounded(1),
        metastore_client.clone(),
        None,
        None,
    );
    let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

    let sequencer_mailbox = super::spawn_sequencer_for_test(&universe, publisher_mailbox);

    let uploader = ParquetUploader::new(
        UploaderType::IndexUploader,
        metastore_client.clone(),
        ram_storage,
        sequencer_mailbox,
        4,
    );
    let (uploader_mailbox, _uploader_handle) = universe.spawn_builder().spawn(uploader);

    let writer_config = ParquetWriterConfig::default();
    let table_config = quickwit_parquet_engine::table_config::TableConfig::default();
    let split_writer = ParquetSplitWriter::new(
        ParquetSplitKind::Metrics,
        writer_config,
        temp_dir.path(),
        &table_config,
    )
    .unwrap();
    let packager = ParquetPackager::new(split_writer, uploader_mailbox);
    let (packager_mailbox, packager_handle) = universe.spawn_builder().spawn(packager);

    let indexer = ParquetIndexer::new(
        IndexUid::for_test("test-metrics-index", 0),
        "test-source".to_string(),
        None,
        packager_mailbox,
        None,
    );
    let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

    let doc_processor = ParquetDocProcessor::new(
        super::parquet_doc_processor::IngestProcessor::Metrics(
            quickwit_parquet_engine::ingest::ParquetIngestProcessor,
        ),
        "test-metrics-index".to_string(),
        "test-source".to_string(),
        indexer_mailbox,
    );
    let (doc_processor_mailbox, doc_processor_handle) =
        universe.spawn_builder().spawn(doc_processor);

    // Phase 1: Send 5 batches without force_commit — should accumulate, no splits.
    let rows_per_batch: usize = 20;
    for i in 0u64..5 {
        let batch = create_test_batch(rows_per_batch, "cpu.usage", "web", 100 + i * 20, 0.0);
        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let raw = create_raw_doc_batch(
            &ipc_bytes,
            i * rows_per_batch as u64..(i + 1) * rows_per_batch as u64,
            false,
        );
        doc_processor_mailbox.send_message(raw).await.unwrap();
    }

    doc_processor_handle.process_pending_and_observe().await;
    let indexer_counters = indexer_handle.process_pending_and_observe().await.state;

    assert_eq!(indexer_counters.batches_received, 5);
    assert_eq!(indexer_counters.rows_indexed, 100);
    assert_eq!(
        indexer_counters.batches_flushed, 0,
        "No flushes without force_commit"
    );

    // Phase 2: Send one more batch with force_commit — triggers split.
    let batch = create_test_batch(10, "cpu.usage", "web", 200, 0.0);
    let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
    let raw = create_raw_doc_batch(&ipc_bytes, 100..110, true);
    doc_processor_mailbox.send_message(raw).await.unwrap();

    let doc_processor_counters = doc_processor_handle
        .process_pending_and_observe()
        .await
        .state;
    let indexer_counters = indexer_handle.process_pending_and_observe().await.state;

    wait_for_published_splits(&publisher_handle, 1)
        .await
        .expect("Publisher should have published 1 split");

    assert_eq!(doc_processor_counters.valid_batches, 6);
    assert_eq!(doc_processor_counters.valid_rows, 110);
    assert_eq!(doc_processor_counters.parse_errors, 0);
    assert_eq!(
        indexer_counters.batches_flushed, 1,
        "force_commit should flush 1 batch"
    );
    // Verify packager produced the split
    let packager_counters = packager_handle.process_pending_and_observe().await.state;
    assert_eq!(
        packager_counters.splits_produced.load(Ordering::Relaxed),
        1,
        "packager should produce 1 split"
    );
    assert!(packager_counters.bytes_written.load(Ordering::Relaxed) > 0);
    assert_eq!(publisher_handle.last_observation().num_published_splits, 1);

    universe.assert_quit().await;
}

// =============================================================================
// FileBackedMetastore metrics operations
// =============================================================================

#[tokio::test]
async fn test_file_backed_metastore_metrics_operations() {
    use quickwit_config::IndexConfig;
    use quickwit_metastore::{
        CreateIndexRequestExt, FileBackedMetastore, ListParquetSplitsQuery,
        ListParquetSplitsRequestExt, ListParquetSplitsResponseExt, ParquetSplitRecord, SplitState,
        StageParquetSplitsRequestExt,
    };
    use quickwit_parquet_engine::split::{ParquetSplitMetadata, TimeRange};
    use quickwit_proto::metastore::{
        CreateIndexRequest, ListMetricsSplitsRequest, MetastoreService,
        PublishMetricsSplitsRequest, StageMetricsSplitsRequest,
    };

    let ram_storage = Arc::new(RamStorage::default());
    let metastore = FileBackedMetastore::try_new(ram_storage.clone(), None)
        .await
        .unwrap();

    let index_id = "metrics-test-index";
    let index_config = IndexConfig::for_test(index_id, "ram:///indexes/metrics-test-index");
    let create_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let create_response = metastore.create_index(create_request).await.unwrap();
    let index_uid = create_response.index_uid.unwrap();

    let split1 = ParquetSplitMetadata::metrics_builder()
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(1000, 1100))
        .num_rows(100)
        .size_bytes(1024)
        .add_metric_name("cpu.usage")
        .build();

    let split2 = ParquetSplitMetadata::metrics_builder()
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(2000, 2100))
        .num_rows(200)
        .size_bytes(2048)
        .add_metric_name("memory.used")
        .build();

    // Stage
    let stage_request = StageMetricsSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        &[split1.clone(), split2.clone()],
    )
    .unwrap();
    metastore.stage_metrics_splits(stage_request).await.unwrap();

    // Verify staged
    let query = ListParquetSplitsQuery::for_index(index_uid.clone())
        .with_split_states([SplitState::Staged]);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let staged: Vec<ParquetSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(staged.len(), 2);

    // Publish split1
    let publish_request = PublishMetricsSplitsRequest {
        index_uid: Some(index_uid.clone()),
        staged_split_ids: vec![split1.split_id.to_string()],
        replaced_split_ids: vec![],
        index_checkpoint_delta_json_opt: None,
        publish_token_opt: None,
    };
    metastore
        .publish_metrics_splits(publish_request)
        .await
        .unwrap();

    // Verify published
    let query = ListParquetSplitsQuery::for_index(index_uid.clone())
        .with_split_states([SplitState::Published]);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let published: Vec<ParquetSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].metadata.split_id, split1.split_id);

    // Time range filtering
    let query = ListParquetSplitsQuery::for_index(index_uid.clone())
        .with_split_states([SplitState::Published])
        .with_time_range_start_gte(1000)
        .with_time_range_end_lte(1100);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let in_range: Vec<ParquetSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(in_range.len(), 1);

    let query = ListParquetSplitsQuery::for_index(index_uid.clone())
        .with_split_states([SplitState::Published])
        .with_time_range_start_gte(5000)
        .with_time_range_end_lte(5100);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let out_of_range: Vec<ParquetSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(out_of_range.len(), 0);

    // Metric name filtering
    let query = ListParquetSplitsQuery::for_index(index_uid.clone())
        .with_split_states([SplitState::Published])
        .with_metric_names(vec!["cpu.usage".to_string()]);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let by_metric: Vec<ParquetSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(by_metric.len(), 1);
}

// =============================================================================
// Sketch pipeline E2E
// =============================================================================

fn create_sketch_test_batch(
    num_rows: usize,
    metric_name: &str,
    service: &str,
    base_timestamp: u64,
) -> RecordBatch {
    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("metric_name", dict_type.clone(), false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("count", DataType::UInt64, false),
        Field::new("sum", DataType::Float64, false),
        Field::new("min", DataType::Float64, false),
        Field::new("max", DataType::Float64, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new(
            "keys",
            DataType::List(Arc::new(Field::new("item", DataType::Int16, false))),
            false,
        ),
        Field::new(
            "counts",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
            false,
        ),
        Field::new("timeseries_id", DataType::Int64, false),
        Field::new("service", dict_type, true),
    ]));

    let metric_name_arr: ArrayRef = {
        let keys = Int32Array::from(vec![0i32; num_rows]);
        let vals = StringArray::from(vec![metric_name]);
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(vals)).unwrap())
    };
    let timestamps: Vec<u64> = (0..num_rows).map(|i| base_timestamp + i as u64).collect();
    let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
    let count: ArrayRef = Arc::new(UInt64Array::from(vec![100u64; num_rows]));
    let sum: ArrayRef = Arc::new(Float64Array::from(vec![500.0; num_rows]));
    let min_arr: ArrayRef = Arc::new(Float64Array::from(vec![1.0; num_rows]));
    let max_arr: ArrayRef = Arc::new(Float64Array::from(vec![200.0; num_rows]));
    let flags: ArrayRef = Arc::new(UInt32Array::from(vec![0u32; num_rows]));

    // Build List<Int16> keys column
    let bucket_keys: Vec<i16> = vec![100, 200, 300];
    let mut key_offsets = vec![0i32];
    let mut key_values = Vec::new();
    for _ in 0..num_rows {
        key_values.extend_from_slice(&bucket_keys);
        key_offsets.push(key_values.len() as i32);
    }
    let keys_arr: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int16, false)),
        OffsetBuffer::new(key_offsets.into()),
        Arc::new(Int16Array::from(key_values)),
        None,
    ));

    // Build List<UInt64> counts column
    let bucket_counts: Vec<u64> = vec![50, 30, 20];
    let mut count_offsets = vec![0i32];
    let mut count_values = Vec::new();
    for _ in 0..num_rows {
        count_values.extend_from_slice(&bucket_counts);
        count_offsets.push(count_values.len() as i32);
    }
    let counts_arr: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::UInt64, false)),
        OffsetBuffer::new(count_offsets.into()),
        Arc::new(UInt64Array::from(count_values)),
        None,
    ));

    let timeseries_ids: Vec<i64> = (0..num_rows).map(|i| 2000 + i as i64).collect();
    let timeseries_id: ArrayRef = Arc::new(Int64Array::from(timeseries_ids));

    let service_arr: ArrayRef = {
        let keys = Int32Array::from(vec![0i32; num_rows]);
        let vals = StringArray::from(vec![service]);
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(vals)).unwrap())
    };

    RecordBatch::try_new(
        schema,
        vec![
            metric_name_arr,
            timestamp_secs,
            count,
            sum,
            min_arr,
            max_arr,
            flags,
            keys_arr,
            counts_arr,
            timeseries_id,
            service_arr,
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_sketch_pipeline_e2e() {
    use super::parquet_doc_processor::IngestProcessor;

    let universe = Universe::with_accelerated_time();
    let temp_dir = tempfile::tempdir().unwrap();

    let mut mock_metastore = MockMetastoreService::new();
    mock_metastore
        .expect_stage_sketch_splits()
        .returning(|_| Ok(EmptyResponse {}));
    mock_metastore
        .expect_publish_sketch_splits()
        .returning(|_| Ok(EmptyResponse {}));

    let metastore_client =
        quickwit_proto::metastore::MetastoreServiceClient::from_mock(mock_metastore);
    let ram_storage = Arc::new(RamStorage::default());

    let publisher = Publisher::new(
        super::METRICS_PUBLISHER_NAME,
        QueueCapacity::Bounded(1),
        metastore_client.clone(),
        None,
        None,
    );
    let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

    let sequencer = Sequencer::new(publisher_mailbox);
    let (sequencer_mailbox, _sequencer_handle) = universe.spawn_builder().spawn(sequencer);

    let uploader = ParquetUploader::new(
        UploaderType::IndexUploader,
        metastore_client.clone(),
        ram_storage.clone(),
        sequencer_mailbox,
        4,
    );
    let (uploader_mailbox, _uploader_handle) = universe.spawn_builder().spawn(uploader);

    let writer_config = ParquetWriterConfig::default();
    let split_writer = ParquetSplitWriter::new(
        ParquetSplitKind::Sketches,
        writer_config,
        temp_dir.path(),
        &quickwit_parquet_engine::table_config::TableConfig::default(),
    )
    .unwrap();
    let packager = ParquetPackager::new(split_writer, uploader_mailbox);
    let (packager_mailbox, packager_handle) = universe.spawn_builder().spawn(packager);

    let indexer = ParquetIndexer::new(
        IndexUid::for_test("sketches-test-index", 0),
        "test-source".to_string(),
        None,
        packager_mailbox,
        None,
    );
    let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

    let sketch_processor = IngestProcessor::Sketches(
        quickwit_parquet_engine::ingest::SketchParquetIngestProcessor::new(),
    );
    let doc_processor = ParquetDocProcessor::new(
        sketch_processor,
        "sketches-test-index".to_string(),
        "test-source".to_string(),
        indexer_mailbox,
    );
    let (doc_processor_mailbox, doc_processor_handle) =
        universe.spawn_builder().spawn(doc_processor);

    // Send sketch batches without force_commit
    for i in 0u64..3 {
        let batch = create_sketch_test_batch(10, "req.latency", "api", 1000 + i * 10);
        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let raw = create_raw_doc_batch(&ipc_bytes, i * 10..(i + 1) * 10, false);
        doc_processor_mailbox.send_message(raw).await.unwrap();
    }

    doc_processor_handle.process_pending_and_observe().await;
    let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
    assert_eq!(indexer_counters.batches_received, 3);
    assert_eq!(indexer_counters.rows_indexed, 30);
    assert_eq!(
        indexer_counters.batches_flushed, 0,
        "no flushes without force_commit"
    );

    // Send one more batch with force_commit
    let batch = create_sketch_test_batch(5, "req.latency", "api", 2000);
    let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
    let raw = create_raw_doc_batch(&ipc_bytes, 30..35, true);
    doc_processor_mailbox.send_message(raw).await.unwrap();

    doc_processor_handle.process_pending_and_observe().await;
    indexer_handle.process_pending_and_observe().await;

    wait_for_published_splits(&publisher_handle, 1)
        .await
        .expect("publisher should have published 1 sketch split");

    let doc_counters = doc_processor_handle
        .process_pending_and_observe()
        .await
        .state;
    assert_eq!(doc_counters.valid_batches, 4);
    assert_eq!(doc_counters.valid_rows, 35);
    assert_eq!(doc_counters.parse_errors, 0);

    let packager_counters = packager_handle.process_pending_and_observe().await.state;
    assert_eq!(packager_counters.splits_produced.load(Ordering::Relaxed), 1);
    assert!(packager_counters.bytes_written.load(Ordering::Relaxed) > 0);

    // Verify the parquet file was uploaded to storage
    let files: Vec<_> = ram_storage.list_files().await;
    assert!(
        files
            .iter()
            .any(|f| f.extension().map(|ext| ext == "parquet").unwrap_or(false)),
        "expected a parquet file in storage, found: {files:?}"
    );

    assert_eq!(publisher_handle.last_observation().num_published_splits, 1);

    universe.assert_quit().await;
}
