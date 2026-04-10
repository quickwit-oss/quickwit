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

//! End-to-end tests for the metrics pipeline.
//!
//! These tests wire up the full metrics pipeline:
//! ParquetDocProcessor → ParquetIndexer → ParquetUploader → ParquetPublisher

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow::array::{
    ArrayRef, DictionaryArray, Float64Array, Int32Array, StringArray, UInt8Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use quickwit_actors::{ActorHandle, Universe};
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_metastore::checkpoint::SourceCheckpointDelta;
use quickwit_parquet_engine::ingest::record_batch_to_ipc;
use quickwit_parquet_engine::storage::{ParquetSplitWriter, ParquetWriterConfig};
use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
use quickwit_proto::types::IndexUid;
use quickwit_storage::RamStorage;

use crate::actors::{
    ParquetDocProcessor, ParquetIndexer, ParquetPackager, ParquetPublisher, ParquetUploader,
    PublisherType, SplitsUpdateMailbox, UploaderType,
};
use crate::models::RawDocBatch;

// =============================================================================
// Helpers
// =============================================================================

async fn wait_for_published_splits(
    publisher_handle: &ActorHandle<ParquetPublisher>,
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
// Tests
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

    let publisher = ParquetPublisher::new(
        PublisherType::ParquetPublisher,
        metastore_client.clone(),
        None,
        None,
    );
    let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

    let uploader = ParquetUploader::new(
        UploaderType::IndexUploader,
        metastore_client.clone(),
        ram_storage,
        SplitsUpdateMailbox::Publisher(publisher_mailbox),
        4,
    );
    let (uploader_mailbox, _uploader_handle) = universe.spawn_builder().spawn(uploader);

    // ParquetPackager between indexer and uploader
    let writer_config = ParquetWriterConfig::default();
    let table_config = quickwit_parquet_engine::table_config::TableConfig::default();
    let split_writer = ParquetSplitWriter::new(writer_config, temp_dir.path(), &table_config);
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

/// FileBackedMetastore metrics splits operations (no mocks).
///
/// Validates stage, publish, list (with time range and metric name filtering).
#[tokio::test]
async fn test_file_backed_metastore_metrics_operations() {
    use quickwit_config::IndexConfig;
    use quickwit_metastore::{
        CreateIndexRequestExt, FileBackedMetastore, ListMetricsSplitsQuery,
        ListMetricsSplitsRequestExt, ListMetricsSplitsResponseExt, SplitState,
        StageMetricsSplitsRequestExt,
    };
    use quickwit_parquet_engine::split::{MetricsSplitMetadata, MetricsSplitRecord, TimeRange};
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

    let split1 = MetricsSplitMetadata::builder()
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(1000, 1100))
        .num_rows(100)
        .size_bytes(1024)
        .add_metric_name("cpu.usage")
        .build();

    let split2 = MetricsSplitMetadata::builder()
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
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states([SplitState::Staged]);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let staged: Vec<MetricsSplitRecord> = list_response.deserialize_splits().unwrap();
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
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states([SplitState::Published]);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let published: Vec<MetricsSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].metadata.split_id, split1.split_id);

    // Time range filtering
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states([SplitState::Published])
        .with_time_range_start_gte(1000)
        .with_time_range_end_lte(1100);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let in_range: Vec<MetricsSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(in_range.len(), 1);

    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states([SplitState::Published])
        .with_time_range_start_gte(5000)
        .with_time_range_end_lte(5100);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let out_of_range: Vec<MetricsSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(out_of_range.len(), 0);

    // Metric name filtering
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states([SplitState::Published])
        .with_metric_names(vec!["cpu.usage".to_string()]);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let by_metric: Vec<MetricsSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(by_metric.len(), 1);
}
