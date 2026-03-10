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
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::variant::{VariantArrayBuilder, VariantBuilderExt};
use quickwit_actors::{ActorHandle, Universe};
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_metastore::checkpoint::SourceCheckpointDelta;
use quickwit_parquet_engine::ingest::record_batch_to_ipc;
use quickwit_parquet_engine::schema::ParquetSchema;
use quickwit_parquet_engine::storage::{ParquetSplitWriter, ParquetWriterConfig};
use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
use quickwit_storage::RamStorage;
use quickwit_proto::types::IndexUid;
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

fn create_dict_array(values: &[&str]) -> ArrayRef {
    let keys: Vec<i32> = (0..values.len()).map(|i| i as i32).collect();
    let string_array = StringArray::from(values.to_vec());
    Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), Arc::new(string_array))
            .unwrap(),
    )
}

fn create_nullable_dict_array(values: &[Option<&str>]) -> ArrayRef {
    let keys: Vec<Option<i32>> = values
        .iter()
        .enumerate()
        .map(|(i, v)| v.map(|_| i as i32))
        .collect();
    let string_values: Vec<&str> = values.iter().filter_map(|v| *v).collect();
    let string_array = StringArray::from(string_values);
    Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), Arc::new(string_array))
            .unwrap(),
    )
}

fn create_variant_array(num_rows: usize, fields: Option<&[(&str, &str)]>) -> ArrayRef {
    let mut builder = VariantArrayBuilder::new(num_rows);
    for _ in 0..num_rows {
        match fields {
            Some(kv_pairs) => {
                let mut obj = builder.new_object();
                for &(key, value) in kv_pairs {
                    obj = obj.with_field(key, value);
                }
                obj.finish();
            }
            None => {
                builder.append_null();
            }
        }
    }
    ArrayRef::from(builder.build())
}

fn create_test_batch(
    num_rows: usize,
    metric_name: &str,
    service: &str,
    base_timestamp: u64,
    base_value: f64,
) -> RecordBatch {
    let schema = ParquetSchema::new();

    let metric_names: Vec<&str> = vec![metric_name; num_rows];
    let metric_name_arr: ArrayRef = create_dict_array(&metric_names);
    let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
    let metric_unit: ArrayRef = Arc::new(StringArray::from(vec![Some("count"); num_rows]));
    let timestamps: Vec<u64> = (0..num_rows).map(|i| base_timestamp + i as u64).collect();
    let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
    let start_timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(vec![None::<u64>; num_rows]));
    let values: Vec<f64> = (0..num_rows).map(|i| base_value + i as f64).collect();
    let value: ArrayRef = Arc::new(Float64Array::from(values));
    let tag_service: ArrayRef = create_nullable_dict_array(&vec![Some(service); num_rows]);
    let tag_env: ArrayRef = create_nullable_dict_array(&vec![Some("prod"); num_rows]);
    let tag_datacenter: ArrayRef = create_nullable_dict_array(&vec![Some("us-east-1"); num_rows]);
    let tag_region: ArrayRef = create_nullable_dict_array(&vec![None; num_rows]);
    let tag_host: ArrayRef = create_nullable_dict_array(&vec![Some("host-001"); num_rows]);
    let attributes: ArrayRef = create_variant_array(num_rows, None);
    let service_names: Vec<&str> = vec![service; num_rows];
    let service_name: ArrayRef = create_dict_array(&service_names);
    let resource_attributes: ArrayRef = create_variant_array(num_rows, None);

    RecordBatch::try_new(
        schema.arrow_schema().clone(),
        vec![
            metric_name_arr,
            metric_type,
            metric_unit,
            timestamp_secs,
            start_timestamp_secs,
            value,
            tag_service,
            tag_env,
            tag_datacenter,
            tag_region,
            tag_host,
            attributes,
            service_name,
            resource_attributes,
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
    let parquet_schema = ParquetSchema::new();
    let writer_config = ParquetWriterConfig::default();
    let split_writer = ParquetSplitWriter::new(parquet_schema, writer_config, temp_dir.path());
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

    assert_eq!(indexer_counters.batches_received.load(Ordering::Relaxed), 5);
    assert_eq!(indexer_counters.rows_indexed.load(Ordering::Relaxed), 100);
    assert_eq!(
        indexer_counters.batches_flushed.load(Ordering::Relaxed),
        0,
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

    assert_eq!(
        doc_processor_counters.valid_batches.load(Ordering::Relaxed),
        6
    );
    assert_eq!(
        doc_processor_counters.valid_rows.load(Ordering::Relaxed),
        110
    );
    assert_eq!(
        doc_processor_counters.parse_errors.load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        indexer_counters.batches_flushed.load(Ordering::Relaxed),
        1,
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
        ListMetricsSplitsRequestExt, ListMetricsSplitsResponseExt, StageMetricsSplitsRequestExt,
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
        .with_split_states(vec!["Staged".to_string()]);
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
        .with_split_states(vec!["Published".to_string()]);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let published: Vec<MetricsSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].metadata.split_id, split1.split_id);

    // Time range filtering
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec!["Published".to_string()])
        .with_time_range(1000, 1100);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let in_range: Vec<MetricsSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(in_range.len(), 1);

    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec!["Published".to_string()])
        .with_time_range(5000, 5100);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let out_of_range: Vec<MetricsSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(out_of_range.len(), 0);

    // Metric name filtering
    let query = ListMetricsSplitsQuery::for_index(index_uid.clone())
        .with_split_states(vec!["Published".to_string()])
        .with_metric_names(vec!["cpu.usage".to_string()]);
    let list_request = ListMetricsSplitsRequest::try_from_query(index_uid.clone(), &query).unwrap();
    let list_response = metastore.list_metrics_splits(list_request).await.unwrap();
    let by_metric: Vec<MetricsSplitRecord> = list_response.deserialize_splits().unwrap();
    assert_eq!(by_metric.len(), 1);
}
