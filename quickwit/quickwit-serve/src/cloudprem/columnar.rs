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

//! Adapter for the columnar two-phase read API (`ListSplits` / `SearchSplitBatch`).
//!
//! This is the thin transport layer over the `quickwit-search`
//! `columnar_stream` primitive: it plans and scans from already-normalized
//! internal requests, encodes the opaque split token, and frames the Arrow
//! record batches into the gRPC server-stream.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::ipc::writer::StreamWriter;
use futures::Stream;
use prost::Message as _;
use quickwit_common::ServiceStream;
use quickwit_proto::cloudprem::{
    CloudPremError, CloudPremResult, SearchSplitBatchResponse, SplitBatch, SplitBatchDetails,
};
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::{
    ColumnarSplitBatch, ColumnarSplitPlanRequest, SearchService, SearchSplitBatchColumnarRequest,
    plan_columnar_splits,
};
use tokio_stream::StreamExt as _;
use tracing::warn;

/// Phase 1 — enumerate the splits a query touches.
pub(crate) async fn list_splits(
    metastore: &MetastoreServiceClient,
    plan_request: ColumnarSplitPlanRequest,
) -> CloudPremResult<impl Stream<Item = CloudPremResult<SplitBatch>> + use<>> {
    let batches = plan_columnar_splits(plan_request, metastore)
        .await
        .inspect_err(|error| warn!("list_splits planning failed: {error}"))?;

    Ok(batches.map(|batch_result| {
        let batch = batch_result.map_err(CloudPremError::from)?;
        encode_split_batch(batch)
    }))
}

fn encode_split_batch(batch: ColumnarSplitBatch) -> CloudPremResult<SplitBatch> {
    let details = SplitBatchDetails {
        index_uid: batch.index_uid.to_string(),
        index_uri: batch.index_uri,
        doc_mapper_str: batch.doc_mapper_str,
        splits: batch.splits,
    };
    Ok(SplitBatch {
        split_batch_details: details.encode_to_vec(),
        total_num_docs: batch.total_num_docs,
        total_size_bytes: batch.total_size_bytes,
    })
}

/// Phase 2 — read a column projection from a batch of splits, streamed as Arrow.
///
/// Returns a plain, unbuffered stream: nothing runs until it's polled, and
/// dropping it cooperatively cancels the underlying scan. The native record
/// batches from every split are framed together as one Arrow IPC stream.
pub(crate) async fn search_split_batch(
    search_service: &Arc<dyn SearchService>,
    request: SearchSplitBatchColumnarRequest,
) -> CloudPremResult<impl Stream<Item = CloudPremResult<SearchSplitBatchResponse>> + use<>> {
    let inner_stream = search_service.search_split_batch_columnar(request).await?;
    Ok(frame_arrow_stream(inner_stream))
}

/// Remaps the native columnar stream into the responses that go over the
/// gRPC server-stream: each response carries one raw Arrow IPC stream chunk
/// (the schema message, a record batch message, or the trailing EOS
/// marker). Arrow IPC messages are self-describing, so the caller doesn't
/// need to be told which is which — it just concatenates the chunks, in
/// order, into a single byte stream and feeds it to an Arrow IPC stream
/// reader. A split with no matches yields no batches, so the response
/// stream is empty — no schema message is sent.
///
/// This is a plain, lazy remapping — it holds the `StreamWriter` state
/// itself but spawns no task and buffers nothing; each response is produced
/// only as the returned stream is polled.
fn frame_arrow_stream(
    mut inner: ServiceStream<quickwit_search::Result<RecordBatch>>,
) -> impl Stream<Item = CloudPremResult<SearchSplitBatchResponse>> {
    async_stream::stream! {
        let mut writer: Option<StreamWriter<Vec<u8>>> = None;
        while let Some(item) = inner.next().await {
            let batch = match item {
                Ok(batch) => batch,
                Err(search_error) => {
                    yield Err(CloudPremError::from(search_error));
                    return;
                }
            };
            let current_writer = match writer.as_mut() {
                Some(existing_writer) => existing_writer,
                None => {
                    let mut new_writer = match StreamWriter::try_new(Vec::new(), &batch.schema()) {
                        Ok(new_writer) => new_writer,
                        Err(error) => {
                            yield Err(ipc_error(error));
                            return;
                        }
                    };
                    let schema_bytes = std::mem::take(new_writer.get_mut());
                    yield Ok(ipc_response(schema_bytes));
                    writer.insert(new_writer)
                }
            };
            if let Err(error) = current_writer.write(&batch) {
                yield Err(ipc_error(error));
                return;
            }
            let bytes = std::mem::take(current_writer.get_mut());
            yield Ok(ipc_response(bytes));
        }
        if let Some(mut writer) = writer {
            match writer.finish() {
                Err(error) => yield Err(ipc_error(error)),
                Ok(()) => {
                    let bytes = std::mem::take(writer.get_mut());
                    if !bytes.is_empty() {
                        yield Ok(ipc_response(bytes));
                    }
                }
            }
        }
    }
}

fn ipc_response(arrow_ipc_message: Vec<u8>) -> SearchSplitBatchResponse {
    SearchSplitBatchResponse { arrow_ipc_message }
}

fn ipc_error(error: arrow::error::ArrowError) -> CloudPremError {
    CloudPremError::Internal(format!("arrow ipc error: {error}"))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::StreamReader;
    use futures::stream;
    use quickwit_proto::search::SplitIdAndFooterOffsets;

    use super::*;

    #[tokio::test]
    async fn multiple_record_batches_are_framed_as_one_arrow_stream() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let first_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let second_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![3, 4]))]).unwrap();
        let native_stream = ServiceStream::new(Box::pin(stream::iter(vec![
            Ok(first_batch),
            Ok(second_batch),
        ])));

        let mut ipc_bytes = Vec::new();
        let response_stream = frame_arrow_stream(native_stream);
        futures::pin_mut!(response_stream);
        while let Some(response_result) = response_stream.next().await {
            ipc_bytes.extend(response_result.unwrap().arrow_ipc_message);
        }

        let reader = StreamReader::try_new(Cursor::new(ipc_bytes), None).unwrap();
        let decoded_row_counts: Vec<usize> = reader
            .map(|batch_result| batch_result.unwrap().num_rows())
            .collect();
        assert_eq!(decoded_row_counts, vec![2, 2]);
    }

    #[test]
    fn split_batch_exposes_only_aggregate_estimates() {
        let batch = ColumnarSplitBatch {
            index_uid: quickwit_proto::types::IndexUid::for_test("my-index", 1),
            index_uri: "s3://bucket/my-index".to_string(),
            doc_mapper_str: "{}".to_string(),
            total_num_docs: 100,
            total_size_bytes: 70,
            splits: vec![
                SplitIdAndFooterOffsets {
                    split_id: "split-1".to_string(),
                    split_footer_start: 10,
                    split_footer_end: 20,
                    timestamp_start: Some(100),
                    timestamp_end: Some(200),
                    num_docs: 42,
                },
                SplitIdAndFooterOffsets {
                    split_id: "split-2".to_string(),
                    split_footer_start: 20,
                    split_footer_end: 50,
                    timestamp_start: Some(201),
                    timestamp_end: Some(300),
                    num_docs: 58,
                },
            ],
        };

        let response = encode_split_batch(batch).unwrap();
        assert_eq!(response.total_num_docs, 100);
        assert_eq!(response.total_size_bytes, 70);
        let details = SplitBatchDetails::decode(response.split_batch_details.as_slice()).unwrap();
        assert_eq!(details.index_uri, "s3://bucket/my-index");
        assert_eq!(details.doc_mapper_str, "{}");
        assert_eq!(details.splits.len(), 2);
    }
}
