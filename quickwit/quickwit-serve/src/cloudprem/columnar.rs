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

//! Adapter for the columnar two-phase read API (`ListSplits` / `SearchSplit`).
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
    CloudPremError, CloudPremResult, ListSplitsResponse, SearchSplitResponse, SplitDescriptor,
    SplitToken,
};
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::{
    ColumnarSplitPlanRequest, SearchService, SearchSplitColumnarRequest, plan_columnar_splits,
};
use tokio_stream::StreamExt as _;
use tracing::warn;

/// Phase 1 — enumerate the splits a query touches.
pub(crate) async fn list_splits(
    metastore: &MetastoreServiceClient,
    plan_request: ColumnarSplitPlanRequest,
) -> CloudPremResult<ListSplitsResponse> {
    let descriptors = plan_columnar_splits(plan_request, metastore)
        .await
        .inspect_err(|error| warn!("list_splits planning failed: {error}"))?;

    let splits = descriptors
        .into_iter()
        .map(|descriptor| {
            let token = SplitToken {
                index_uid: descriptor.index_uid.to_string(),
                index_uri: descriptor.index_uri,
                split: Some(descriptor.split.clone()),
                doc_mapper_str: descriptor.doc_mapper_str,
            };
            SplitDescriptor {
                split_token: token.encode_to_vec(),
                split_id: descriptor.split.split_id,
                index_uid: descriptor.index_uid.to_string(),
                num_docs: descriptor.split.num_docs,
                size_bytes: descriptor.size_bytes,
                time_range_start_ms: descriptor.split.timestamp_start.map(secs_to_ms),
                time_range_end_ms: descriptor.split.timestamp_end.map(secs_to_ms),
                preferred_node_ids: Vec::new(),
            }
        })
        .collect();
    Ok(ListSplitsResponse { splits })
}

/// Phase 2 — read a column projection from one split, streamed as Arrow.
///
/// Returns a plain, unbuffered stream: nothing runs until it's polled, and
/// dropping it cooperatively cancels the underlying scan. Whether to buffer
/// responses ahead of a slow gRPC consumer is decided once, by the caller, at
/// the gRPC boundary (`CloudPremServiceImpl::search_split`).
pub(crate) async fn search_split(
    search_service: &Arc<dyn SearchService>,
    request: SearchSplitColumnarRequest,
) -> CloudPremResult<impl Stream<Item = CloudPremResult<SearchSplitResponse>> + use<>> {
    let inner_stream = search_service.search_split_columnar(request).await?;
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
) -> impl Stream<Item = CloudPremResult<SearchSplitResponse>> {
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

fn ipc_response(arrow_ipc_message: Vec<u8>) -> SearchSplitResponse {
    SearchSplitResponse { arrow_ipc_message }
}

fn ipc_error(error: arrow::error::ArrowError) -> CloudPremError {
    CloudPremError::Internal(format!("arrow ipc error: {error}"))
}

fn secs_to_ms(timestamp_secs: i64) -> i64 {
    timestamp_secs.saturating_mul(1000)
}

#[cfg(test)]
mod tests {
    use quickwit_proto::search::SplitIdAndFooterOffsets;

    use super::*;

    #[test]
    fn split_token_round_trips() {
        let token = SplitToken {
            index_uid: "my-index:01H".to_string(),
            index_uri: "s3://bucket/my-index".to_string(),
            split: Some(SplitIdAndFooterOffsets {
                split_id: "split-1".to_string(),
                split_footer_start: 10,
                split_footer_end: 20,
                timestamp_start: Some(100),
                timestamp_end: Some(200),
                num_docs: 42,
            }),
            doc_mapper_str: "{}".to_string(),
        };
        let bytes = token.encode_to_vec();
        let decoded = SplitToken::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, token);
    }

    #[test]
    fn secs_to_ms_scales_up() {
        assert_eq!(secs_to_ms(3), 3_000);
    }
}
