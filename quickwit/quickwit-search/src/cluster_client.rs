// Copyright (C) 2023 Quickwit, Inc.
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

use std::time::Duration;

use base64::Engine;
use futures::future::ready;
use futures::{Future, StreamExt};
use quickwit_proto::search::{
    FetchDocsRequest, FetchDocsResponse, GetKvRequest, LeafListTermsRequest, LeafListTermsResponse,
    LeafSearchRequest, LeafSearchResponse, LeafSearchStreamRequest, LeafSearchStreamResponse,
    PutKvRequest,
};
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info, warn};

use crate::retry::search::LeafSearchRetryPolicy;
use crate::retry::search_stream::{LeafSearchStreamRetryPolicy, SuccessfulSplitIds};
use crate::retry::{retry_client, DefaultRetryPolicy, RetryPolicy};
use crate::{SearchError, SearchJobPlacer, SearchServiceClient};

/// Maximum number of put requests emitted to perform a replicated given PUT KV.
const MAX_PUT_KV_ATTEMPTS: usize = 6;

/// Maximum number of get requests emitted to perform a GET KV request.
const MAX_GET_KV_ATTEMPTS: usize = 6;

/// We attempt to store our KVs on two nodes.
const TARGET_NUM_REPLICATION: usize = 2;

/// Client that executes placed requests (Request, `SearchServiceClient`) and provides
/// retry policies for `FetchDocsRequest`, `LeafSearchRequest` and `LeafSearchStreamRequest`
/// to retry on other `SearchServiceClient`.
#[derive(Clone)]
pub struct ClusterClient {
    pub(crate) search_job_placer: SearchJobPlacer,
}

impl ClusterClient {
    /// Instantiates [`ClusterClient`].
    pub fn new(search_job_placer: SearchJobPlacer) -> Self {
        Self { search_job_placer }
    }

    /// Fetches docs with retry on another node client.
    pub async fn fetch_docs(
        &self,
        request: FetchDocsRequest,
        mut client: SearchServiceClient,
    ) -> crate::Result<FetchDocsResponse> {
        let mut response_res = client.fetch_docs(request.clone()).await;
        let retry_policy = DefaultRetryPolicy {};
        if let Some(retry_request) = retry_policy.retry_request(request, &response_res) {
            assert!(!retry_request.split_offsets.is_empty());
            client = retry_client(
                &self.search_job_placer,
                client.grpc_addr(),
                &retry_request.split_offsets[0].split_id,
            )
            .await?;
            debug!(
                "Fetch docs response error: `{:?}`. Retry once to execute {:?} with {:?}",
                response_res, retry_request, client
            );
            response_res = client.fetch_docs(retry_request).await;
        }
        response_res
    }

    /// Leaf search with retry on another node client.
    pub async fn leaf_search(
        &self,
        request: LeafSearchRequest,
        mut client: SearchServiceClient,
    ) -> crate::Result<LeafSearchResponse> {
        let mut response_res = client.leaf_search(request.clone()).await;
        let retry_policy = LeafSearchRetryPolicy {};
        if let Some(retry_request) = retry_policy.retry_request(request, &response_res) {
            assert!(!retry_request.split_offsets.is_empty());
            client = retry_client(
                &self.search_job_placer,
                client.grpc_addr(),
                &retry_request.split_offsets[0].split_id,
            )
            .await?;
            debug!(
                "Leaf search response error: `{:?}`. Retry once to execute {:?} with {:?}",
                response_res, retry_request, client
            );
            let retry_result = client.leaf_search(retry_request).await;
            response_res = merge_leaf_search_results(response_res, retry_result);
        }
        response_res
    }

    /// Leaf search stream with retry on another node client.
    pub async fn leaf_search_stream(
        &self,
        request: LeafSearchStreamRequest,
        mut client: SearchServiceClient,
    ) -> UnboundedReceiverStream<crate::Result<LeafSearchStreamResponse>> {
        // We need a dedicated channel to send results with retry. First we send only the successful
        // responses and and ignore errors. If there are some errors, we make one retry and
        // in this case we send all results.
        let (result_sender, result_receiver) = unbounded_channel();
        let client_pool = self.search_job_placer.clone();
        let retry_policy = LeafSearchStreamRetryPolicy {};
        tokio::spawn(async move {
            let result_stream = client.leaf_search_stream(request.clone()).await;
            // Forward only responses and not errors to the sender as we will make one retry on
            // errors.
            let forward_result =
                forward_leaf_search_stream(result_stream, result_sender.clone(), false).await;
            if let Some(retry_request) = retry_policy.retry_request(request, &forward_result) {
                assert!(!retry_request.split_offsets.is_empty());
                let retry_client_res = retry_client(
                    &client_pool,
                    client.grpc_addr(),
                    &retry_request.split_offsets[0].split_id,
                )
                .await;
                let mut retry_client = match retry_client_res {
                    Ok(retry_client) => retry_client,
                    Err(error) => {
                        // Propagates the error if we cannot get a new client and stops the task.
                        let _ = result_sender.send(Err(SearchError::from(error)));
                        return;
                    }
                };
                debug!(
                    "Leaf search stream response error. Retry once to execute {:?} with {:?}",
                    retry_request, client
                );
                let retry_results_stream = retry_client.leaf_search_stream(retry_request).await;
                // Forward all results to the result_sender as we won't do another retry.
                // It is ok to ignore send errors, there is nothing else to do.
                let _ =
                    forward_leaf_search_stream(retry_results_stream, result_sender.clone(), true)
                        .await;
            }
        });

        UnboundedReceiverStream::new(result_receiver)
    }

    /// Leaf search with retry on another node client.
    pub async fn leaf_list_terms(
        &self,
        request: LeafListTermsRequest,
        mut client: SearchServiceClient,
    ) -> crate::Result<LeafListTermsResponse> {
        // TODO: implement retry
        client.leaf_list_terms(request.clone()).await
    }

    /// Attempts to store a given search context within the cluster.
    ///
    /// This function may fail silently, if no clients was available.
    pub async fn put_kv(&self, key: &[u8], payload: &[u8], ttl: Duration) {
        let clients: Vec<SearchServiceClient> = self
            .search_job_placer
            .best_nodes_per_affinity(key)
            .await
            .take(MAX_PUT_KV_ATTEMPTS)
            .collect();

        if clients.is_empty() {
            // We only log a warning as it might be that we are just running in a
            // single node cluster.
            // (That's odd though, the node running this code should be in the pool too)
            warn!("No other node available to replicate scroll context.");
            return;
        }

        // We run the put requests concurrently.
        // Our target is a replication over TARGET_NUM_REPLICATION nodes, we therefore try to avoid
        // replicating on more than TARGET_NUM_REPLICATION nodes at the same time. Of
        // course, this may still result in the replication over more nodes, but this is not
        // a problem.
        //
        // The requests are made in a concurrent manner, up to two at a time. As soon as 2 requests
        // are successful, we stop.
        let put_kv_futs = clients
            .into_iter()
            .map(|client| replicate_kv_to_one_server(client, key, payload, ttl));
        let successful_replication = futures::stream::iter(put_kv_futs)
            .buffer_unordered(TARGET_NUM_REPLICATION)
            .filter(|put_kv_successful| ready(*put_kv_successful))
            .take(TARGET_NUM_REPLICATION)
            .count()
            .await;

        if successful_replication == 0 {
            error!(successful_replication=%successful_replication,"failed-to-replicate-scroll-context");
        }
    }

    /// Returns a search_after context
    pub async fn get_kv(&self, key: &[u8]) -> Option<Vec<u8>> {
        let clients = self.search_job_placer.best_nodes_per_affinity(key).await;
        // On the read side, we attempt to contact up to 6 nodes.
        for mut client in clients.take(MAX_GET_KV_ATTEMPTS) {
            let get_request = GetKvRequest { key: key.to_vec() };
            if let Ok(Some(search_after_resp)) = client.get_kv(get_request.clone()).await {
                return Some(search_after_resp);
            } else {
                let base64_key: String = base64::prelude::BASE64_STANDARD.encode(key);
                info!(destination=?client, key=base64_key, "Failed to get KV");
            }
        }
        None
    }
}

fn replicate_kv_to_one_server(
    mut client: SearchServiceClient,
    key: &[u8],
    payload: &[u8],
    ttl: Duration,
) -> impl Future<Output = bool> {
    let put_kv_request = PutKvRequest {
        key: key.to_vec(),
        payload: payload.to_vec(),
        ttl_secs: ttl.as_secs() as u32,
    };
    let base64_key: String = base64::prelude::BASE64_STANDARD.encode(key);
    async move {
        if client.put_kv(put_kv_request).await.is_ok() {
            true
        } else {
            warn!(destination=?client, key=base64_key, "Failed to replicate KV");
            false
        }
    }
}

/// Takes two intermediate aggregation results serialized using postcard,
/// merge them and returns the merged serialized result.
fn merge_intermediate_aggregation(left: &[u8], right: &[u8]) -> crate::Result<Vec<u8>> {
    let mut intermediate_aggregation_results_left: IntermediateAggregationResults =
        postcard::from_bytes(left)?;
    let intermediate_aggregation_results_right: IntermediateAggregationResults =
        postcard::from_bytes(right)?;
    intermediate_aggregation_results_left.merge_fruits(intermediate_aggregation_results_right)?;
    let serialized = postcard::to_allocvec(&intermediate_aggregation_results_left)?;
    Ok(serialized)
}

fn merge_leaf_search_response(
    mut left_response: LeafSearchResponse,
    right_response: LeafSearchResponse,
) -> crate::Result<LeafSearchResponse> {
    left_response
        .partial_hits
        .extend(right_response.partial_hits);
    let intermediate_aggregation_result: Option<Vec<u8>> = match (
        left_response.intermediate_aggregation_result,
        right_response.intermediate_aggregation_result,
    ) {
        (Some(left_agg_bytes), Some(right_agg_bytes)) => {
            let intermediate_aggregation_bytes: Vec<u8> =
                merge_intermediate_aggregation(&left_agg_bytes[..], &right_agg_bytes[..])?;
            Some(intermediate_aggregation_bytes)
        }
        (None, Some(right)) => Some(right),
        (Some(left), None) => Some(left),
        (None, None) => None,
    };
    Ok(LeafSearchResponse {
        intermediate_aggregation_result,
        num_hits: left_response.num_hits + right_response.num_hits,
        num_attempted_splits: left_response.num_attempted_splits
            + right_response.num_attempted_splits,
        failed_splits: right_response.failed_splits,
        partial_hits: left_response.partial_hits,
    })
}

// Merge initial leaf search results with results obtained from a retry.
fn merge_leaf_search_results(
    left_search_response_result: crate::Result<LeafSearchResponse>,
    right_search_response_result: crate::Result<LeafSearchResponse>,
) -> crate::Result<LeafSearchResponse> {
    match (left_search_response_result, right_search_response_result) {
        (Ok(left_response), Ok(right_response)) => {
            merge_leaf_search_response(left_response, right_response)
        }
        (Ok(single_valid_response), Err(_)) => Ok(single_valid_response),
        (Err(_), Ok(single_valid_response)) => Ok(single_valid_response),
        (Err(error), Err(_)) => Err(error),
    }
}

// Forward leaf search stream results into a sender and
// returns the split ids of Ok(response).
// If `send_error` is false, errors are ignored and not forwarded. This is
// useful if you want to make a retry before propagating errors.
async fn forward_leaf_search_stream(
    mut stream: UnboundedReceiverStream<crate::Result<LeafSearchStreamResponse>>,
    sender: UnboundedSender<crate::Result<LeafSearchStreamResponse>>,
    send_error: bool,
) -> Result<SuccessfulSplitIds, SendError<crate::Result<LeafSearchStreamResponse>>> {
    let mut successful_split_ids: Vec<String> = Vec::new();
    while let Some(result) = stream.next().await {
        match result {
            Ok(response) => {
                successful_split_ids.push(response.split_id.clone());
                sender.send(Ok(response))?;
            }
            Err(error) => {
                if send_error {
                    sender.send(Err(error))?;
                }
            }
        }
    }
    Ok(SuccessfulSplitIds(successful_split_ids))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;

    use quickwit_proto::search::{
        PartialHit, SearchRequest, SearchStreamRequest, SortValue, SplitIdAndFooterOffsets,
        SplitSearchError,
    };
    use quickwit_query::query_ast::qast_json_helper;

    use super::*;
    use crate::root::SearchJob;
    use crate::{searcher_pool_for_test, MockSearchService};

    fn mock_partial_hit(split_id: &str, sort_value: u64, doc_id: u32) -> PartialHit {
        PartialHit {
            sort_value: Some(SortValue::U64(sort_value).into()),
            sort_value2: None,
            split_id: split_id.to_string(),
            segment_ord: 1,
            doc_id,
        }
    }

    fn mock_doc_request(split_id: &str) -> FetchDocsRequest {
        FetchDocsRequest {
            partial_hits: Vec::new(),
            index_uri: "uri".to_string(),
            split_offsets: vec![SplitIdAndFooterOffsets {
                split_id: split_id.to_string(),
                split_footer_end: 100,
                split_footer_start: 0,
                timestamp_start: None,
                timestamp_end: None,
            }],
            ..Default::default()
        }
    }

    fn mock_leaf_search_request() -> LeafSearchRequest {
        let search_request = SearchRequest {
            index_id_patterns: vec!["test-idx".to_string()],
            query_ast: qast_json_helper("test", &["body"]),
            max_hits: 10,
            ..Default::default()
        };
        LeafSearchRequest {
            search_request: Some(search_request),
            doc_mapper: "doc_mapper".to_string(),
            index_uri: "uri".to_string(),
            split_offsets: vec![
                SplitIdAndFooterOffsets {
                    split_id: "split_1".to_string(),
                    split_footer_start: 0,
                    split_footer_end: 100,
                    timestamp_start: None,
                    timestamp_end: None,
                },
                SplitIdAndFooterOffsets {
                    split_id: "split_2".to_string(),
                    split_footer_start: 0,
                    split_footer_end: 100,
                    timestamp_start: None,
                    timestamp_end: None,
                },
            ],
        }
    }

    fn mock_leaf_search_stream_request() -> LeafSearchStreamRequest {
        let search_request = SearchStreamRequest {
            index_id: "test-idx".to_string(),
            query_ast: qast_json_helper("text", &["body"]),
            snippet_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            fast_field: "fast".to_string(),
            output_format: 0,
            partition_by_field: None,
        };
        LeafSearchStreamRequest {
            request: Some(search_request),
            doc_mapper: "doc_mapper".to_string(),
            index_uri: "uri".to_string(),
            split_offsets: vec![
                SplitIdAndFooterOffsets {
                    split_id: "split_1".to_string(),
                    split_footer_start: 0,
                    split_footer_end: 100,
                    timestamp_start: None,
                    timestamp_end: None,
                },
                SplitIdAndFooterOffsets {
                    split_id: "split_2".to_string(),
                    split_footer_start: 0,
                    split_footer_end: 100,
                    timestamp_start: None,
                    timestamp_end: None,
                },
            ],
        }
    }

    #[tokio::test]
    async fn test_cluster_client_fetch_docs_no_retry() {
        let request = mock_doc_request("split_1");
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_fetch_docs().return_once(
            |_: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse { hits: Vec::new() })
            },
        );
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let first_client = search_job_placer
            .assign_job(SearchJob::for_test("split_1", 0), &HashSet::new())
            .await
            .unwrap();
        let cluster_client = ClusterClient::new(search_job_placer);
        let fetch_docs_response = cluster_client
            .fetch_docs(request, first_client)
            .await
            .unwrap();
        assert_eq!(fetch_docs_response.hits.len(), 0);
    }

    #[tokio::test]
    async fn test_cluster_client_fetch_docs_retry_with_final_success() {
        let request = mock_doc_request("split_1");
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1.expect_fetch_docs().return_once(
            |_: quickwit_proto::search::FetchDocsRequest| {
                Err(SearchError::Internal("error".to_string()))
            },
        );
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_fetch_docs().return_once(
            |_: quickwit_proto::search::FetchDocsRequest| {
                Ok(quickwit_proto::search::FetchDocsResponse { hits: Vec::new() })
            },
        );
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
        ]);
        let first_client_addr: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let first_client = searcher_pool.get(&first_client_addr).unwrap();
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer);
        let fetch_docs_response = cluster_client
            .fetch_docs(request, first_client)
            .await
            .unwrap();
        assert_eq!(fetch_docs_response.hits.len(), 0);
    }

    #[tokio::test]
    async fn test_cluster_client_fetch_docs_retry_with_final_error() {
        let request = mock_doc_request("split_1");
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_fetch_docs().returning(
            |_: quickwit_proto::search::FetchDocsRequest| {
                Err(SearchError::Internal("error".to_string()))
            },
        );
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let first_client_addr: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let first_client = searcher_pool.get(&first_client_addr).unwrap();
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer);
        let search_error = cluster_client
            .fetch_docs(request, first_client)
            .await
            .unwrap_err();
        assert!(matches!(search_error, SearchError::Internal(_)));
    }

    #[tokio::test]
    async fn test_cluster_client_leaf_search_no_retry() {
        let request = mock_leaf_search_request();
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_leaf_search()
            .return_once(|_: LeafSearchRequest| {
                Ok(LeafSearchResponse {
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            });
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let first_client = search_job_placer
            .assign_job(SearchJob::for_test("split_1", 0), &HashSet::new())
            .await
            .unwrap();
        let cluster_client = ClusterClient::new(search_job_placer);
        let leaf_search_response = cluster_client
            .leaf_search(request, first_client)
            .await
            .unwrap();
        assert_eq!(leaf_search_response.num_attempted_splits, 1);
    }

    #[tokio::test]
    async fn test_cluster_client_leaf_search_retry_on_failing_splits() {
        let request = mock_leaf_search_request();
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_leaf_search()
            .withf(|request| request.split_offsets[0].split_id == "split_1")
            .return_once(|_: LeafSearchRequest| {
                Ok(LeafSearchResponse {
                    num_hits: 1,
                    failed_splits: vec![SplitSearchError {
                        error: "mock_error".to_string(),
                        split_id: "split_2".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            });
        mock_search_service
            .expect_leaf_search()
            .withf(|request| request.split_offsets[0].split_id == "split_2")
            .return_once(|_: LeafSearchRequest| {
                Ok(LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: Vec::new(),
                    failed_splits: vec![SplitSearchError {
                        error: "mock_error".to_string(),
                        split_id: "split_3".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            });
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let first_client = search_job_placer
            .assign_job(SearchJob::for_test("split_1", 0), &HashSet::new())
            .await
            .unwrap();
        let cluster_client = ClusterClient::new(search_job_placer);
        let result = cluster_client.leaf_search(request, first_client).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().num_hits, 2);
    }

    #[test]
    fn test_merge_leaf_search_retry_on_partial_success() -> anyhow::Result<()> {
        let split_error = SplitSearchError {
            error: "error".to_string(),
            split_id: "split_2".to_string(),
            retryable_error: true,
        };
        let leaf_response = LeafSearchResponse {
            num_hits: 1,
            partial_hits: vec![mock_partial_hit("split_1", 3, 1)],
            failed_splits: vec![split_error],
            num_attempted_splits: 1,
            ..Default::default()
        };
        let leaf_response_retry = LeafSearchResponse {
            num_hits: 1,
            partial_hits: vec![mock_partial_hit("split_2", 3, 1)],
            failed_splits: Vec::new(),
            num_attempted_splits: 1,
            ..Default::default()
        };
        let merged_leaf_search_response =
            merge_leaf_search_results(Ok(leaf_response), Ok(leaf_response_retry)).unwrap();
        assert_eq!(merged_leaf_search_response.num_attempted_splits, 2);
        assert_eq!(merged_leaf_search_response.num_hits, 2);
        assert_eq!(merged_leaf_search_response.partial_hits.len(), 2);
        assert_eq!(merged_leaf_search_response.failed_splits.len(), 0);
        Ok(())
    }

    #[test]
    fn test_merge_leaf_search_retry_on_error() -> anyhow::Result<()> {
        let split_error = SplitSearchError {
            error: "error".to_string(),
            split_id: "split_2".to_string(),
            retryable_error: true,
        };
        let leaf_response = LeafSearchResponse {
            num_hits: 1,
            partial_hits: vec![mock_partial_hit("split_1", 3, 1)],
            failed_splits: vec![split_error],
            num_attempted_splits: 1,
            ..Default::default()
        };
        let merged_result = merge_leaf_search_results(
            Err(SearchError::Internal("error".to_string())),
            Ok(leaf_response),
        )
        .unwrap();
        assert_eq!(merged_result.num_attempted_splits, 1);
        assert_eq!(merged_result.num_hits, 1);
        assert_eq!(merged_result.partial_hits.len(), 1);
        assert_eq!(merged_result.failed_splits.len(), 1);
        Ok(())
    }

    #[test]
    fn test_merge_leaf_search_retry_error_on_error() -> anyhow::Result<()> {
        let merge_error = merge_leaf_search_results(
            Err(SearchError::Internal("error".to_string())),
            Err(SearchError::Internal("retry error".to_string())),
        )
        .unwrap_err();
        assert_eq!(merge_error.to_string(), "internal error: `error`");
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_client_leaf_stream_retry_on_error() {
        let request = mock_leaf_search_stream_request();

        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1
            .expect_leaf_search_stream()
            .return_once(|_| Err(SearchError::Internal("error".to_string())));

        let mut mock_search_service_2 = MockSearchService::new();
        let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
        mock_search_service_2
            .expect_leaf_search_stream()
            .return_once(|_| Ok(UnboundedReceiverStream::new(result_receiver)));

        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool.clone());
        let cluster_client = ClusterClient::new(search_job_placer);

        result_sender
            .send(Ok(LeafSearchStreamResponse {
                data: Vec::new(),
                split_id: "split_1".to_string(),
            }))
            .unwrap();
        result_sender
            .send(Err(SearchError::Internal("last split error".to_string())))
            .unwrap();
        drop(result_sender);

        let first_client_addr: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let first_client = searcher_pool.get(&first_client_addr).unwrap();
        let result = cluster_client
            .leaf_search_stream(request, first_client)
            .await;
        let results: Vec<_> = result.collect().await;
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
    }

    #[tokio::test]
    async fn test_put_kv_happy_path() {
        // 3 servers 1, 2, 3
        // Targetted key has affinity [1, 2, 3].
        //
        // Put on 1 and 2 is successful
        // Get succeeds on 1.
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1
            .expect_put_kv()
            .once()
            .returning(|_put_req: quickwit_proto::search::PutKvRequest| {});
        mock_search_service_1.expect_get_kv().once().returning(
            |_get_req: quickwit_proto::search::GetKvRequest| Some(b"my_payload".to_vec()),
        );
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2
            .expect_put_kv()
            .once()
            .returning(|_put_req: quickwit_proto::search::PutKvRequest| {});
        let mut mock_search_service_3 = MockSearchService::new();
        // Due to the buffered call it is possible for the
        // put request to 3 to be emitted too.
        mock_search_service_3
            .expect_put_kv()
            .returning(|_put_req: quickwit_proto::search::PutKvRequest| {});
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
            ("127.0.0.1:1003", mock_search_service_3),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer);
        cluster_client
            .put_kv(
                &b"my_key"[..],
                &b"my_payload"[..],
                Duration::from_secs(10 * 60),
            )
            .await;
        let result = cluster_client.get_kv(&b"my_key"[..]).await;
        assert_eq!(result, Some(b"my_payload".to_vec()))
    }

    #[tokio::test]
    async fn test_put_kv_failing_get() {
        // 3 servers 1, 2, 3
        // Targetted key has affinity [1, 2, 3].
        //
        // Put on 1 and 2 is successful
        // Get fails on 1.
        // Get succeeds on 2.
        let mut mock_search_service_1 = MockSearchService::new();
        mock_search_service_1
            .expect_put_kv()
            .once()
            .returning(|_put_req: quickwit_proto::search::PutKvRequest| {});
        mock_search_service_1
            .expect_get_kv()
            .once()
            .returning(|_get_req: quickwit_proto::search::GetKvRequest| None);
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2
            .expect_put_kv()
            .once()
            .returning(|_put_req: quickwit_proto::search::PutKvRequest| {});
        mock_search_service_2.expect_get_kv().once().returning(
            |_get_req: quickwit_proto::search::GetKvRequest| Some(b"my_payload".to_vec()),
        );
        let mut mock_search_service_3 = MockSearchService::new();
        mock_search_service_3
            .expect_put_kv()
            .returning(|_leaf_search_req: quickwit_proto::search::PutKvRequest| {});
        let searcher_pool = searcher_pool_for_test([
            ("127.0.0.1:1001", mock_search_service_1),
            ("127.0.0.1:1002", mock_search_service_2),
            ("127.0.0.1:1003", mock_search_service_3),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer);
        cluster_client
            .put_kv(
                &b"my_key"[..],
                &b"my_payload"[..],
                Duration::from_secs(10 * 60),
            )
            .await;
        let result = cluster_client.get_kv(&b"my_key"[..]).await;
        assert_eq!(result, Some(b"my_payload".to_vec()))
    }
}
