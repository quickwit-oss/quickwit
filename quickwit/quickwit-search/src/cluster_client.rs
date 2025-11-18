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

use std::time::Duration;

use base64::Engine;
use futures::future::ready;
use futures::{Future, StreamExt};
use quickwit_proto::search::{
    FetchDocsRequest, FetchDocsResponse, GetKvRequest, LeafListFieldsRequest, LeafListTermsRequest,
    LeafListTermsResponse, LeafSearchRequest, LeafSearchResponse, ListFieldsResponse, PutKvRequest,
};
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tracing::{debug, error, info, warn};

use crate::retry::search::LeafSearchRetryPolicy;
use crate::retry::{DefaultRetryPolicy, RetryPolicy, retry_client};
use crate::{SearchJobPlacer, SearchServiceClient, merge_resource_stats_it};

/// Maximum number of put requests emitted to perform a replicated given PUT KV.
const MAX_PUT_KV_ATTEMPTS: usize = 6;

/// Maximum number of get requests emitted to perform a GET KV request.
const MAX_GET_KV_ATTEMPTS: usize = 6;

/// We attempt to store our KVs on two nodes.
const TARGET_NUM_REPLICATION: usize = 2;

/// Client that executes placed requests (Request, `SearchServiceClient`) and
/// provides retry policies for `FetchDocsRequest` and `LeafSearchRequest` to
/// retry on other `SearchServiceClient`.
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
        // We retry only once.
        let Some(retry_request) = retry_policy.retry_request(request, &response_res) else {
            return response_res;
        };
        let Some(first_split) = retry_request
            .leaf_requests
            .iter()
            .flat_map(|leaf_req| leaf_req.split_offsets.iter())
            .next()
        else {
            warn!(
                "the retry request did not contain any split to retry. this should never happen, \
                 please report"
            );
            return response_res;
        };
        // There could be more than one split in the retry request. We pick a single client
        // arbitrarily only considering the affinity of the first split.
        client = retry_client(
            &self.search_job_placer,
            client.grpc_addr(),
            &first_split.split_id,
        )
        .await?;
        debug!(
            "Leaf search response error: `{:?}`. Retry once to execute {:?} with {:?}",
            response_res, retry_request, client
        );
        let retry_result = client.leaf_search(retry_request).await;
        response_res = merge_original_with_retry_leaf_search_results(response_res, retry_result);
        response_res
    }

    /// Leaf search with retry on another node client.
    pub async fn leaf_list_fields(
        &self,
        request: LeafListFieldsRequest,
        mut client: SearchServiceClient,
    ) -> crate::Result<ListFieldsResponse> {
        client.leaf_list_fields(request.clone()).await
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

    /// Attempts to store a given key value pair within the cluster.
    ///
    /// Tries to replicate the pair to [`TARGET_NUM_REPLICATION`] nodes, but this function may fail
    /// silently (e.g if no client was available). Even in case of success, this storage is not
    /// persistent. For instance during a rolling upgrade, all replicas will be lost as there is no
    /// mechanism to maintain the replication count.
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
            warn!("no other node available to replicate scroll context");
            return;
        }

        // We run the put requests concurrently.
        // Our target is a replication over TARGET_NUM_REPLICATION nodes, we therefore try to avoid
        // replicating on more than TARGET_NUM_REPLICATION nodes at the same time. Of
        // course, this may still result in the replication over more nodes, but this is not
        // a problem.
        //
        // The requests are made in a concurrent manner, up to TARGET_NUM_REPLICATION at a time. As
        // soon as TARGET_NUM_REPLICATION requests are successful, we stop.
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

/// Merge two leaf search response.
///
/// # Quirk
///
/// This is implemented for a retries.
/// For instance, the set of attempted splits of right is supposed to be the set of failed
/// list of the left one, so that the list of the overal failed splits is the list of splits on the
/// `right_response`.
fn merge_original_with_retry_leaf_search_response(
    mut original_response: LeafSearchResponse,
    retry_response: LeafSearchResponse,
) -> crate::Result<LeafSearchResponse> {
    original_response
        .partial_hits
        .extend(retry_response.partial_hits);
    let intermediate_aggregation_result: Option<Vec<u8>> = match (
        original_response.intermediate_aggregation_result,
        retry_response.intermediate_aggregation_result,
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
    let resource_stats = merge_resource_stats_it([
        &original_response.resource_stats,
        &retry_response.resource_stats,
    ]);
    Ok(LeafSearchResponse {
        intermediate_aggregation_result,
        num_hits: original_response.num_hits + retry_response.num_hits,
        num_attempted_splits: original_response.num_attempted_splits
            + retry_response.num_attempted_splits,
        failed_splits: retry_response.failed_splits,
        partial_hits: original_response.partial_hits,
        num_successful_splits: original_response.num_successful_splits
            + retry_response.num_successful_splits,
        resource_stats,
    })
}

// Merge initial leaf search results with results obtained from a retry.
fn merge_original_with_retry_leaf_search_results(
    left_search_response_result: crate::Result<LeafSearchResponse>,
    right_search_response_result: crate::Result<LeafSearchResponse>,
) -> crate::Result<LeafSearchResponse> {
    match (left_search_response_result, right_search_response_result) {
        (Ok(left_response), Ok(right_response)) => {
            merge_original_with_retry_leaf_search_response(left_response, right_response)
        }
        (Ok(single_valid_response), Err(_)) => Ok(single_valid_response),
        (Err(_), Ok(single_valid_response)) => Ok(single_valid_response),
        (Err(error), Err(_)) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;

    use quickwit_proto::search::{
        LeafRequestRef, PartialHit, SearchRequest, SortValue, SplitIdAndFooterOffsets,
        SplitSearchError,
    };
    use quickwit_query::query_ast::qast_json_helper;

    use super::*;
    use crate::root::SearchJob;
    use crate::{MockSearchService, SearchError, searcher_pool_for_test};

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
                num_docs: 0,
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
            doc_mappers: vec!["doc_mapper".to_string()],
            index_uris: vec!["uri".to_string()],
            leaf_requests: vec![LeafRequestRef {
                index_uri_ord: 0,
                doc_mapper_ord: 0,
                split_offsets: vec![
                    SplitIdAndFooterOffsets {
                        split_id: "split_1".to_string(),
                        split_footer_start: 0,
                        split_footer_end: 100,
                        timestamp_start: None,
                        timestamp_end: None,
                        num_docs: 0,
                    },
                    SplitIdAndFooterOffsets {
                        split_id: "split_2".to_string(),
                        split_footer_start: 0,
                        split_footer_end: 100,
                        timestamp_start: None,
                        timestamp_end: None,
                        num_docs: 0,
                    },
                ],
            }],
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
            .withf(|request| request.leaf_requests[0].split_offsets[0].split_id == "split_1")
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
            .withf(|request| request.leaf_requests[0].split_offsets[0].split_id == "split_2")
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
        let merged_leaf_search_response = merge_original_with_retry_leaf_search_results(
            Ok(leaf_response),
            Ok(leaf_response_retry),
        )
        .unwrap();
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
        let merged_result = merge_original_with_retry_leaf_search_results(
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
        let merge_error = merge_original_with_retry_leaf_search_results(
            Err(SearchError::Internal("error".to_string())),
            Err(SearchError::Internal("retry error".to_string())),
        )
        .unwrap_err();
        assert_eq!(merge_error.to_string(), "internal error: `error`");
        Ok(())
    }

    #[tokio::test]
    async fn test_put_kv_happy_path() {
        // 3 servers 1, 2, 3
        // Targeted key has affinity [2, 3, 1].
        //
        // Put on 2 and 3 is successful
        // Get succeeds on 2.
        let mock_search_service_1 = MockSearchService::new();
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_put_kv().once().returning(
            |put_req: quickwit_proto::search::PutKvRequest| {
                assert_eq!(put_req.key, b"my_key");
                assert_eq!(put_req.payload, b"my_payload");
            },
        );
        mock_search_service_2.expect_get_kv().once().returning(
            |get_req: quickwit_proto::search::GetKvRequest| {
                assert_eq!(get_req.key, b"my_key");
                Some(b"my_payload".to_vec())
            },
        );
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
        // Targeted key has affinity [2, 3, 1].
        //
        // Put on 2 and 3 is successful
        // Get fails on 2.
        // Get succeeds on 3.
        let mock_search_service_1 = MockSearchService::new();
        let mut mock_search_service_2 = MockSearchService::new();
        mock_search_service_2.expect_put_kv().once().returning(
            |put_req: quickwit_proto::search::PutKvRequest| {
                assert_eq!(put_req.key, b"my_key");
                assert_eq!(put_req.payload, b"my_payload");
            },
        );
        mock_search_service_2.expect_get_kv().once().returning(
            |get_req: quickwit_proto::search::GetKvRequest| {
                assert_eq!(get_req.key, b"my_key");
                None
            },
        );
        let mut mock_search_service_3 = MockSearchService::new();
        mock_search_service_3.expect_put_kv().once().returning(
            |put_req: quickwit_proto::search::PutKvRequest| {
                assert_eq!(put_req.key, b"my_key");
                assert_eq!(put_req.payload, b"my_payload");
            },
        );
        mock_search_service_3.expect_get_kv().once().returning(
            |get_req: quickwit_proto::search::GetKvRequest| {
                assert_eq!(get_req.key, b"my_key");
                Some(b"my_payload".to_vec())
            },
        );
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
