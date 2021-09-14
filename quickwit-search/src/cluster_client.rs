// Copyright (C) 2021 Quickwit, Inc.
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

use std::sync::Arc;

use futures::StreamExt;
use quickwit_proto::{
    FetchDocsRequest, FetchDocsResult, LeafSearchRequest, LeafSearchResult,
    LeafSearchStreamRequest, LeafSearchStreamResult,
};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::debug;

use crate::retry::search::LeafSearchRetryPolicy;
use crate::retry::search_stream::{LeafSearchStreamRetryPolicy, SuccessFullSplitIds};
use crate::retry::{retry_client, DefaultRetryPolicy, RetryPolicy};
use crate::{SearchClientPool, SearchError, SearchServiceClient};

/// Client that executes placed requests (Request, `SearchServiceClient`) and provides
/// retry policies for `FetchDocsRequest`, `LeafSearchRequest` and `LeafSearchStreamRequest`
/// to retry on other `SearchServiceClient`.
#[derive(Clone)]
pub struct ClusterClient {
    client_pool: Arc<SearchClientPool>,
}

impl ClusterClient {
    /// Instanciate ClusterClient.
    pub fn new(client_pool: Arc<SearchClientPool>) -> Self {
        Self { client_pool }
    }

    /// Fetch docs with retry on another node client.
    pub async fn fetch_docs(
        &self,
        placed_request: (FetchDocsRequest, SearchServiceClient),
    ) -> Result<FetchDocsResult, SearchError> {
        let (request, mut client) = placed_request;
        let mut result = client.fetch_docs(request.clone()).await;
        let retry_policy = DefaultRetryPolicy {};
        if retry_policy.should_retry(&request, result.as_ref()) {
            let retry_request = retry_policy.retry_request(&request, result.as_ref());
            client = retry_client(&self.client_pool, &client, &retry_request).await?;
            debug!(
                "Fetch docs response error: `{:?}`. Retry once to execute {:?} with {:?}",
                result, retry_request, client
            );
            result = client.fetch_docs(retry_request).await;
        }
        result
    }

    /// Leaf search with retry on another node client.
    pub async fn leaf_search(
        &self,
        placed_request: (LeafSearchRequest, SearchServiceClient),
    ) -> Result<LeafSearchResult, SearchError> {
        let (request, mut client) = placed_request;
        let mut result = client.leaf_search(request.clone()).await;
        let retry_policy = LeafSearchRetryPolicy {};
        if retry_policy.should_retry(&request, result.as_ref()) {
            let retry_request = retry_policy.retry_request(&request, result.as_ref());
            client = retry_client(&self.client_pool, &client, &retry_request).await?;
            debug!(
                "Leaf search response error: `{:?}`. Retry once to execute {:?} with {:?}",
                result, retry_request, client
            );
            let retry_result = client.leaf_search(retry_request).await;
            result = merge_leaf_search_result(result, retry_result);
        }

        result
    }

    /// Leaf search stream with retry on another node client.
    pub async fn leaf_search_stream(
        &self,
        placed_request: (LeafSearchStreamRequest, SearchServiceClient),
    ) -> UnboundedReceiverStream<Result<LeafSearchStreamResult, SearchError>> {
        let (request, mut client) = placed_request;
        let (result_sender, result_receiver) = unbounded_channel();
        let client_pool = self.client_pool.clone();
        let retry_policy = LeafSearchStreamRetryPolicy {};
        tokio::spawn(async move {
            let results_stream = client.leaf_search_stream(request.clone()).await;
            let forward_result =
                forward_leaf_search_stream(results_stream, result_sender.clone(), true).await;
            if retry_policy.should_retry(&request, forward_result.as_ref()) {
                let retry_request = retry_policy.retry_request(&request, forward_result.as_ref());
                let retry_client_opt = retry_client(&client_pool, &client, &retry_request).await;
                // Propagates the error if we cannot get a new client and stops the task.
                if let Err(error) = retry_client_opt {
                    let _ = result_sender.send(Err(SearchError::from(error)));
                    return;
                }
                let mut retry_client = retry_client_opt.unwrap();
                debug!(
                    "Leaf search stream response error. Retry once to execute {:?} with {:?}",
                    retry_request, client
                );
                let retry_results_stream = retry_client.leaf_search_stream(retry_request).await;
                // It is ok to ignore send errors, there is nothing else to do.
                let _ =
                    forward_leaf_search_stream(retry_results_stream, result_sender.clone(), false)
                        .await;
            }
        });

        UnboundedReceiverStream::new(result_receiver)
    }
}

// Merge initial leaf search results with results obtained from a retry.
fn merge_leaf_search_result(
    result: Result<LeafSearchResult, SearchError>,
    retry_result: Result<LeafSearchResult, SearchError>,
) -> Result<LeafSearchResult, SearchError> {
    match (result, retry_result) {
        (Ok(mut response), Ok(mut retry_response)) => {
            response
                .partial_hits
                .append(&mut retry_response.partial_hits);
            let merged_response = LeafSearchResult {
                num_hits: response.num_hits + retry_response.num_hits,
                num_attempted_splits: response.num_attempted_splits
                    + retry_response.num_attempted_splits,
                failed_splits: retry_response.failed_splits,
                partial_hits: response.partial_hits,
            };
            Ok(merged_response)
        }
        (Ok(response), Err(_)) => Ok(response),
        (Err(_), Ok(retry_response)) => Ok(retry_response),
        (Err(error), Err(_)) => Err(error),
    }
}

async fn forward_leaf_search_stream(
    mut stream: UnboundedReceiverStream<Result<LeafSearchStreamResult, SearchError>>,
    sender: UnboundedSender<Result<LeafSearchStreamResult, SearchError>>,
    stop_on_error: bool,
) -> Result<SuccessFullSplitIds, SendError<Result<LeafSearchStreamResult, SearchError>>> {
    let mut sucessful_split_ids: Vec<String> = Vec::new();
    while let Some(result) = stream.next().await {
        match result {
            Ok(response) => {
                sucessful_split_ids.push(response.split_id.clone());
                sender.send(Ok(response))?;
            }
            Err(error) => {
                if stop_on_error {
                    break;
                } else {
                    sender.send(Err(error))?;
                }
            }
        }
    }
    Ok(sucessful_split_ids)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use futures::StreamExt;
    use quickwit_proto::{
        FetchDocsRequest, LeafSearchRequest, LeafSearchResult, LeafSearchStreamRequest,
        LeafSearchStreamResult, PartialHit, SearchRequest, SearchStreamRequest,
        SplitIdAndFooterOffsets, SplitSearchError,
    };
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use crate::client_pool::Job;
    use crate::cluster_client::{merge_leaf_search_result, ClusterClient};
    use crate::{ClientPool, MockSearchService, SearchClientPool, SearchError};

    fn mock_partial_hit(split_id: &str, sorting_field_value: u64, doc_id: u32) -> PartialHit {
        PartialHit {
            sorting_field_value,
            split_id: split_id.to_string(),
            segment_ord: 1,
            doc_id,
        }
    }

    fn mock_doc_request(split_id: &str) -> FetchDocsRequest {
        FetchDocsRequest {
            partial_hits: vec![],
            index_id: "id".to_string(),
            index_uri: "uri".to_string(),
            split_metadata: vec![SplitIdAndFooterOffsets {
                split_id: split_id.to_string(),
                split_footer_end: 100,
                split_footer_start: 0,
            }],
        }
    }

    fn mock_leaf_search_request() -> LeafSearchRequest {
        let search_request = SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            tags: vec![],
        };
        LeafSearchRequest {
            search_request: Some(search_request),
            index_config: "config".to_string(),
            index_uri: "uri".to_string(),
            split_metadata: vec![
                SplitIdAndFooterOffsets {
                    split_id: "split_1".to_string(),
                    split_footer_start: 0,
                    split_footer_end: 100,
                },
                SplitIdAndFooterOffsets {
                    split_id: "split_2".to_string(),
                    split_footer_start: 0,
                    split_footer_end: 100,
                },
            ],
        }
    }

    fn mock_leaf_search_stream_request() -> LeafSearchStreamRequest {
        let search_request = SearchStreamRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            fast_field: "fast".to_string(),
            output_format: 0,
            tags: vec![],
        };
        LeafSearchStreamRequest {
            request: Some(search_request),
            index_config: "config".to_string(),
            index_uri: "uri".to_string(),
            split_metadata: vec![
                SplitIdAndFooterOffsets {
                    split_id: "split_1".to_string(),
                    split_footer_start: 0,
                    split_footer_end: 100,
                },
                SplitIdAndFooterOffsets {
                    split_id: "split_2".to_string(),
                    split_footer_start: 0,
                    split_footer_end: 100,
                },
            ],
        }
    }

    #[tokio::test]
    async fn test_cluster_client_fetch_docs_no_retry() -> anyhow::Result<()> {
        let request = mock_doc_request("split_1");
        let mut mock_service = MockSearchService::new();
        mock_service
            .expect_fetch_docs()
            .return_once(|_: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult { hits: vec![] })
            });
        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_service)]).await?);
        let first_client = client_pool
            .assign_job(
                Job {
                    split_id: "split_1".to_string(),
                    cost: 0,
                },
                &HashSet::new(),
            )
            .await?;
        let cluster_client = ClusterClient::new(client_pool);
        let result = cluster_client.fetch_docs((request, first_client)).await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_client_fetch_docs_retry_with_final_success() -> anyhow::Result<()> {
        let request = mock_doc_request("split_1");
        let mut mock_service_1 = MockSearchService::new();
        mock_service_1
            .expect_fetch_docs()
            .return_once(|_: quickwit_proto::FetchDocsRequest| {
                Err(SearchError::InternalError("error".to_string()))
            });
        let mut mock_service_2 = MockSearchService::new();
        mock_service_2
            .expect_fetch_docs()
            .return_once(|_: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult { hits: vec![] })
            });
        let client_pool = Arc::new(
            SearchClientPool::from_mocks(vec![Arc::new(mock_service_1), Arc::new(mock_service_2)])
                .await?,
        );
        let client_hashmap = client_pool.clients.read().await;
        let first_grpc_addr: SocketAddr = "127.0.0.1:10000".parse()?;
        let first_client = client_hashmap.get(&first_grpc_addr).unwrap();
        let cluster_client = ClusterClient::new(client_pool.clone());
        let result = cluster_client
            .fetch_docs((request, first_client.clone()))
            .await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_client_fetch_docs_retry_with_final_error() -> anyhow::Result<()> {
        let request = mock_doc_request("split_1");
        let mut mock_service_1 = MockSearchService::new();
        mock_service_1
            .expect_fetch_docs()
            .returning(|_: quickwit_proto::FetchDocsRequest| {
                Err(SearchError::InternalError("error".to_string()))
            });
        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_service_1)]).await?);
        let client_hashmap = client_pool.clients.read().await;
        let first_grpc_addr: SocketAddr = "127.0.0.1:10000".parse()?;
        let first_client = client_hashmap.get(&first_grpc_addr).unwrap();
        let cluster_client = ClusterClient::new(client_pool.clone());
        let result = cluster_client
            .fetch_docs((request, first_client.clone()))
            .await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_client_leaf_search_no_retry() -> anyhow::Result<()> {
        let request = mock_leaf_search_request();
        let mut mock_service = MockSearchService::new();
        mock_service
            .expect_leaf_search()
            .return_once(|_: LeafSearchRequest| {
                Ok(LeafSearchResult {
                    num_hits: 0,
                    partial_hits: vec![],
                    failed_splits: vec![],
                    num_attempted_splits: 1,
                })
            });
        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_service)]).await?);
        let first_client = client_pool
            .assign_job(
                Job {
                    split_id: "split_1".to_string(),
                    cost: 0,
                },
                &HashSet::new(),
            )
            .await?;
        let cluster_client = ClusterClient::new(client_pool);
        let result = cluster_client.leaf_search((request, first_client)).await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_client_leaf_search_retry_on_failing_splits() -> anyhow::Result<()> {
        let request = mock_leaf_search_request();
        let mut mock_service = MockSearchService::new();
        mock_service
            .expect_leaf_search()
            .withf(|request| request.split_metadata[0].split_id == "split_1")
            .return_once(|_: LeafSearchRequest| {
                Ok(LeafSearchResult {
                    num_hits: 1,
                    partial_hits: vec![],
                    failed_splits: vec![SplitSearchError {
                        error: "mock_error".to_string(),
                        split_id: "split_2".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 1,
                })
            });
        mock_service
            .expect_leaf_search()
            .withf(|request| request.split_metadata[0].split_id == "split_2")
            .return_once(|_: LeafSearchRequest| {
                Ok(LeafSearchResult {
                    num_hits: 1,
                    partial_hits: vec![],
                    failed_splits: vec![SplitSearchError {
                        error: "mock_error".to_string(),
                        split_id: "split_3".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 1,
                })
            });
        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_service)]).await?);
        let first_client = client_pool
            .assign_job(
                Job {
                    split_id: "split_1".to_string(),
                    cost: 0,
                },
                &HashSet::new(),
            )
            .await?;
        let cluster_client = ClusterClient::new(client_pool);
        let result = cluster_client.leaf_search((request, first_client)).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().num_hits, 2);
        Ok(())
    }

    #[test]
    fn test_merge_leaf_search_retry_on_partial_success() -> anyhow::Result<()> {
        let split_error = SplitSearchError {
            error: "error".to_string(),
            split_id: "split_2".to_string(),
            retryable_error: true,
        };
        let leaf_response = LeafSearchResult {
            num_hits: 1,
            partial_hits: vec![mock_partial_hit("split_1", 3, 1)],
            failed_splits: vec![split_error],
            num_attempted_splits: 1,
        };
        let leaf_response_retry = LeafSearchResult {
            num_hits: 1,
            partial_hits: vec![mock_partial_hit("split_2", 3, 1)],
            failed_splits: vec![],
            num_attempted_splits: 1,
        };
        let merged_result =
            merge_leaf_search_result(Ok(leaf_response), Ok(leaf_response_retry)).unwrap();
        assert_eq!(merged_result.num_attempted_splits, 2);
        assert_eq!(merged_result.num_hits, 2);
        assert_eq!(merged_result.partial_hits.len(), 2);
        assert_eq!(merged_result.failed_splits.len(), 0);
        Ok(())
    }

    #[test]
    fn test_merge_leaf_search_retry_on_error() -> anyhow::Result<()> {
        let split_error = SplitSearchError {
            error: "error".to_string(),
            split_id: "split_2".to_string(),
            retryable_error: true,
        };
        let leaf_response = LeafSearchResult {
            num_hits: 1,
            partial_hits: vec![mock_partial_hit("split_1", 3, 1)],
            failed_splits: vec![split_error],
            num_attempted_splits: 1,
        };
        let merged_result = merge_leaf_search_result(
            Err(SearchError::InternalError("error".to_string())),
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
        let merge_error = merge_leaf_search_result(
            Err(SearchError::InternalError("error".to_string())),
            Err(SearchError::InternalError("retry error".to_string())),
        )
        .unwrap_err();
        assert_eq!(merge_error.to_string(), "Internal error: `error`.");
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_client_leaf_stream_retry_on_error() -> anyhow::Result<()> {
        let request = mock_leaf_search_stream_request();
        let mut mock_service_1 = MockSearchService::new();
        mock_service_1
            .expect_leaf_search_stream()
            .return_once(|_| Err(SearchError::InternalError("error".to_string())));
        let mut mock_service_2 = MockSearchService::new();
        let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
        mock_service_2
            .expect_leaf_search_stream()
            .return_once(|_| Ok(UnboundedReceiverStream::new(result_receiver)));
        let client_pool = Arc::new(
            SearchClientPool::from_mocks(vec![Arc::new(mock_service_1), Arc::new(mock_service_2)])
                .await?,
        );
        result_sender.send(Ok(LeafSearchStreamResult {
            data: Vec::new(),
            split_id: "split_1".to_string(),
        }))?;
        result_sender.send(Err(SearchError::InternalError(
            "last split error".to_string(),
        )))?;
        drop(result_sender);
        let client_hashmap = client_pool.clients.read().await;
        let first_grpc_addr: SocketAddr = "127.0.0.1:10000".parse()?;
        let first_client = client_hashmap.get(&first_grpc_addr).unwrap();
        let cluster_client = ClusterClient::new(client_pool.clone());
        let result = cluster_client
            .leaf_search_stream((request, first_client.clone()))
            .await;
        let results: Vec<_> = result.collect().await;
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        Ok(())
    }
}
