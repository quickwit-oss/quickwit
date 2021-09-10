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

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_proto::{FetchDocsRequest, FetchDocsResult};

use crate::client_pool::Job;
use crate::{ClientPool, RetryPolicy, SearchClientPool, SearchError, SearchServiceClient};

// Oversimplified retry policy for `FetchDocsRequest`.
// If a `SearchServiceClient` returns an error, a retry is executed with the same request
// on another `SearchServiceClient` given by the client pool.
// By default, only one retry is made.
pub struct FetchDocshRetryPolicy {
    attempts: usize,
}

impl FetchDocshRetryPolicy {
    pub fn new(attempts: usize) -> Self {
        Self { attempts }
    }
}

impl Default for FetchDocshRetryPolicy {
    fn default() -> Self {
        Self { attempts: 1 }
    }
}

#[async_trait]
impl RetryPolicy<FetchDocsRequest, FetchDocsResult, SearchError> for FetchDocshRetryPolicy {
    async fn retry(
        &self,
        _request: &FetchDocsRequest,
        result: Result<&FetchDocsResult, &SearchError>,
    ) -> Option<Self> {
        match result {
            Ok(_) => None,
            Err(_) => {
                if self.attempts > 0 {
                    Some(Self::new(self.attempts - 1))
                } else {
                    None
                }
            }
        }
    }

    // Build a retry request which is the same as the orinal one.
    async fn retry_request(
        &self,
        request: &FetchDocsRequest,
        _result: Result<&FetchDocsResult, &SearchError>,
    ) -> anyhow::Result<FetchDocsRequest> {
        Ok(request.clone())
    }

    // Select a client from the client pool with an oversimplified policy:
    // - get only the first split_id
    // - and ask for a relevant client for that split
    // - and excluding the original client for which there was an error.
    async fn retry_client(
        &self,
        client_pool: &Arc<SearchClientPool>,
        client: &SearchServiceClient,
        _result: Result<&FetchDocsResult, &SearchError>,
        retry_request: &FetchDocsRequest,
    ) -> anyhow::Result<SearchServiceClient> {
        let mut exclude_addresses = HashSet::new();
        exclude_addresses.insert(client.grpc_addr());
        let job = Job {
            split_id: retry_request.split_metadata[0].split_id.clone(),
            cost: 0,
        };
        client_pool.assign_job(job, &exclude_addresses).await
    }
}

pub struct FetchDocsClusterClient {
    client_pool: Arc<SearchClientPool>,
}

/// Client that executes fetch docs requests on any cluster nodes
/// with a retry policy.
impl FetchDocsClusterClient {
    pub fn new(client_pool: Arc<SearchClientPool>) -> Self {
        Self { client_pool }
    }

    pub async fn execute(
        &self,
        placed_request: (FetchDocsRequest, SearchServiceClient),
    ) -> Result<FetchDocsResult, SearchError> {
        let (request, mut client) = placed_request;
        let mut result = client.fetch_docs(request.clone()).await;
        // TODO: use a retry policy factory.
        let mut retry_policy = FetchDocshRetryPolicy::default();
        while let Some(updated_retry_policy) = retry_policy.retry(&request, result.as_ref()).await {
            let retry_request = retry_policy
                .retry_request(&request, result.as_ref())
                .await?;
            client = retry_policy
                .retry_client(&self.client_pool, &client, result.as_ref(), &retry_request)
                .await?;
            result = client.fetch_docs(retry_request).await;
            retry_policy = updated_retry_policy;
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use quickwit_proto::{FetchDocsRequest, FetchDocsResult, SplitIdAndFooterOffsets};

    use super::FetchDocshRetryPolicy;
    use crate::client_pool::Job;
    use crate::cluster_client::fetch_docs::FetchDocsClusterClient;
    use crate::{ClientPool, MockSearchService, RetryPolicy, SearchClientPool, SearchError};

    fn mock_doc_request() -> FetchDocsRequest {
        FetchDocsRequest {
            partial_hits: vec![],
            index_id: "id".to_string(),
            index_uri: "uri".to_string(),
            split_metadata: vec![SplitIdAndFooterOffsets {
                split_id: "split_1".to_string(),
                split_footer_end: 100,
                split_footer_start: 0,
            }],
        }
    }

    #[tokio::test]
    async fn test_should_retry_and_attempts_should_decrease() -> anyhow::Result<()> {
        let retry_policy = FetchDocshRetryPolicy::new(1);
        let request = mock_doc_request();
        let result = Err(SearchError::InternalError("test".to_string()));
        let retry = retry_policy.retry(&request, result.as_ref()).await;
        assert!(retry.is_some());
        assert_eq!(retry.unwrap().attempts, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_should_not_retry_if_attempts_is_0() -> anyhow::Result<()> {
        let retry_policy = FetchDocshRetryPolicy::new(0);
        let request = mock_doc_request();
        let result = Err(SearchError::InternalError("test".to_string()));
        let retry = retry_policy.retry(&request, result.as_ref()).await;
        assert!(retry.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_should_not_retry_if_result_is_ok() -> anyhow::Result<()> {
        let retry_policy = FetchDocshRetryPolicy::new(1);
        let request = mock_doc_request();
        let result = Ok(FetchDocsResult { hits: vec![] });
        let retry = retry_policy.retry(&request, result.as_ref()).await;
        assert!(retry.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_should_retry_client_be_different_from_client_used_by_first_query(
    ) -> anyhow::Result<()> {
        let retry_policy = FetchDocshRetryPolicy::new(1);
        let request = mock_doc_request();
        let result = Ok(FetchDocsResult { hits: vec![] });
        let mock_service_1 = MockSearchService::new(); // 127.0.0.1:10000
        let mock_service_2 = MockSearchService::new(); // 127.0.0.1:10010
        let client_pool = Arc::new(
            SearchClientPool::from_mocks(vec![Arc::new(mock_service_1), Arc::new(mock_service_2)])
                .await?,
        );
        let first_client = client_pool
            .assign_job(
                Job {
                    split_id: "split_1".to_string(),
                    cost: 0,
                },
                &HashSet::new(),
            )
            .await?;
        let retry_client = retry_policy
            .retry_client(&client_pool, &first_client, result.as_ref(), &request)
            .await?;
        assert_ne!(retry_client.grpc_addr(), first_client.grpc_addr());
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_client_no_retry() -> anyhow::Result<()> {
        let request = mock_doc_request();
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
        let cluster_client = FetchDocsClusterClient::new(client_pool);
        let result = cluster_client.execute((request, first_client)).await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_client_retry_with_final_success() -> anyhow::Result<()> {
        let request = mock_doc_request();
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
        let cluster_client = FetchDocsClusterClient::new(client_pool.clone());
        let result = cluster_client
            .execute((request, first_client.clone()))
            .await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_client_retry_with_final_error() -> anyhow::Result<()> {
        let request = mock_doc_request();
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
        let cluster_client = FetchDocsClusterClient::new(client_pool.clone());
        let result = cluster_client
            .execute((request, first_client.clone()))
            .await;
        assert!(result.is_err());
        Ok(())
    }
}
