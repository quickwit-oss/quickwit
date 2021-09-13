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

pub mod search;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_proto::{FetchDocsRequest, LeafSearchRequest, LeafSearchStreamRequest};

use crate::client_pool::Job;
use crate::{ClientPool, SearchClientPool, SearchServiceClient};

/// A retry policy to evaluate if a request should be retried.
#[async_trait]
pub trait RetryPolicy<Request, Response, Error>: Sized + Sync + Send
where
    Request: RequestOnSplit + Clone + Send + Sync,
    Response: Send + Sync,
    Error: Send + Sync,
{
    /// Checks the policy if a certain request should be retried.
    /// If the request should not be retried, returns `None`.
    /// If the request should be retried, return the
    /// policy that would apply for the next request attempt.
    async fn retry(&self, req: &Request, result: Result<&Response, &Error>) -> Option<Self>;

    /// Returns the request to use for retry.
    /// By default, clone the original request on error.
    async fn retry_request(
        &self,
        req: &Request,
        result: Result<&Response, &Error>,
    ) -> anyhow::Result<Request> {
        match result {
            Ok(_) => {
                anyhow::bail!("Retry request must be called when there are some errors.")
            }
            Err(_) => Ok(req.clone()),
        }
    }
}

/// Default retry policy that limits the number of attempts.
/// All responses are treated as success.
/// All errors are treated as failures and will lead to a retry.
pub struct DefaultRetryPolicy {
    attempts: usize,
}

impl Default for DefaultRetryPolicy {
    fn default() -> Self {
        Self { attempts: 1 }
    }
}

#[async_trait]
impl<Request, Response, Error> RetryPolicy<Request, Response, Error> for DefaultRetryPolicy
where
    Request: RequestOnSplit + Clone + Send + Sync,
    Response: Send + Sync,
    Error: Send + Sync,
{
    async fn retry(&self, _req: &Request, result: Result<&Response, &Error>) -> Option<Self> {
        match result {
            Ok(_) => None,
            Err(_) => {
                if self.attempts > 0 {
                    Some(Self {
                        attempts: self.attempts - 1,
                    })
                } else {
                    None
                }
            }
        }
    }
}

// Select a new client from the client pool by the following oversimplified policy:
// 1. Take the first split_id of the request
// 2. Ask for a relevant client for that split while excluding the failing client.
pub async fn retry_client<Request>(
    client_pool: &Arc<SearchClientPool>,
    failing_client: &SearchServiceClient,
    retry_request: &Request,
) -> anyhow::Result<SearchServiceClient>
where
    Request: RequestOnSplit,
{
    let mut exclude_addresses = HashSet::new();
    exclude_addresses.insert(failing_client.grpc_addr());
    let split_ids = retry_request.split_ids();
    assert!(
        !split_ids.is_empty(),
        "A request must be at least on one split."
    );
    let job = Job {
        split_id: split_ids[0].clone(),
        cost: 0,
    };
    client_pool.assign_job(job, &exclude_addresses).await
}

/// Split based request.
/// All requests end to leaves are split based:
/// - FetchDocsRequest
/// - LeafSearchRequest
/// - LeafSearchStreamRequest
pub trait RequestOnSplit {
    fn split_ids(&self) -> Vec<String>;
}

impl RequestOnSplit for FetchDocsRequest {
    fn split_ids(&self) -> Vec<String> {
        self.split_metadata
            .iter()
            .map(|split| split.split_id.clone())
            .collect_vec()
    }
}

impl RequestOnSplit for LeafSearchRequest {
    fn split_ids(&self) -> Vec<String> {
        self.split_metadata
            .iter()
            .map(|split| split.split_id.clone())
            .collect_vec()
    }
}

impl RequestOnSplit for LeafSearchStreamRequest {
    fn split_ids(&self) -> Vec<String> {
        self.split_metadata
            .iter()
            .map(|split| split.split_id.clone())
            .collect_vec()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use quickwit_proto::{FetchDocsRequest, FetchDocsResult, SplitIdAndFooterOffsets};

    use crate::retry::{retry_client, DefaultRetryPolicy, RetryPolicy};
    use crate::{MockSearchService, SearchClientPool, SearchError};

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
        let retry_policy = DefaultRetryPolicy::default();
        let request = mock_doc_request();
        let result = Result::<(), SearchError>::Err(SearchError::InternalError("test".to_string()));
        let retry = retry_policy.retry(&request, result.as_ref()).await;
        assert!(retry.is_some());
        assert_eq!(retry.unwrap().attempts, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_should_not_retry_if_attempts_is_0() -> anyhow::Result<()> {
        let retry_policy = DefaultRetryPolicy { attempts: 0 };
        let request = mock_doc_request();
        let result = Result::<(), SearchError>::Err(SearchError::InternalError("test".to_string()));
        let retry = retry_policy.retry(&request, result.as_ref()).await;
        assert!(retry.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_should_not_retry_if_result_is_ok() -> anyhow::Result<()> {
        let retry_policy = DefaultRetryPolicy::default();
        let request = mock_doc_request();
        let result = Result::<FetchDocsResult, SearchError>::Ok(FetchDocsResult { hits: vec![] });
        let retry = retry_policy.retry(&request, result.as_ref()).await;
        assert!(retry.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_client_should_return_another_client() -> anyhow::Result<()> {
        let request = mock_doc_request();
        let mock_service_1 = MockSearchService::new();
        let mock_service_2 = MockSearchService::new();
        let client_pool = Arc::new(
            SearchClientPool::from_mocks(vec![Arc::new(mock_service_1), Arc::new(mock_service_2)])
                .await?,
        );
        let client_hashmap = client_pool.clients.read().await;
        let first_grpc_addr: SocketAddr = "127.0.0.1:10000".parse()?;
        let failing_client = client_hashmap.get(&first_grpc_addr).unwrap();

        let client_for_retry = retry_client(&client_pool, failing_client, &request)
            .await
            .unwrap();
        assert_eq!(client_for_retry.grpc_addr().to_string(), "127.0.0.1:10010");
        Ok(())
    }
}
