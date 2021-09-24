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
pub mod search_stream;

use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;
use quickwit_proto::{FetchDocsRequest, LeafSearchRequest, LeafSearchStreamRequest};

use crate::client_pool::Job;
use crate::{ClientPool, SearchClientPool, SearchServiceClient};

/// A retry policy to evaluate if a request should be retried.
/// A retry can be made either on an error or on a partial success.
pub trait RetryPolicy<Request, Response, Error>: Sized
where Request: RequestOnSplit + Clone
{
    /// Returns a retry request in case of retry.
    fn retry_request(&self, req: &Request, _result: Result<&Response, &Error>) -> Option<Request>;
}

/// Default retry policy:
/// - All responses are treated as success.
/// - All errors are retryable and the retry request is the same as the original one.
pub struct DefaultRetryPolicy {}

impl<Request, Response, Error> RetryPolicy<Request, Response, Error> for DefaultRetryPolicy
where Request: RequestOnSplit + Clone
{
    fn retry_request(&self, req: &Request, result: Result<&Response, &Error>) -> Option<Request> {
        match result {
            Ok(_) => None,
            Err(_) => Some(req.clone()),
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

/// Split based request. This includes:
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

    use quickwit_proto::{FetchDocsRequest, FetchDocsResponse, SplitIdAndFooterOffsets};

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

    #[test]
    fn test_should_retry_on_error() -> anyhow::Result<()> {
        let retry_policy = DefaultRetryPolicy {};
        let request = mock_doc_request();
        let result = crate::Result::<()>::Err(SearchError::InternalError("test".to_string()));
        let retry = retry_policy
            .retry_request(&request, result.as_ref())
            .is_some();
        assert!(retry);
        Ok(())
    }

    #[test]
    fn test_should_not_retry_if_result_is_ok() -> anyhow::Result<()> {
        let retry_policy = DefaultRetryPolicy {};
        let request = mock_doc_request();
        let result = crate::Result::<FetchDocsResponse>::Ok(FetchDocsResponse { hits: vec![] });
        let retry = retry_policy
            .retry_request(&request, result.as_ref())
            .is_some();
        assert_eq!(retry, false);
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
