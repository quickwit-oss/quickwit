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

pub mod search;
pub mod search_stream;

use std::collections::HashSet;
use std::net::SocketAddr;

use crate::search_job_placer::Job;
use crate::{SearchJobPlacer, SearchServiceClient};

/// A retry policy to evaluate if a request should be retried.
/// A retry can be made either on an error or on a partial success.
pub trait RetryPolicy<Request, Response, Error>: Sized {
    /// Returns a retry request in case of retry.
    fn retry_request(
        &self,
        request: Request,
        response_res: &Result<Response, Error>,
    ) -> Option<Request>;
}

/// Default retry policy:
/// - All responses are treated as success.
/// - All errors are retryable and the retry request is the same as the original one.
pub struct DefaultRetryPolicy {}

impl<Request, Response, Error> RetryPolicy<Request, Response, Error> for DefaultRetryPolicy {
    fn retry_request(
        &self,
        request: Request,
        response_res: &Result<Response, Error>,
    ) -> Option<Request> {
        match response_res {
            Ok(_) => None,
            Err(_) => Some(request),
        }
    }
}

impl<'a> Job for &'a str {
    fn split_id(&self) -> &str {
        self
    }

    fn cost(&self) -> usize {
        1
    }
}

// Select a new client from the client pool by the following oversimplified policy:
// 1. Take the first split_id of the request
// 2. Ask for a relevant client for that split while excluding the failing identified by its socket
// addr.
pub async fn retry_client(
    search_job_placer: &SearchJobPlacer,
    excluded_addr: SocketAddr,
    split_id: &str,
) -> anyhow::Result<SearchServiceClient> {
    let excluded_addrs = HashSet::from_iter([excluded_addr]);
    search_job_placer
        .assign_job(split_id, &excluded_addrs)
        .await
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use quickwit_proto::{FetchDocsResponse, SplitIdAndFooterOffsets};

    use crate::retry::{retry_client, DefaultRetryPolicy, RetryPolicy};
    use crate::{
        MockSearchService, SearchError, SearchJobPlacer, SearchServiceClient, SearcherPool,
    };

    #[test]
    fn test_should_retry_on_error() {
        let retry_policy = DefaultRetryPolicy {};
        let response_res = crate::Result::<()>::Err(SearchError::InternalError("test".to_string()));
        retry_policy.retry_request((), &response_res).unwrap()
    }

    #[test]
    fn test_should_not_retry_if_result_is_ok() {
        let retry_policy = DefaultRetryPolicy {};
        let response_res =
            crate::Result::<FetchDocsResponse>::Ok(FetchDocsResponse { hits: Vec::new() });
        assert!(retry_policy.retry_request((), &response_res).is_none());
    }

    #[tokio::test]
    async fn test_retry_client_should_return_another_client() -> anyhow::Result<()> {
        let searcher_grpc_addr_1 = ([127, 0, 0, 1], 1000).into();
        let mock_search_service_1 = MockSearchService::new();
        let searcher_client_1 = SearchServiceClient::from_service(
            Arc::new(mock_search_service_1),
            searcher_grpc_addr_1,
        );
        let searcher_grpc_addr_2 = ([127, 0, 0, 1], 1001).into();
        let mock_search_service_2 = MockSearchService::new();
        let searcher_client_2 = SearchServiceClient::from_service(
            Arc::new(mock_search_service_2),
            searcher_grpc_addr_2,
        );
        let searcher_pool = SearcherPool::from_iter([
            (searcher_grpc_addr_1, searcher_client_1),
            (searcher_grpc_addr_2, searcher_client_2),
        ]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let _first_grpc_addr: SocketAddr = "127.0.0.1:1000".parse()?;
        let split_id_and_footer_offsets = SplitIdAndFooterOffsets {
            split_id: "split_1".to_string(),
            split_footer_end: 100,
            split_footer_start: 0,
            timestamp_start: None,
            timestamp_end: None,
        };
        let client_for_retry = retry_client(
            &search_job_placer,
            searcher_grpc_addr_1,
            &split_id_and_footer_offsets.split_id,
        )
        .await
        .unwrap();
        assert_eq!(client_for_retry.grpc_addr().to_string(), "127.0.0.1:1001");
        Ok(())
    }
}
