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
use crate::{ClientPool, RetryPolicy, SearchError, SearchServiceClient};

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

    // Build a retry request with only failing split ids.
    async fn retry_request(
        &self,
        request: &FetchDocsRequest,
        _result: Result<&FetchDocsResult, &SearchError>,
    ) -> anyhow::Result<FetchDocsRequest> {
        Ok(request.clone())
    }

    // Select a client from client pool.
    // Oversimplified policy to select another client. Just use the first split to
    // get a node that is relevant at least for this split.
    async fn retry_client(
        &self,
        client_pool: &Arc<dyn ClientPool>,
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
    client_pool: Arc<dyn ClientPool>,
}

impl FetchDocsClusterClient {
    pub fn new(client_pool: Arc<dyn ClientPool>) -> Self {
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
