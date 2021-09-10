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

use async_trait::async_trait;

use crate::{SearchClientPool, SearchServiceClient};

/// A retry policy to evaluate if a request should be retried.
#[async_trait]
pub trait RetryPolicy<Request, Response, Error>: Sized {
    /// Checks the policy if a certain request should be retried.
    /// If the request should not be retried, returns `None`.
    /// If the request should be retried, return the
    /// policy that would apply for the next request attempt.
    async fn retry(&self, req: &Request, result: Result<&Response, &Error>) -> Option<Self>;

    /// Returns the request to use for retry.
    async fn retry_request(
        &self,
        req: &Request,
        result: Result<&Response, &Error>,
    ) -> anyhow::Result<Request>;

    /// Returns the `SearchServiceClient` to use for retry.
    async fn retry_client(
        &self,
        client_pool: &Arc<SearchClientPool>,
        client: &SearchServiceClient,
        result: Result<&Response, &Error>,
        retry_request: &Request,
    ) -> anyhow::Result<SearchServiceClient>;
}
