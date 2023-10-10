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

use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use futures::Future;
use quickwit_common::retry::{retry_with_mockable_time, MockableTime, RetryParams, Retryable};

struct TokioTime;

#[async_trait]
impl MockableTime for TokioTime {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Retry with exponential backoff and full jitter. Implementation and default values originate from
/// the Java SDK. See also: <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>.
pub async fn retry<U, E, Fut>(retry_params: &RetryParams, f: impl Fn() -> Fut) -> Result<U, E>
where
    Fut: Future<Output = Result<U, E>>,
    E: Retryable + Debug + 'static,
{
    retry_with_mockable_time(retry_params, f, TokioTime).await
}

#[derive(Debug, Eq, PartialEq)]
pub enum Retry<E> {
    Permanent(E),
    Transient(E),
}
