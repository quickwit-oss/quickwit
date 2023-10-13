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

use futures::{Future, TryFutureExt};
use quickwit_common::retry::{
    retry_with_mockable_sleep, Retry, RetryParams, Retryable, TokioSleep,
};

pub trait AwsRetryable {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl<E> AwsRetryable for Retry<E> {
    fn is_retryable(&self) -> bool {
        match self {
            Retry::Transient(_) => true,
            Retry::Permanent(_) => false,
        }
    }
}

#[derive(Debug)]
struct AwsRetryableWrapper<E>(E);

impl<E> Retryable for AwsRetryableWrapper<E>
where E: AwsRetryable
{
    fn is_retryable(&self) -> bool {
        self.0.is_retryable()
    }
}

pub async fn aws_retry<U, E, Fut>(retry_params: &RetryParams, f: impl Fn() -> Fut) -> Result<U, E>
where
    Fut: Future<Output = Result<U, E>>,
    E: AwsRetryable + Debug + 'static,
{
    retry_with_mockable_sleep(
        retry_params,
        || f().map_err(AwsRetryableWrapper),
        TokioSleep,
    )
    .await
    .map_err(|error| error.0)
}
