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

use std::fmt::Debug;

use futures::{Future, TryFutureExt};
use quickwit_common::retry::{
    Retry, RetryParams, Retryable, TokioSleep, retry_with_mockable_sleep,
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
