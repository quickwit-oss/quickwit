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

use std::{fmt::Display, time::Duration};

use futures::Future;
use quickwit_storage::retry::IsRetryable;
use rand::Rng;
use tracing::{debug, warn};

const MAX_RETRY_ATTEMPTS: usize = 3;
const BASE_DELAY: Duration = Duration::from_millis(if cfg!(test) { 1 } else { 250 });
const MAX_DELAY: Duration = Duration::from_millis(if cfg!(test) { 1 } else { 20_000 });

pub struct RetryPolicyParams {
    base_delay: Duration,
    max_delay: Duration,
    max_attempts: usize,
}

impl Default for RetryPolicyParams {
    fn default() -> Self {
        Self {
            base_delay: BASE_DELAY,
            max_delay: MAX_DELAY,
            max_attempts: MAX_RETRY_ATTEMPTS,
        }
    }
}

pub async fn retry<F, U, E, Fut>(f: F, policy: &RetryPolicyParams) -> Result<U, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<U, E>>,
    E: IsRetryable + Display + 'static,
{
    let mut attempt_count = 0;

    loop {
        let response = f().await;

        attempt_count += 1;

        match response {
            Ok(response) => {
                return Ok(response);
            }
            Err(error) => {
                if !error.is_retryable() {
                    return Err(error);
                }
                if attempt_count >= policy.max_attempts {
                    warn!(
                        attempt_count = %attempt_count,
                        "Request failed"
                    );
                    return Err(error);
                }

                let ceiling_ms = (policy.base_delay.as_millis() as u64
                    * 2u64.pow(attempt_count as u32))
                .min(policy.max_delay.as_millis() as u64);
                let delay_ms = rand::thread_rng().gen_range(0..ceiling_ms);
                debug!(
                    attempt_count = %attempt_count,
                    delay_ms = %delay_ms,
                    error = %error,
                    "Request failed, retrying"
                );

                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }
}
