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

use core::future::Future;
use std::time::Duration;

use rand::Rng;
use tracing::{debug, warn};

const MAX_RETRY_ATTEMPTS: usize = 3;
const BASE_DELAY: Duration = Duration::from_millis(250);
const MAX_DELAY: Duration = Duration::from_millis(20_000);

pub struct RetryRequest<F, Fut, U>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<U>>,
{
    request: F,
    max_attempts: Option<usize>,
    base_delay: Option<Duration>,
    max_delay: Option<Duration>,
}

impl<F, Fut, U> RetryRequest<F, Fut, U>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<U>>,
{
    pub fn new(request: F) -> Self {
        RetryRequest {
            request,
            max_attempts: None,
            base_delay: None,
            max_delay: None,
        }
    }

    /// Set the retry request's max attempts.
    pub fn max_attempts(&mut self, max_attempts: usize) {
        self.max_attempts = Some(max_attempts);
    }

    /// Set the retry request's base delay.
    pub fn base_delay(&mut self, base_delay: Duration) {
        self.base_delay = Some(base_delay);
    }

    /// Set the retry request's max delay.
    pub fn max_delay(&mut self, max_delay: Duration) {
        self.max_delay = Some(max_delay);
    }

    pub async fn execute(&self) -> anyhow::Result<U> {
        let max_attempts = self.max_attempts.unwrap_or(MAX_RETRY_ATTEMPTS);
        let base_delay = self.base_delay.unwrap_or(BASE_DELAY);
        let max_delay = self.max_delay.unwrap_or(MAX_DELAY);

        let mut attempt_count = 0;

        loop {
            let response = (self.request)().await;

            attempt_count += 1;

            match response {
                Ok(response) => {
                    return Ok(response);
                }
                Err(error) => {
                    if attempt_count >= max_attempts {
                        warn!(
                            attempt_count = %attempt_count,
                            "Request failed"
                        );
                        return Err(error);
                    }

                    let ceiling_ms = (base_delay.as_millis() as u64
                        * 2u64.pow(attempt_count as u32))
                    .min(max_delay.as_millis() as u64);
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
}
