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

use std::time::Duration;

use rand::Rng;

const DEFAULT_MAX_ATTEMPTS: usize = 30;
const DEFAULT_BASE_DELAY: Duration = Duration::from_millis(250);
const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(20);

pub trait Retryable {
    fn is_retryable(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RetryParams {
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub max_attempts: usize,
}

impl Default for RetryParams {
    fn default() -> Self {
        Self {
            base_delay: DEFAULT_BASE_DELAY,
            max_delay: DEFAULT_MAX_DELAY,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }
}

impl RetryParams {
    /// Computes the delay after which a new attempt should be performed. The randomized delay
    /// increases after each attempt (exponential backoff and full jitter). Implementation and
    /// default values originate from the Java SDK. See also: <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>.
    pub fn compute_delay(&self, num_retries: usize) -> Duration {
        let delay_ms = self.base_delay.as_millis() as u64 * 2u64.pow(num_retries as u32);
        let ceil_delay_ms = delay_ms.min(self.max_delay.as_millis() as u64);
        let half_delay_ms = ceil_delay_ms / 2;
        let jitter_range = 0..half_delay_ms + 1;
        let jittered_delay_ms = half_delay_ms + rand::thread_rng().gen_range(jitter_range);
        Duration::from_millis(jittered_delay_ms)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        Self {
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
            ..Default::default()
        }
    }
}
