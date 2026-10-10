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

use tokio::time::Instant;

/// Token bucket limiting the number of documents sent per second. The bucket holds at most one
/// second worth of documents, so short bursts are allowed.
pub(crate) struct DocRateLimiter {
    max_docs_per_sec: f64,
    num_available_docs: f64,
    last_refill_at: Instant,
}

impl DocRateLimiter {
    pub fn new(max_docs_per_sec: u32, now: Instant) -> Self {
        assert!(max_docs_per_sec > 0, "max docs per sec must be positive");
        let max_docs_per_sec = max_docs_per_sec as f64;
        Self {
            max_docs_per_sec,
            num_available_docs: max_docs_per_sec,
            last_refill_at: now,
        }
    }

    /// Consumes up to `num_docs` documents and returns the number of documents allowed.
    pub fn acquire(&mut self, num_docs: usize, now: Instant) -> usize {
        let elapsed_secs = now.duration_since(self.last_refill_at).as_secs_f64();
        self.num_available_docs = (self.num_available_docs + elapsed_secs * self.max_docs_per_sec)
            .min(self.max_docs_per_sec);
        self.last_refill_at = now;

        let num_allowed_docs = num_docs.min(self.num_available_docs.floor() as usize);
        self.num_available_docs -= num_allowed_docs as f64;
        num_allowed_docs
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_doc_rate_limiter() {
        let now = Instant::now();
        let mut rate_limiter = DocRateLimiter::new(100, now);

        assert_eq!(rate_limiter.acquire(60, now), 60);
        assert_eq!(rate_limiter.acquire(60, now), 40);
        assert_eq!(rate_limiter.acquire(1, now), 0);

        let now = now + Duration::from_millis(250);
        assert_eq!(rate_limiter.acquire(60, now), 25);

        // The bucket never holds more than one second worth of documents.
        let now = now + Duration::from_secs(10);
        assert_eq!(rate_limiter.acquire(1_000, now), 100);
    }
}
