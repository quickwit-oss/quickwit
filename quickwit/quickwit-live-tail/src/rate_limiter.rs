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
///
/// A rate limiter can be shared by several sessions, which check the available documents, filter,
/// and then consume the documents they send. Sessions doing so concurrently can consume more
/// documents than available: the bucket then goes into debt, which the following refills pay back,
/// so that the average rate stays bounded by `max_docs_per_sec`.
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

    /// Refills the bucket and returns the number of documents that can be sent.
    pub fn num_available_docs(&mut self, now: Instant) -> usize {
        let elapsed_secs = now.duration_since(self.last_refill_at).as_secs_f64();
        self.num_available_docs = (self.num_available_docs + elapsed_secs * self.max_docs_per_sec)
            .min(self.max_docs_per_sec);
        self.last_refill_at = now;
        self.num_available_docs.max(0.0).floor() as usize
    }

    /// Consumes `num_docs` documents. This can put the bucket into debt.
    pub fn consume(&mut self, num_docs: usize) {
        self.num_available_docs -= num_docs as f64;
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

        assert_eq!(rate_limiter.num_available_docs(now), 100);
        rate_limiter.consume(60);
        assert_eq!(rate_limiter.num_available_docs(now), 40);
        rate_limiter.consume(40);
        assert_eq!(rate_limiter.num_available_docs(now), 0);

        let now = now + Duration::from_millis(250);
        assert_eq!(rate_limiter.num_available_docs(now), 25);

        // The bucket never holds more than one second worth of documents.
        let now = now + Duration::from_secs(10);
        assert_eq!(rate_limiter.num_available_docs(now), 100);

        // Two sessions sharing the rate limiter both consumed the available documents: the debt is
        // paid back before new documents are available.
        rate_limiter.consume(100);
        rate_limiter.consume(100);
        assert_eq!(rate_limiter.num_available_docs(now), 0);
        let now = now + Duration::from_millis(500);
        assert_eq!(rate_limiter.num_available_docs(now), 0);
        let now = now + Duration::from_millis(750);
        assert_eq!(rate_limiter.num_available_docs(now), 25);
    }
}
