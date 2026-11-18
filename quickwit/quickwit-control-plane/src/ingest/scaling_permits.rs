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

use std::collections::HashMap;
use std::time::Duration;

use quickwit_common::rate_limiter::{RateLimiter, RateLimiterSettings};
use quickwit_common::tower::ConstantRate;
use quickwit_config::IngestApiConfig;
use quickwit_proto::types::SourceUid;

use super::scaling_arbiter::ScalingMode;

struct ScalingRateLimiters {
    scaling_up: RateLimiter,
    scaling_down: RateLimiter,
}

/// Rate limits the scale up and scale down operations of each source. Each direction allows a
/// burst of operations back to back, then one operation per interval.
pub(crate) struct ScalingPermits {
    scaling_up_rate_limiter_settings: RateLimiterSettings,
    scaling_down_rate_limiter_settings: RateLimiterSettings,
    scaling_rate_limiters: HashMap<SourceUid, ScalingRateLimiters>,
}

impl ScalingPermits {
    pub fn new(ingest_api_config: &IngestApiConfig) -> Self {
        let scaling_up_interval = Duration::from(ingest_api_config.shard_scale_up_interval.clone());
        let scaling_down_interval =
            Duration::from(ingest_api_config.shard_scale_down_interval.clone());
        Self {
            scaling_up_rate_limiter_settings: RateLimiterSettings {
                burst_limit: ingest_api_config.shard_scale_up_burst.get(),
                rate_limit: ConstantRate::new(1, scaling_up_interval),
                refill_period: scaling_up_interval,
            },
            scaling_down_rate_limiter_settings: RateLimiterSettings {
                burst_limit: ingest_api_config.shard_scale_down_burst.get(),
                rate_limit: ConstantRate::new(1, scaling_down_interval),
                refill_period: scaling_down_interval,
            },
            scaling_rate_limiters: HashMap::new(),
        }
    }

    pub fn acquire(&mut self, source_uid: &SourceUid, scaling_mode: ScalingMode) -> bool {
        self.rate_limiter(source_uid, scaling_mode).acquire(1)
    }

    pub fn release(&mut self, source_uid: &SourceUid, scaling_mode: ScalingMode) {
        self.rate_limiter(source_uid, scaling_mode).release(1);
    }

    /// Prevents the source from scaling down for one scale down interval.
    pub fn drain_scale_down_permits(&mut self, source_uid: &SourceUid) {
        self.rate_limiter(source_uid, ScalingMode::Down).drain();
    }

    fn rate_limiter(
        &mut self,
        source_uid: &SourceUid,
        scaling_mode: ScalingMode,
    ) -> &mut RateLimiter {
        let scaling_up_rate_limiter_settings = self.scaling_up_rate_limiter_settings;
        let scaling_down_rate_limiter_settings = self.scaling_down_rate_limiter_settings;
        let scaling_rate_limiters = self
            .scaling_rate_limiters
            .entry(source_uid.clone())
            .or_insert_with(|| ScalingRateLimiters {
                scaling_up: RateLimiter::from_settings(scaling_up_rate_limiter_settings),
                scaling_down: RateLimiter::from_settings(scaling_down_rate_limiter_settings),
            });
        match scaling_mode {
            ScalingMode::Up(_) => &mut scaling_rate_limiters.scaling_up,
            ScalingMode::Down => &mut scaling_rate_limiters.scaling_down,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use quickwit_common::tower::Rate;
    use quickwit_proto::types::IndexUid;

    use super::*;

    fn source_uid_for_test(index_id: &str, source_id: &str) -> SourceUid {
        SourceUid {
            index_uid: IndexUid::for_test(index_id, 0),
            source_id: source_id.to_string(),
        }
    }

    #[test]
    fn test_scaling_permits_default_settings() {
        let scaling_permits = ScalingPermits::new(&IngestApiConfig::default());

        // 5 scale up operations per minute, refilled one every 12 seconds.
        let scaling_up = scaling_permits.scaling_up_rate_limiter_settings;
        assert_eq!(scaling_up.burst_limit, 5);
        assert_eq!(scaling_up.refill_period, Duration::from_secs(12));
        assert_eq!(
            scaling_up.rate_limit.rescale(Duration::from_mins(1)).work(),
            5
        );

        // 1 scale down operation per minute.
        let scaling_down = scaling_permits.scaling_down_rate_limiter_settings;
        assert_eq!(scaling_down.burst_limit, 1);
        assert_eq!(scaling_down.refill_period, Duration::from_mins(1));
        assert_eq!(
            scaling_down
                .rate_limit
                .rescale(Duration::from_mins(1))
                .work(),
            1
        );
    }

    #[test]
    fn test_scaling_permits_acquire_scaling_up() {
        let mut scaling_permits = ScalingPermits::new(&IngestApiConfig::default());
        let source_uid = source_uid_for_test("test-index", "test-source");

        for _ in 0..5 {
            assert!(scaling_permits.acquire(&source_uid, ScalingMode::Up(1)));
        }
        assert!(!scaling_permits.acquire(&source_uid, ScalingMode::Up(1)));
    }

    #[test]
    fn test_scaling_permits_acquire_scaling_down() {
        let mut scaling_permits = ScalingPermits::new(&IngestApiConfig::default());
        let source_uid = source_uid_for_test("test-index", "test-source");

        assert!(scaling_permits.acquire(&source_uid, ScalingMode::Down));
        assert!(!scaling_permits.acquire(&source_uid, ScalingMode::Down));

        // Scaling up and scaling down have independent permits.
        assert!(scaling_permits.acquire(&source_uid, ScalingMode::Up(1)));
    }

    #[test]
    fn test_scaling_permits_configured_bursts() {
        let mut ingest_api_config = IngestApiConfig::default();
        ingest_api_config.shard_scale_up_burst = NonZeroU64::new(2).unwrap();
        ingest_api_config.shard_scale_down_burst = NonZeroU64::new(3).unwrap();
        let mut scaling_permits = ScalingPermits::new(&ingest_api_config);
        let source_uid = source_uid_for_test("test-index", "test-source");

        for _ in 0..2 {
            assert!(scaling_permits.acquire(&source_uid, ScalingMode::Up(1)));
        }
        assert!(!scaling_permits.acquire(&source_uid, ScalingMode::Up(1)));

        for _ in 0..3 {
            assert!(scaling_permits.acquire(&source_uid, ScalingMode::Down));
        }
        assert!(!scaling_permits.acquire(&source_uid, ScalingMode::Down));
    }

    #[test]
    fn test_scaling_permits_release() {
        let mut scaling_permits = ScalingPermits::new(&IngestApiConfig::default());
        let source_uid = source_uid_for_test("test-index", "test-source");

        assert!(scaling_permits.acquire(&source_uid, ScalingMode::Down));
        scaling_permits.release(&source_uid, ScalingMode::Down);
        assert!(scaling_permits.acquire(&source_uid, ScalingMode::Down));

        for _ in 0..5 {
            assert!(scaling_permits.acquire(&source_uid, ScalingMode::Up(1)));
        }
        scaling_permits.release(&source_uid, ScalingMode::Up(1));
        assert!(scaling_permits.acquire(&source_uid, ScalingMode::Up(1)));
    }

    #[test]
    fn test_scaling_permits_drain_scale_down_permits() {
        let mut scaling_permits = ScalingPermits::new(&IngestApiConfig::default());
        let source_uid = source_uid_for_test("test-index", "test-source");

        scaling_permits.drain_scale_down_permits(&source_uid);
        assert!(!scaling_permits.acquire(&source_uid, ScalingMode::Down));
        assert!(scaling_permits.acquire(&source_uid, ScalingMode::Up(1)));
    }
}
