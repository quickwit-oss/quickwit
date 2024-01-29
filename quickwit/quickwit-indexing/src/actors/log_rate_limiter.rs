// Copyright (C) 2024 Quickwit, Inc.
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

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

/// `RateLimiter` will allow `max_per_minute` calls per minute.
///
/// It does not keep a sliding window, so it is not very precise, but very cheap.
///
/// It allows the same log at most 3 times per minute.
pub struct LogRateLimiter {
    count: usize,
    start_time: Instant,
    max_per_minute: usize,
    // we store only the hash of the error message to avoid storing the whole string.
    errors_logged: HashMap<u64, usize>,
}
fn hash_string(input: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}
impl LogRateLimiter {
    pub fn new(max_per_minute: usize) -> Self {
        LogRateLimiter {
            count: 0,
            start_time: Instant::now(),
            max_per_minute,
            errors_logged: HashMap::new(),
        }
    }

    pub fn should_log(&mut self, error: &str) -> bool {
        let hash = hash_string(error);
        let now = Instant::now();
        let elapsed = now.duration_since(self.start_time);

        if elapsed > Duration::from_secs(60) {
            self.count = 0;
            self.start_time = now;
            self.errors_logged.clear();
            self.errors_logged.insert(hash, 1);
            true
        } else {
            self.count += 1;
            if self.count > self.max_per_minute {
                return false;
            }
            let error_count = self.errors_logged.entry(hash).or_insert(0);
            *error_count += 1;
            // if we have logged the same error more than 3 times, we stop logging it in this
            // minute.
            if *error_count > 3 {
                return false;
            }
            true
        }
    }
}
