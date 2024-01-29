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
//

use std::time::{Duration, Instant};

/// `RateLimiter` will allow `max_per_minute` calls per minute.
///
/// It does not keep a sliding window, so it is not very precise, but very cheap.
pub struct LogRateLimiter {
    count: usize,
    start_time: Instant,
    max_per_minute: usize,
}

impl LogRateLimiter {
    pub fn new(max_per_minute: usize) -> Self {
        LogRateLimiter {
            count: 0,
            start_time: Instant::now(),
            max_per_minute,
        }
    }

    pub fn should_log(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.start_time);

        if elapsed > Duration::from_secs(60) {
            self.count = 0;
            self.start_time = now;
            true
        } else {
            self.count += 1;
            self.count < self.max_per_minute
        }
    }
}
