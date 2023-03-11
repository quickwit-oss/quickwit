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

use byte_unit::Byte;

pub trait Rate: Clone {
    /// Returns the amount of work per time period.
    fn work(&self) -> u64;

    /// Returns the duration of a time period.
    fn period(&self) -> Duration;
}

/// A rate of unit of work per time period.
#[derive(Debug, Copy, Clone)]
pub struct ConstantRate {
    work: u64,
    period: Duration,
}

impl ConstantRate {
    /// Creates a new constant rate.
    ///
    /// # Panics
    ///
    /// This function panics if `work` is equal to zero or `period` is < 1ms.
    pub fn new(work: u64, period: Duration) -> Self {
        assert!(work > 0);
        assert!(period.as_millis() > 0);

        Self { work, period }
    }

    pub fn from_bytes(bytes: Byte, period: Duration) -> Self {
        let work = bytes.get_bytes();
        Self::new(work, period)
    }
}

impl Rate for ConstantRate {
    fn work(&self) -> u64 {
        self.work
    }

    fn period(&self) -> Duration {
        self.period
    }
}
