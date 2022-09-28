// Copyright (C) 2022 Quickwit, Inc.
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

#[cfg(not(feature = "tokio-time"))]
use instant::Instant;
#[cfg(feature = "tokio-time")]
use tokio::time::Instant;

use crate::OwnedBytes;

/// It is a bit overkill to put this in its own module, but I
/// wanted to ensure that no one would access payload without updating `last_access_time`.
pub(super) struct StoredItem {
    last_access_time: Instant,
    payload: OwnedBytes,
}

impl StoredItem {
    pub fn new(payload: OwnedBytes, now: Instant) -> Self {
        StoredItem {
            last_access_time: now,
            payload,
        }
    }
}

impl StoredItem {
    pub fn payload(&mut self) -> OwnedBytes {
        self.last_access_time = Instant::now();
        self.payload.clone()
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn last_access_time(&self) -> Instant {
        self.last_access_time
    }
}
