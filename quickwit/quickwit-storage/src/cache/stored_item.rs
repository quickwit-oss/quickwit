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

use tantivy::HasLen;
use tokio::time::Instant;

/// It is a bit overkill to put this in its own module, but I
/// wanted to ensure that no one would access payload without updating `last_access_time`.
pub(super) struct StoredItem<T> {
    last_access_time: Instant,
    payload: T,
}

impl<T: Clone + HasLen> StoredItem<T> {
    pub fn new(payload: T, now: Instant) -> Self {
        StoredItem {
            last_access_time: now,
            payload,
        }
    }

    pub fn payload(&mut self) -> T {
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
