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

use tantivy::directory::OwnedBytes;
use tokio::time::Instant;

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
