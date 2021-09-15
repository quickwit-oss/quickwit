// Copyright (C) 2021 Quickwit, Inc.
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

const DEFAULT_COMMIT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_NUM_DOCS_COMMIT_THRESHOLD: u64 = 10_000_000;

use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct CommitPolicy {
    pub timeout: Duration,
    pub num_docs_threshold: u64,
}

impl Default for CommitPolicy {
    fn default() -> Self {
        CommitPolicy {
            timeout: DEFAULT_COMMIT_TIMEOUT,
            num_docs_threshold: DEFAULT_NUM_DOCS_COMMIT_THRESHOLD,
        }
    }
}
