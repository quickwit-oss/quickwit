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

use std::fmt;

use bytes::Bytes;
use quickwit_metastore::checkpoint::SourceCheckpointDelta;

#[derive(Default)]
pub struct RawDocBatch {
    pub docs: Vec<Bytes>,
    pub checkpoint_delta: SourceCheckpointDelta,
    pub force_commit: bool,
}

impl RawDocBatch {
    pub fn new(
        docs: Vec<Bytes>,
        checkpoint_delta: SourceCheckpointDelta,
        force_commit: bool,
    ) -> Self {
        Self {
            docs,
            checkpoint_delta,
            force_commit,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            docs: Vec::with_capacity(capacity),
            checkpoint_delta: SourceCheckpointDelta::default(),
            force_commit: false,
        }
    }

    pub fn num_docs(&self) -> usize {
        self.docs.len()
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(docs: &[&str], range: std::ops::Range<u64>) -> Self {
        let docs = docs
            .iter()
            .map(|doc| Bytes::from(doc.to_string()))
            .collect();
        let checkpoint_delta = SourceCheckpointDelta::from_range(range);

        Self {
            docs,
            checkpoint_delta,
            force_commit: false,
        }
    }
}

impl fmt::Debug for RawDocBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("RawDocBatch")
            .field("num_docs", &self.num_docs())
            .field("checkpoint_delta", &self.checkpoint_delta)
            .field("force_commit", &self.force_commit)
            .finish()
    }
}
