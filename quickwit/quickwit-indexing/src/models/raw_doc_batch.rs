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

use std::fmt;

use bytes::Bytes;
use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use quickwit_metastore::checkpoint::SourceCheckpointDelta;

pub struct RawDocBatch {
    // Do not directly append documents to this vector; otherwise, in-flight metrics will be
    // incorrect.
    pub docs: Vec<Bytes>,
    pub checkpoint_delta: SourceCheckpointDelta,
    pub force_commit: bool,
    _gauge_guard: GaugeGuard,
}

impl RawDocBatch {
    pub fn new(
        docs: Vec<Bytes>,
        checkpoint_delta: SourceCheckpointDelta,
        force_commit: bool,
    ) -> Self {
        let delta = docs.iter().map(|doc| doc.len() as i64).sum::<i64>();
        let _gauge_guard =
            GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight_data.doc_processor_mailbox, delta);

        Self {
            docs,
            checkpoint_delta,
            force_commit,
            _gauge_guard,
        }
    }

    #[cfg(test)]
    pub fn for_test(docs: &[&[u8]], range: std::ops::Range<u64>) -> Self {
        let docs = docs.iter().map(|doc| Bytes::from(doc.to_vec())).collect();
        let checkpoint_delta = SourceCheckpointDelta::from_range(range);
        Self::new(docs, checkpoint_delta, false)
    }
}

impl fmt::Debug for RawDocBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("RawDocBatch")
            .field("num_docs", &self.docs.len())
            .field("checkpoint_delta", &self.checkpoint_delta)
            .field("force_commit", &self.force_commit)
            .finish()
    }
}

impl Default for RawDocBatch {
    fn default() -> Self {
        let _gauge_guard =
            GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight_data.doc_processor_mailbox, 0);

        Self {
            docs: Vec::new(),
            checkpoint_delta: SourceCheckpointDelta::default(),
            force_commit: false,
            _gauge_guard,
        }
    }
}
