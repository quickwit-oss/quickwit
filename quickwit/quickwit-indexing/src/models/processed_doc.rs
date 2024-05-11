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

use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use quickwit_metastore::checkpoint::SourceCheckpointDelta;
use tantivy::{DateTime, TantivyDocument};

pub struct ProcessedDoc {
    pub doc: TantivyDocument,
    pub timestamp_opt: Option<DateTime>,
    pub partition: u64,
    pub num_bytes: usize,
}

impl fmt::Debug for ProcessedDoc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessedDoc")
            .field("timestamp_opt", &self.timestamp_opt)
            .field("partition", &self.partition)
            .field("num_bytes", &self.num_bytes)
            .finish()
    }
}

pub struct ProcessedDocBatch {
    // Do not directly append documents to this vector; otherwise, in-flight metrics will be
    // incorrect.
    pub docs: Vec<ProcessedDoc>,
    pub checkpoint_delta: SourceCheckpointDelta,
    pub force_commit: bool,
    _gauge_guard: GaugeGuard<'static>,
}

impl ProcessedDocBatch {
    pub fn new(
        docs: Vec<ProcessedDoc>,
        checkpoint_delta: SourceCheckpointDelta,
        force_commit: bool,
    ) -> Self {
        let delta = docs.iter().map(|doc| doc.num_bytes as i64).sum::<i64>();
        let mut gauge_guard = GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.indexer_mailbox);
        gauge_guard.add(delta);
        Self {
            docs,
            checkpoint_delta,
            force_commit,
            _gauge_guard: gauge_guard,
        }
    }
}

impl fmt::Debug for ProcessedDocBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessedDocBatch")
            .field("num_docs", &self.docs.len())
            .field("checkpoint_delta", &self.checkpoint_delta)
            .field("force_commit", &self.force_commit)
            .finish()
    }
}
