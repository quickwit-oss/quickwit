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
