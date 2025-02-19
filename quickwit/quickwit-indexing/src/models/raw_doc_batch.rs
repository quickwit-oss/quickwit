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

use bytes::Bytes;
use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use quickwit_metastore::checkpoint::SourceCheckpointDelta;

pub struct RawDocBatch {
    // Do not directly append documents to this vector; otherwise, in-flight metrics will be
    // incorrect.
    pub docs: Vec<Bytes>,
    pub checkpoint_delta: SourceCheckpointDelta,
    pub force_commit: bool,
    _gauge_guard: GaugeGuard<'static>,
}

impl RawDocBatch {
    pub fn new(
        docs: Vec<Bytes>,
        checkpoint_delta: SourceCheckpointDelta,
        force_commit: bool,
    ) -> Self {
        let delta = docs.iter().map(|doc| doc.len() as i64).sum::<i64>();
        let mut gauge_guard =
            GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.doc_processor_mailbox);
        gauge_guard.add(delta);

        Self {
            docs,
            checkpoint_delta,
            force_commit,
            _gauge_guard: gauge_guard,
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
        let _gauge_guard = GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.doc_processor_mailbox);
        Self {
            docs: Vec::new(),
            checkpoint_delta: SourceCheckpointDelta::default(),
            force_commit: false,
            _gauge_guard,
        }
    }
}
