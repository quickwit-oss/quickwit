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

use arrow_array::RecordBatch;
use quickwit_common::metrics::IN_FLIGHT_DOC_PROCESSOR_MAILBOX;
use quickwit_metastore::checkpoint::SourceCheckpointDelta;
use quickwit_metrics::GaugeGuard;

/// Rows of an Arrow record batch sent by the Parquet source to the doc processor, which builds
/// documents from the columns directly instead of going through JSON.
pub struct ArrowDocBatch {
    pub record_batch: RecordBatch,
    pub checkpoint_delta: SourceCheckpointDelta,
    pub force_commit: bool,
    _gauge_guard: GaugeGuard,
}

impl ArrowDocBatch {
    pub fn new(
        record_batch: RecordBatch,
        checkpoint_delta: SourceCheckpointDelta,
        force_commit: bool,
    ) -> Self {
        let num_bytes = record_batch.get_array_memory_size();
        let gauge_guard = GaugeGuard::new(&IN_FLIGHT_DOC_PROCESSOR_MAILBOX, num_bytes as f64);
        Self {
            record_batch,
            checkpoint_delta,
            force_commit,
            _gauge_guard: gauge_guard,
        }
    }
}

impl fmt::Debug for ArrowDocBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("ArrowDocBatch")
            .field("num_rows", &self.record_batch.num_rows())
            .field("checkpoint_delta", &self.checkpoint_delta)
            .field("force_commit", &self.force_commit)
            .finish()
    }
}
