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

use quickwit_common::metrics::exponential_buckets;
use quickwit_metrics::{LabelNames, LazyCounter, LazyGauge, LazyHistogram, lazy_counter, lazy_gauge, lazy_histogram, label_names};

/// Shared label template for the per-compaction metrics keyed by source and merge level.
pub(crate) const SOURCE_UID_MERGE_LEVEL: LabelNames<2> =
    label_names!("source_uid", "merge_level");

pub(crate) static COMPACTIONS_IN_PROGRESS: LazyGauge = lazy_gauge!(
    name: "compactions_in_progress",
    description: "number of compaction merge operations currently running on this compactor",
    subsystem: "compactor",
);

pub(crate) static COMPACTIONS_FAILED: LazyCounter = lazy_counter!(
    name: "compactions_failed",
    description: "total number of compaction merge operations that have failed",
    subsystem: "compactor",
);

pub(crate) static COMPACTIONS_SUCCEEDED: LazyCounter = lazy_counter!(
    name: "compactions_succeeded",
    description: "total number of compaction merge operations that have completed successfully",
    subsystem: "compactor",
);

pub(crate) static AVAILABLE_SLOTS: LazyGauge = lazy_gauge!(
    name: "available_slots",
    description: "number of compaction slots currently available on this compactor",
    subsystem: "compactor",
);

pub(crate) static COMPACTION_DURATION: LazyHistogram = lazy_histogram!(
    name: "compaction_duration_seconds",
    description: "duration of compaction merge operations in seconds",
    subsystem: "compactor",
    buckets: exponential_buckets(0.5, 2.0, 14).expect("compaction duration buckets should be valid"),
);