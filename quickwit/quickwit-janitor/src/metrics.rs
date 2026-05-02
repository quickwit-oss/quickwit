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

use std::sync::LazyLock;

use quickwit_metrics::{Counter, Gauge, Labels, counter, gauge};

pub(crate) static ONGOING_NUM_DELETE_OPERATIONS_TOTAL: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "ongoing_num_delete_operations_total",
        description: "Num of ongoing delete operations (per index).",
        subsystem: "quickwit_janitor",
    )
});

pub(crate) const INDEX_LABELS: Labels<1> = Labels::new(["index"]);

pub(crate) static GC_DELETED_SPLITS: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "gc_deleted_splits_total",
        description: "Total number of splits deleted by the garbage collector.",
        subsystem: "quickwit_janitor",
    )
});

pub(crate) const GC_RESULT_SPLIT_TYPE_LABELS: Labels<2> = Labels::new(["result", "split_type"]);

pub(crate) static GC_DELETED_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "gc_deleted_bytes_total",
        description: "Total number of bytes deleted by the garbage collector.",
        subsystem: "quickwit_janitor",
    )
});

pub(crate) const GC_SPLIT_TYPE_LABELS: Labels<1> = Labels::new(["split_type"]);

pub(crate) static GC_RUNS: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "gc_runs_total",
        description: "Total number of garbage collector execition.",
        subsystem: "quickwit_janitor",
    )
});

pub(crate) static GC_SECONDS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "gc_seconds_total",
        description: "Total time spent running the garbage collector",
        subsystem: "quickwit_janitor",
    )
});
