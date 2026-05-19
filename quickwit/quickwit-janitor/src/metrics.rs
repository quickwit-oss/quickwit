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

use quickwit_metrics::{LazyCounter, LazyGauge, lazy_counter, lazy_gauge};

pub(crate) static ONGOING_NUM_DELETE_OPERATIONS_TOTAL: LazyGauge = lazy_gauge!(
        name: "ongoing_num_delete_operations_total",
        description: "Num of ongoing delete operations (per index).",
        subsystem: "janitor",
);

pub(crate) static GC_DELETED_SPLITS: LazyCounter = lazy_counter!(
        name: "gc_deleted_splits_total",
        description: "Total number of splits deleted by the garbage collector.",
        subsystem: "janitor",
);

pub(crate) static GC_DELETED_BYTES: LazyCounter = lazy_counter!(
        name: "gc_deleted_bytes_total",
        description: "Total number of bytes deleted by the garbage collector.",
        subsystem: "janitor",
);

pub(crate) static GC_RUNS: LazyCounter = lazy_counter!(
        name: "gc_runs_total",
        description: "Total number of garbage collector execition.",
        subsystem: "janitor",
);

pub(crate) static GC_SECONDS_TOTAL: LazyCounter = lazy_counter!(
        name: "gc_seconds_total",
        description: "Total time spent running the garbage collector",
        subsystem: "janitor",
);
