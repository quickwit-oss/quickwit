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

use quickwit_metrics::{LabelNames, LazyCounter, LazyGauge, label_names, lazy_counter, lazy_gauge};

pub(crate) const SOURCE_UID: LabelNames<1> = label_names!("source_uid");
pub(crate) const SOURCE_UID_MERGE_LEVEL: LabelNames<2> = label_names!("source_uid", "merge_level");
pub(crate) const OPERATION: LabelNames<1> = label_names!("operation");

pub(crate) static NEW_SPLITS_SCANNED: LazyCounter = lazy_counter!(
    name: "new_splits_scanned",
    description: "cumulative number of immature splits scanned from the metastore",
    subsystem: "compaction_planner",
);

pub(crate) static SPLITS_NEEDING_COMPACTION: LazyGauge = lazy_gauge!(
    name: "splits_needing_compaction",
    description: "number of splits currently tracked as needing compaction",
    subsystem: "compaction_planner",
);

pub(crate) static PENDING_MERGE_OPERATIONS: LazyGauge = lazy_gauge!(
    name: "pending_merge_operations",
    description: "number of pending merge operations awaiting assignment",
    subsystem: "compaction_planner",
);

pub(crate) static TIMED_OUT_OPERATIONS: LazyCounter = lazy_counter!(
    name: "timed_out_operations",
    description: "cumulative number of merge operations that timed out waiting for a worker heartbeat",
    subsystem: "compaction_planner",
);

pub(crate) static METASTORE_ERRORS: LazyCounter = lazy_counter!(
    name: "metastore_errors",
    description: "cumulative number of metastore errors encountered by the compaction planner",
    subsystem: "compaction_planner",
);
