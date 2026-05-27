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

pub(crate) const VALIDITY: LabelNames<1> = label_names!("validity");

pub(crate) static DOCS_BYTES_TOTAL: LazyCounter = lazy_counter!(
        name: "docs_bytes_total",
        description: "Total size of the docs ingested, measured in ingester's leader, after validation and before persistence/replication",
        subsystem: "ingest",
);

pub(crate) static DOCS_TOTAL: LazyCounter = lazy_counter!(
        name: "docs_total",
        description: "Total number of the docs ingested, measured in ingester's leader, after validation and before persistence/replication",
        subsystem: "ingest",
);

pub(crate) static REPLICATED_NUM_BYTES_TOTAL: LazyCounter = lazy_counter!(
        name: "replicated_num_bytes_total",
        description: "Total size in bytes of the replicated docs.",
        subsystem: "ingest",
);

pub(crate) static REPLICATED_NUM_DOCS_TOTAL: LazyCounter = lazy_counter!(
        name: "replicated_num_docs_total",
        description: "Total number of docs replicated.",
        subsystem: "ingest",
);

#[allow(dead_code)] // this really shouldn't be dead, it needs to be used somewhere
pub(crate) static QUEUE_COUNT: LazyGauge = lazy_gauge!(
        name: "queue_count",
        description: "Number of queues currently active",
        subsystem: "ingest",
);
