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

use quickwit_common::metrics::{Counter, Gauge, counter, gauge};

pub struct IngestMetrics {
    pub docs_bytes_total: Counter,
    pub docs_total: Counter,

    pub replicated_num_bytes_total: Counter,
    pub replicated_num_docs_total: Counter,
    #[allow(dead_code)] // this really shouldn't be dead, it needs to be used somewhere
    pub queue_count: Gauge,
}

static DOCS_BYTES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "docs_bytes_total",
        description: "Total size of the docs ingested, measured in ingester's leader, after validation and before persistence/replication",
        subsystem: "ingest",
    )
});

static DOCS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "docs_total",
        description: "Total number of the docs ingested, measured in ingester's leader, after validation and before persistence/replication",
        subsystem: "ingest",
    )
});

static REPLICATED_NUM_BYTES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "replicated_num_bytes_total",
        description: "Total size in bytes of the replicated docs.",
        subsystem: "ingest",
    )
});

static REPLICATED_NUM_DOCS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "replicated_num_docs_total",
        description: "Total number of docs replicated.",
        subsystem: "ingest",
    )
});

static QUEUE_COUNT: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "queue_count",
        description: "Number of queues currently active",
        subsystem: "ingest",
    )
});

impl Default for IngestMetrics {
    fn default() -> Self {
        IngestMetrics {
            docs_bytes_total: DOCS_BYTES_TOTAL.clone(),
            docs_total: DOCS_TOTAL.clone(),
            replicated_num_bytes_total: REPLICATED_NUM_BYTES_TOTAL.clone(),
            replicated_num_docs_total: REPLICATED_NUM_DOCS_TOTAL.clone(),
            queue_count: QUEUE_COUNT.clone(),
        }
    }
}

pub static INGEST_METRICS: LazyLock<IngestMetrics> = LazyLock::new(IngestMetrics::default);
