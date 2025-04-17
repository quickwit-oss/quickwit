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

use once_cell::sync::Lazy;
use quickwit_common::metrics::{IntCounter, IntGauge, new_counter, new_counter_vec, new_gauge};

pub struct IngestMetrics {
    pub ingested_docs_bytes_valid: IntCounter,
    pub ingested_docs_bytes_invalid: IntCounter,
    pub ingested_docs_invalid: IntCounter,
    pub ingested_docs_valid: IntCounter,

    pub replicated_num_bytes_total: IntCounter,
    pub replicated_num_docs_total: IntCounter,
    #[allow(dead_code)] // this really shouldn't be dead, it needs to be used somewhere
    pub queue_count: IntGauge,
}

impl Default for IngestMetrics {
    fn default() -> Self {
        let ingest_docs_bytes_total = new_counter_vec(
            "docs_bytes_total",
            "Total size of the docs ingested, measured in ingester's leader, after validation and \
             before persistence/replication",
            "ingest",
            &[],
            ["validity"],
        );
        let ingested_docs_bytes_valid = ingest_docs_bytes_total.with_label_values(["valid"]);
        let ingested_docs_bytes_invalid = ingest_docs_bytes_total.with_label_values(["invalid"]);

        let ingest_docs_total = new_counter_vec(
            "docs_total",
            "Total number of the docs ingested, measured in ingester's leader, after validation \
             and before persistence/replication",
            "ingest",
            &[],
            ["validity"],
        );
        let ingested_docs_valid = ingest_docs_total.with_label_values(["valid"]);
        let ingested_docs_invalid = ingest_docs_total.with_label_values(["invalid"]);

        IngestMetrics {
            ingested_docs_bytes_valid,
            ingested_docs_bytes_invalid,
            ingested_docs_valid,
            ingested_docs_invalid,
            replicated_num_bytes_total: new_counter(
                "replicated_num_bytes_total",
                "Total size in bytes of the replicated docs.",
                "ingest",
                &[],
            ),
            replicated_num_docs_total: new_counter(
                "replicated_num_docs_total",
                "Total number of docs replicated.",
                "ingest",
                &[],
            ),
            queue_count: new_gauge(
                "queue_count",
                "Number of queues currently active",
                "ingest",
                &[],
            ),
        }
    }
}

pub static INGEST_METRICS: Lazy<IngestMetrics> = Lazy::new(IngestMetrics::default);
