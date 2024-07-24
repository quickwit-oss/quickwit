// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use once_cell::sync::Lazy;
use quickwit_common::metrics::{new_counter, new_counter_vec, new_gauge, IntCounter, IntGauge};

pub struct IngestMetrics {
    pub ingested_docs_bytes_valid: IntCounter,
    pub ingested_docs_bytes_invalid: IntCounter,
    pub ingested_docs_invalid: IntCounter,
    pub ingested_docs_valid: IntCounter,

    pub replicated_num_bytes_total: IntCounter,
    pub replicated_num_docs_total: IntCounter,
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
