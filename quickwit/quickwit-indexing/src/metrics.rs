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
use quickwit_common::metrics::{
    new_counter_vec, new_gauge, new_gauge_vec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
};

#[derive(Debug)]
pub struct DocProcessorMetrics {
    pub valid: IntCounter,
    pub doc_mapper_error: IntCounter,
    pub json_parse_error: IntCounter,
    pub otlp_parse_error: IntCounter,
    pub transform_error: IntCounter,
}

impl DocProcessorMetrics {
    pub fn from_counters(index_id: &str, counter_vec: &IntCounterVec<2>) -> DocProcessorMetrics {
        let index_label = quickwit_common::metrics::index_label(index_id);
        DocProcessorMetrics {
            valid: counter_vec.with_label_values([index_label, "valid"]),
            doc_mapper_error: counter_vec.with_label_values([index_label, "doc_mapper_error"]),
            json_parse_error: counter_vec.with_label_values([index_label, "json_parse_error"]),
            otlp_parse_error: counter_vec.with_label_values([index_label, "otlp_parse_error"]),
            transform_error: counter_vec.with_label_values([index_label, "transform_error"]),
        }
    }
}

pub struct IndexerMetrics {
    pub processed_docs_total: IntCounterVec<2>,
    pub processed_bytes: IntCounterVec<2>,
    pub backpressure_micros: IntCounterVec<1>,
    pub available_concurrent_upload_permits: IntGaugeVec<1>,
    pub split_builders: IntGauge,
    pub ongoing_merge_operations: IntGauge,
    pub pending_merge_operations: IntGauge,
    pub pending_merge_bytes: IntGauge,
}

impl Default for IndexerMetrics {
    fn default() -> Self {
        IndexerMetrics {
            processed_docs_total: new_counter_vec(
                "processed_docs_total",
                "Number of processed docs by index, source and processed status in [valid, \
                 schema_error, parse_error, transform_error]",
                "indexing",
                &[],
                ["index", "docs_processed_status"],
            ),
            processed_bytes: new_counter_vec(
                "processed_bytes",
                "Number of bytes of processed documents by index, source and processed status in \
                 [valid, schema_error, parse_error, transform_error]",
                "indexing",
                &[],
                ["index", "docs_processed_status"],
            ),
            backpressure_micros: new_counter_vec(
                "backpressure_micros",
                "Amount of time spent in backpressure (in micros). This time only includes the \
                 amount of time spent waiting for a place in the queue of another actor.",
                "indexing",
                &[],
                ["actor_name"],
            ),
            available_concurrent_upload_permits: new_gauge_vec(
                "concurrent_upload_available_permits_num",
                "Number of available concurrent upload permits by component in [merger, indexer]",
                "indexing",
                &[],
                ["component"],
            ),
            split_builders: new_gauge(
                "split_builders",
                "Number of existing index writer instances.",
                "indexing",
                &[],
            ),
            ongoing_merge_operations: new_gauge(
                "ongoing_merge_operations",
                "Number of ongoing merge operations",
                "indexing",
                &[],
            ),
            pending_merge_operations: new_gauge(
                "pending_merge_operations",
                "Number of pending merge operations",
                "indexing",
                &[],
            ),
            pending_merge_bytes: new_gauge(
                "pending_merge_bytes",
                "Number of pending merge bytes",
                "indexing",
                &[],
            ),
        }
    }
}

/// `INDEXER_METRICS` exposes indexing related metrics through a prometheus
/// endpoint.
pub static INDEXER_METRICS: Lazy<IndexerMetrics> = Lazy::new(IndexerMetrics::default);
