// Copyright (C) 2022 Quickwit, Inc.
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
use quickwit_common::metrics::{new_counter_vec, new_gauge_vec, IntCounterVec, IntGaugeVec};

pub struct IndexerMetrics {
    pub processed_docs_total: IntCounterVec<3>,
    pub processed_bytes: IntCounterVec<3>,
    pub backpressure_micros: IntCounterVec<2>,
    pub available_concurrent_upload_permits: IntGaugeVec<1>,
    pub ongoing_merge_operations: IntGaugeVec<2>,
}

impl Default for IndexerMetrics {
    fn default() -> Self {
        IndexerMetrics {
            processed_docs_total: new_counter_vec(
                "processed_docs_total",
                "Number of processed docs by index, source and processed status in [valid, \
                 missing_field, parsing_error]",
                "quickwit_indexing",
                ["index", "source", "docs_processed_status"],
            ),
            processed_bytes: new_counter_vec(
                "processed_bytes",
                "Number of bytes of processed documents by index, source and processed status in \
                 [valid, missing_field, parsing_error]",
                "quickwit_indexing",
                ["index", "source", "docs_processed_status"],
            ),
            backpressure_micros: new_counter_vec(
                "backpressure_micros",
                "Amount of time spent in backpressure (in micros). This time only includes the \
                 amount of time spent waiting for a place in the queue of another actor.",
                "quickwit_indexing",
                ["index", "actor_name"],
            ),
            available_concurrent_upload_permits: new_gauge_vec(
                "concurrent_upload_available_permits_num",
                "Number of available concurrent upload permits by component in [merger, indexer]",
                "quickwit_indexing",
                ["component"],
            ),
            ongoing_merge_operations: new_gauge_vec(
                "ongoing_merge_operations",
                "Number of ongoing merge operations",
                "quickwit_indexing",
                ["index", "source"],
            ),
        }
    }
}

/// `INDEXER_METRICS` exposes indexing related metrics through a prometheus
/// endpoint.
pub static INDEXER_METRICS: Lazy<IndexerMetrics> = Lazy::new(IndexerMetrics::default);
