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
use quickwit_common::metrics::{new_counter_vec, new_gauge, IntCounterVec, IntGauge};

pub struct IndexerMetrics {
    pub parsing_errors_num_docs_total: IntCounterVec,
    pub missing_field_num_docs_total: IntCounterVec,
    pub valid_num_docs_total: IntCounterVec,
    pub parsing_errors_num_bytes_total: IntCounterVec,
    pub missing_field_num_bytes_total: IntCounterVec,
    pub valid_num_bytes_total: IntCounterVec,
    pub concurrent_upload_available_permits: IntGauge,
}

impl Default for IndexerMetrics {
    fn default() -> Self {
        IndexerMetrics {
            parsing_errors_num_docs_total: new_counter_vec(
                "parsing_errors_num_docs_total",
                "Num of docs that were discarded due to a parsing error (per index).",
                "quickwit_indexing",
                &["index"],
            ),
            missing_field_num_docs_total: new_counter_vec(
                "missing_field_num_docs_total",
                "Num of docs that were discard because they were missing a field (per index).",
                "quickwit_indexing",
                &["index"],
            ),
            valid_num_docs_total: new_counter_vec(
                "valid_num_docs_total",
                "Num of processed docs that were valid (per index).",
                "quickwit_indexing",
                &["index"],
            ),
            parsing_errors_num_bytes_total: new_counter_vec(
                "parsing_errors_num_bytes_total",
                "Sum of bytes of the documents that were discarded due to a parsing error (per \
                 index).",
                "quickwit_indexing",
                &["index"],
            ),
            missing_field_num_bytes_total: new_counter_vec(
                "missing_field_num_bytes_total",
                "Sum of bytes of the documents that were discarded due to a missing field (per \
                 index).",
                "quickwit_indexing",
                &["index"],
            ),
            valid_num_bytes_total: new_counter_vec(
                "valid_num_bytes_total",
                "Sum of bytes of valid documents that have been processed (per index).",
                "quickwit_indexing",
                &["index"],
            ),
            concurrent_upload_available_permits: new_gauge(
                "concurrent_upload_available_permits",
                "Number of concurrent upload available permits.",
                "quickwit_indexing",
            ),
        }
    }
}

/// `INDEXER_METRICS` exposes a bunch a set of storage/cache related metrics through a prometheus
/// endpoint.
pub static INDEXER_METRICS: Lazy<IndexerMetrics> = Lazy::new(IndexerMetrics::default);
