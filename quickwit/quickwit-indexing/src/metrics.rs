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
use quickwit_common::metrics::{
    new_counter, new_gauge, new_histogram_vec, HistogramVec, IntCounter, IntGauge,
};

pub struct IndexerMetrics {
    pub parsing_errors_num_docs_total: IntCounter,
    pub missing_field_num_docs_total: IntCounter,
    pub valid_num_docs_total: IntCounter,
    pub parsing_errors_num_bytes_total: IntCounter,
    pub missing_field_num_bytes_total: IntCounter,
    pub valid_num_bytes_total: IntCounter,
    pub waiting_time_to_send_message: HistogramVec,
    pub concurrent_upload_available_permits: IntGauge,
}

impl Default for IndexerMetrics {
    fn default() -> Self {
        IndexerMetrics {
            parsing_errors_num_docs_total: new_counter(
                "parsing_errors_num_docs_total",
                "Num of docs that were discarded due to a parsing error.",
                "quickwit_indexing",
            ),
            missing_field_num_docs_total: new_counter(
                "missing_field_num_docs_total",
                "Num of docs that were discard because they were missing a field.",
                "quickwit_indexing",
            ),
            valid_num_docs_total: new_counter(
                "valid_num_docs_total",
                "Num of processed docs that were valid.",
                "quickwit_indexing",
            ),
            parsing_errors_num_bytes_total: new_counter(
                "parsing_errors_num_bytes_total",
                "Sum of bytes of the documents that were discarded due to a parsing error.",
                "quickwit_indexing",
            ),
            missing_field_num_bytes_total: new_counter(
                "missing_field_num_bytes_total",
                "Sum of bytes of the documents that were discarded due to a missing field.",
                "quickwit_indexing",
            ),
            valid_num_bytes_total: new_counter(
                "valid_num_bytes_total",
                "Sum of bytes of valid documents that have been processed.",
                "quickwit_indexing",
            ),
            waiting_time_to_send_message: new_histogram_vec(
                "waiting_time_to_send_message",
                "Waiting time to send a message in an actor mailbox.",
                "quickwit_indexing",
                &["actor_name"],
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
