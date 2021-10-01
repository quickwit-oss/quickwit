// Copyright (C) 2021 Quickwit, Inc.
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
use prometheus::{IntCounterVec, IntGaugeVec};
use quickwit_common::metrics::{new_counter_vec, new_gauge_vec};

pub struct Counters {
    pub num_docs: IntGaugeVec,
    pub num_invalid_docs: IntGaugeVec,
    pub num_local_splits: IntGaugeVec,
    pub num_published_splits: IntGaugeVec,
    pub num_uploaded_splits: IntGaugeVec,
    pub num_staged_splits: IntGaugeVec,
    pub total_bytes_processed: IntGaugeVec,
    pub total_size_splits: IntGaugeVec,
}

impl Default for Counters {
    fn default() -> Self {
        Counters {
            num_docs: new_gauge_vec("indexing:num_docs", "Number of docs indexed", &["index_id"]),
            num_invalid_docs: new_gauge_vec("indexing:num_invalid_docs", "Number of invalid docs", &["index_id"]),
            num_local_splits: new_gauge_vec("indexing:num_local_splits", "Number of local splits", &["index_id"]),
            num_published_splits: new_gauge_vec("indexing:num_published_splits", "Number of published splits", &["index_id"]),
            num_uploaded_splits: new_gauge_vec("indexing:num_uploaded_splits", "Number of uploaded splits", &["index_id"]),
            num_staged_splits: new_gauge_vec("indexing:num_staged_splits", "Number of staged splits", &["index_id"]),
            total_bytes_processed: new_gauge_vec("indexing:total_bytes_processed", "Total bytes processed", &["index_id"]),
            total_size_splits: new_gauge_vec("indexing:total_size_splits", "Total size of splits", &["index_id"]),
        }
    }
}

pub static COUNTERS: Lazy<Counters> = Lazy::new(Counters::default);
