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
use quickwit_common::metrics::{new_counter, new_histogram, Histogram, IntCounter};

pub struct IndexingMetrics {
    pub publish_splits_total: IntCounter,
    pub published_original_bytes_total: IntCounter,
    pub published_docs_total: IntCounter,
    pub time_to_search_seconds: Histogram,
}

impl Default for IndexingMetrics {
    fn default() -> Self {
        IndexingMetrics {
            publish_splits_total: new_counter(
                "published_splits_total",
                "Total number of split published",
                "quickwit",
            ),
            published_original_bytes_total: new_counter(
                "published_original_bytes_total",
                "Total number of (original) bytes indexed",
                "quickwit",
            ),
            published_docs_total: new_counter(
                "published_docs_total",
                "Total number of docs indexed",
                "quickwit",
            ),
            time_to_search_seconds: new_histogram(
                "time_to_search_seconds",
                "Time to search seconds",
                "quickwit",
            ),
        }
    }
}

pub static INDEXING_METRICS: Lazy<IndexingMetrics> = Lazy::new(IndexingMetrics::default);
