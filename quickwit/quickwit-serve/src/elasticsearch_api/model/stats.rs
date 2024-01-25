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

use std::collections::HashMap;
use std::ops::AddAssign;

use quickwit_metastore::SplitMetadata;
use serde::{Deserialize, Serialize};

/// Returns JSON in the format:
///
/// {
///   "_all": {
///     "primaries": {
///       "store": {"size_in_bytes": 123456789},
///       "docs": {"count": 5000}
///     },
///     "total": {
///       "segments": {"count": 100},
///       "docs": {"count": 5000}
///     }
///   },
///   "indices": {
///     "exampleIndex": {
///       "primaries": {
///         "store": {"size_in_bytes": 123456789},
///         "docs": {"count": 5000}
///       },
///       "total": {
///         "segments": {"count": 50},
///         "docs": {"count": 5000}
///       }
///     }
///   }
/// }
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ElasticsearchStatsResponse {
    pub _all: StatsResponseEntry,
    pub indices: HashMap<String, StatsResponseEntry>, // String is Field name
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct StatsResponseEntry {
    primaries: StatsPrimariesResponse,
    total: StatsTotalResponse,
}

impl AddAssign for StatsResponseEntry {
    fn add_assign(&mut self, rhs: Self) {
        self.primaries.store.size_in_bytes += rhs.primaries.store.size_in_bytes;
        self.primaries.docs.count += rhs.primaries.docs.count;
        self.total.segments.count += rhs.total.segments.count;
        self.total.docs.count += rhs.total.docs.count;
    }
}

impl From<SplitMetadata> for StatsResponseEntry {
    fn from(split_metadata: SplitMetadata) -> Self {
        let mut stats_response_entry = StatsResponseEntry::default();
        stats_response_entry.primaries.store.size_in_bytes =
            split_metadata.as_split_info().file_size_bytes.as_u64();
        stats_response_entry.primaries.docs.count = split_metadata.num_docs as u64;
        stats_response_entry.total.docs.count = split_metadata.num_docs as u64;
        stats_response_entry.total.segments.count = 1;
        stats_response_entry
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct StatsPrimariesResponse {
    store: StatsStoreResponse,
    docs: StatsDocsResponse,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct StatsStoreResponse {
    size_in_bytes: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct StatsDocsResponse {
    count: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct StatsTotalResponse {
    segments: StatsTotalSegmentsResponse,
    docs: StatsDocsResponse,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct StatsTotalSegmentsResponse {
    count: u64,
}
