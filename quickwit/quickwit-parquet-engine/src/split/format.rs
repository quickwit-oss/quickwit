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

//! Parquet split format and serialization.

use serde::{Deserialize, Serialize};

use super::metadata::{ParquetSplitId, ParquetSplitMetadata, TimeRange};

/// A parquet split - the storage unit for metrics data.
///
/// Analogous to Tantivy's Split but contains Parquet file(s) instead
/// of Tantivy segments. Immutable once created.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetSplit {
    /// Split metadata.
    pub metadata: ParquetSplitMetadata,

    /// Format version for forward compatibility.
    pub format_version: u32,
}

/// Current format version.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

impl ParquetSplit {
    /// Create a new ParquetSplit.
    pub fn new(metadata: ParquetSplitMetadata) -> Self {
        Self {
            metadata,
            format_version: CURRENT_FORMAT_VERSION,
        }
    }

    /// Get the split ID.
    pub fn id(&self) -> &ParquetSplitId {
        &self.metadata.split_id
    }

    /// Get the time range.
    pub fn time_range(&self) -> &TimeRange {
        &self.metadata.time_range
    }

    /// Check if this split might contain data for the given time range.
    pub fn overlaps_time_range(&self, query_range: &TimeRange) -> bool {
        self.metadata.time_range.overlaps(query_range)
    }

    /// Check if this split might contain the given metric.
    pub fn might_contain_metric(&self, metric_name: &str) -> bool {
        self.metadata.metric_names.is_empty() || self.metadata.metric_names.contains(metric_name)
    }

    /// Serialize to JSON bytes.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes.
    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_split() -> ParquetSplit {
        let metadata = ParquetSplitMetadata::metrics_builder()
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .num_rows(10000)
            .size_bytes(1024 * 1024)
            .add_metric_name("cpu.usage")
            .add_metric_name("memory.used")
            .add_low_cardinality_tag(crate::split::TAG_SERVICE, "web-server")
            .build();

        ParquetSplit::new(metadata)
    }

    #[test]
    fn test_split_creation() {
        let split = create_test_split();
        assert_eq!(split.format_version, CURRENT_FORMAT_VERSION);
        assert_eq!(split.metadata.num_rows, 10000);
    }

    #[test]
    fn test_time_range_overlap() {
        let split = create_test_split();

        // Overlapping range
        assert!(split.overlaps_time_range(&TimeRange::new(1500, 2500)));

        // Non-overlapping range
        assert!(!split.overlaps_time_range(&TimeRange::new(3000, 4000)));
    }

    #[test]
    fn test_metric_pruning() {
        let split = create_test_split();

        assert!(split.might_contain_metric("cpu.usage"));
        assert!(!split.might_contain_metric("disk.io"));
    }

    #[test]
    fn test_json_roundtrip() {
        let split = create_test_split();
        let json = split.to_json().unwrap();
        let restored = ParquetSplit::from_json(&json).unwrap();

        assert_eq!(split.id().as_str(), restored.id().as_str());
        assert_eq!(split.metadata.num_rows, restored.metadata.num_rows);
    }
}
