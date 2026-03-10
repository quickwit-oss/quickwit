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

//! Metrics split metadata definitions.

use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

// Well-known tag key constants
pub const TAG_SERVICE: &str = "service";
pub const TAG_ENV: &str = "env";
pub const TAG_DATACENTER: &str = "datacenter";
pub const TAG_REGION: &str = "region";
pub const TAG_HOST: &str = "host";

/// Unique identifier for a metrics split.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SplitId(String);

impl SplitId {
    /// Create a new SplitId from a string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Generates a new unique SplitId using a ULID (timestamp + randomness).
    pub fn generate() -> Self {
        Self(format!("metrics_{}", ulid::Ulid::new().to_string().to_lowercase()))
    }

    /// Get the string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SplitId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Time range covered by a metrics split.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    /// Start timestamp in seconds (inclusive).
    pub start_secs: u64,
    /// End timestamp in seconds (exclusive).
    pub end_secs: u64,
}

impl TimeRange {
    /// Create a new time range.
    pub fn new(start_secs: u64, end_secs: u64) -> Self {
        debug_assert!(start_secs <= end_secs);
        Self {
            start_secs,
            end_secs,
        }
    }

    /// Check if a timestamp falls within this range.
    pub fn contains(&self, timestamp_secs: u64) -> bool {
        timestamp_secs >= self.start_secs && timestamp_secs < self.end_secs
    }

    /// Check if this range overlaps with another.
    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.start_secs < other.end_secs && other.start_secs < self.end_secs
    }

    /// Duration of this range in seconds.
    pub fn duration_secs(&self) -> u64 {
        self.end_secs - self.start_secs
    }
}

/// State of a metrics split in the metastore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricsSplitState {
    /// Split is staged (being written, not yet queryable).
    Staged,
    /// Split is published (queryable).
    Published,
    /// Split is marked for deletion.
    MarkedForDeletion,
}

impl MetricsSplitState {
    /// Returns a string representation for database storage.
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricsSplitState::Staged => "Staged",
            MetricsSplitState::Published => "Published",
            MetricsSplitState::MarkedForDeletion => "MarkedForDeletion",
        }
    }
}

impl std::fmt::Display for MetricsSplitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Metadata for a metrics split.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSplitMetadata {
    /// Unique split identifier.
    pub split_id: SplitId,

    /// Index unique identifier for Postgres foreign key relationship.
    pub index_uid: String,

    /// Time range covered by this split.
    pub time_range: TimeRange,

    /// Number of data points in this split.
    pub num_rows: u64,

    /// Size of Parquet file(s) in bytes.
    pub size_bytes: u64,

    /// Distinct metric names in this split (for pruning).
    pub metric_names: HashSet<String>,

    /// Low-cardinality tag values by key (for Postgres pruning).
    /// Tags with cardinality < CARDINALITY_THRESHOLD are stored here.
    /// Format: HashMap<tag_key, HashSet<tag_value>>
    /// Example: {"service": {"web", "api"}, "env": {"prod", "staging"}}
    pub low_cardinality_tags: HashMap<String, HashSet<String>>,

    /// High-cardinality tag keys (for Parquet bloom filter).
    /// Tags with cardinality >= CARDINALITY_THRESHOLD have only their keys stored here.
    /// The actual values are stored in Parquet bloom filters.
    pub high_cardinality_tag_keys: HashSet<String>,

    /// When this split was created.
    pub created_at: SystemTime,
}

impl MetricsSplitMetadata {
    /// Returns the parquet filename for this split, relative to the storage root.
    /// Always `{split_id}.parquet`.
    pub fn parquet_filename(&self) -> String {
        format!("{}.parquet", self.split_id)
    }

    /// Cardinality threshold for routing tags to Postgres vs Parquet.
    /// Tags with < CARDINALITY_THRESHOLD unique values go to Postgres.
    /// Tags with >= CARDINALITY_THRESHOLD unique values use Parquet bloom filters.
    pub const CARDINALITY_THRESHOLD: usize = 1000;

    /// Create a new MetricsSplitMetadata builder.
    pub fn builder() -> MetricsSplitMetadataBuilder {
        MetricsSplitMetadataBuilder::default()
    }

    /// Check if a tag key exceeds the cardinality threshold.
    pub fn is_high_cardinality(&self, tag_key: &str) -> bool {
        self.high_cardinality_tag_keys.contains(tag_key)
    }

    /// Get all tag values for a low-cardinality tag key.
    pub fn get_tag_values(&self, tag_key: &str) -> Option<&HashSet<String>> {
        self.low_cardinality_tags.get(tag_key)
    }

    /// Get service names (convenience method for common query pattern).
    pub fn service_names(&self) -> Option<&HashSet<String>> {
        self.get_tag_values(TAG_SERVICE)
    }

    /// Promote a tag key from low to high cardinality storage.
    /// Called when tag value count exceeds CARDINALITY_THRESHOLD during split finalization.
    pub fn promote_to_high_cardinality(&mut self, tag_key: &str) {
        self.low_cardinality_tags.remove(tag_key);
        self.high_cardinality_tag_keys.insert(tag_key.to_string());
    }

    /// Finalize tag cardinality by checking thresholds.
    /// Tags exceeding CARDINALITY_THRESHOLD are promoted to high cardinality.
    pub fn finalize_tag_cardinality(&mut self) {
        let keys_to_promote: Vec<String> = self
            .low_cardinality_tags
            .iter()
            .filter(|(_, values)| values.len() >= Self::CARDINALITY_THRESHOLD)
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_promote {
            self.promote_to_high_cardinality(&key);
        }
    }
}

/// Builder for MetricsSplitMetadata.
#[derive(Default)]
pub struct MetricsSplitMetadataBuilder {
    split_id: Option<SplitId>,
    index_uid: Option<String>,
    time_range: Option<TimeRange>,
    num_rows: u64,
    size_bytes: u64,
    metric_names: HashSet<String>,
    low_cardinality_tags: HashMap<String, HashSet<String>>,
    high_cardinality_tag_keys: HashSet<String>,
}

impl MetricsSplitMetadataBuilder {
    pub fn split_id(mut self, id: SplitId) -> Self {
        self.split_id = Some(id);
        self
    }

    pub fn time_range(mut self, range: TimeRange) -> Self {
        self.time_range = Some(range);
        self
    }

    pub fn num_rows(mut self, count: u64) -> Self {
        self.num_rows = count;
        self
    }

    pub fn size_bytes(mut self, size: u64) -> Self {
        self.size_bytes = size;
        self
    }

    pub fn add_metric_name(mut self, name: impl Into<String>) -> Self {
        self.metric_names.insert(name.into());
        self
    }

    pub fn index_uid(mut self, uid: impl Into<String>) -> Self {
        self.index_uid = Some(uid.into());
        self
    }

    pub fn add_low_cardinality_tag(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.low_cardinality_tags
            .entry(key.into())
            .or_default()
            .insert(value.into());
        self
    }

    pub fn add_high_cardinality_tag_key(mut self, key: impl Into<String>) -> Self {
        self.high_cardinality_tag_keys.insert(key.into());
        self
    }

    /// Add a tag, automatically routing to low cardinality storage.
    /// For initial ingestion, always routes to low cardinality; threshold check happens at split
    /// finalization.
    pub fn add_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        self.low_cardinality_tags
            .entry(key)
            .or_default()
            .insert(value);
        self
    }

    pub fn build(self) -> MetricsSplitMetadata {
        MetricsSplitMetadata {
            split_id: self.split_id.unwrap_or_else(SplitId::generate),
            index_uid: self.index_uid.expect("index_uid is required"),
            time_range: self.time_range.expect("time_range is required"),
            num_rows: self.num_rows,
            size_bytes: self.size_bytes,
            metric_names: self.metric_names,
            low_cardinality_tags: self.low_cardinality_tags,
            high_cardinality_tag_keys: self.high_cardinality_tag_keys,
            created_at: SystemTime::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_id_generation() {
        let id1 = SplitId::generate();
        // Sleep 1ms to ensure different timestamp
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = SplitId::generate();
        assert_ne!(id1.as_str(), id2.as_str());
        assert!(id1.as_str().starts_with("metrics_"));
    }

    #[test]
    fn test_time_range_overlap() {
        let range1 = TimeRange::new(100, 200);
        let range2 = TimeRange::new(150, 250);
        let range3 = TimeRange::new(200, 300);
        let range4 = TimeRange::new(50, 100);

        assert!(range1.overlaps(&range2));
        assert!(!range1.overlaps(&range3)); // Adjacent, not overlapping
        assert!(!range1.overlaps(&range4)); // Adjacent, not overlapping
    }

    #[test]
    fn test_metadata_builder_with_tags() {
        let metadata = MetricsSplitMetadata::builder()
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .add_metric_name("cpu.usage")
            .add_low_cardinality_tag(TAG_SERVICE, "web")
            .add_low_cardinality_tag(TAG_SERVICE, "api")
            .add_low_cardinality_tag(TAG_ENV, "prod")
            .add_high_cardinality_tag_key(TAG_HOST)
            .build();

        assert_eq!(metadata.index_uid, "test-index:00000000000000000000000000");
        assert!(metadata.metric_names.contains("cpu.usage"));
        assert_eq!(metadata.get_tag_values(TAG_SERVICE).unwrap().len(), 2);
        assert!(
            metadata
                .get_tag_values(TAG_SERVICE)
                .unwrap()
                .contains("web")
        );
        assert!(metadata.get_tag_values(TAG_ENV).unwrap().contains("prod"));
        assert!(metadata.is_high_cardinality(TAG_HOST));
        assert!(!metadata.is_high_cardinality(TAG_SERVICE));
    }

    #[test]
    fn test_cardinality_promotion() {
        let mut metadata = MetricsSplitMetadata::builder()
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .build();

        // Add 1001 unique values for "host" tag
        for i in 0..1001 {
            metadata
                .low_cardinality_tags
                .entry(TAG_HOST.to_string())
                .or_default()
                .insert(format!("host-{}", i));
        }

        assert!(!metadata.is_high_cardinality(TAG_HOST));
        assert_eq!(metadata.get_tag_values(TAG_HOST).unwrap().len(), 1001);

        metadata.finalize_tag_cardinality();

        assert!(metadata.is_high_cardinality(TAG_HOST));
        assert!(metadata.get_tag_values(TAG_HOST).is_none());
    }

    #[test]
    fn test_service_names_convenience() {
        let metadata = MetricsSplitMetadata::builder()
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .add_low_cardinality_tag(TAG_SERVICE, "web")
            .add_low_cardinality_tag(TAG_SERVICE, "api")
            .build();

        let services = metadata.service_names().unwrap();
        assert_eq!(services.len(), 2);
        assert!(services.contains("web"));
        assert!(services.contains("api"));
    }

    #[test]
    fn test_metrics_split_state() {
        assert_eq!(MetricsSplitState::Staged.as_str(), "Staged");
        assert_eq!(MetricsSplitState::Published.as_str(), "Published");
        assert_eq!(
            MetricsSplitState::MarkedForDeletion.as_str(),
            "MarkedForDeletion"
        );
        assert_eq!(format!("{}", MetricsSplitState::Published), "Published");
    }
}
