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

//! Unified parquet split metadata definitions for both metrics and sketch splits.

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

// Well-known tag key constants
pub const TAG_SERVICE: &str = "service";
pub const TAG_ENV: &str = "env";
pub const TAG_DATACENTER: &str = "datacenter";
pub const TAG_REGION: &str = "region";
pub const TAG_HOST: &str = "host";

/// Distinguishes between metrics and sketch parquet splits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ParquetSplitKind {
    #[default]
    Metrics,
    Sketches,
}

impl ParquetSplitKind {
    /// Returns the prefix used when generating split IDs.
    pub fn split_id_prefix(&self) -> &'static str {
        match self {
            Self::Metrics => "metrics_",
            Self::Sketches => "sketches_",
        }
    }

    /// Returns the Postgres table name for this split kind.
    pub fn table_name(&self) -> &'static str {
        match self {
            Self::Metrics => "metrics_splits",
            Self::Sketches => "sketch_splits",
        }
    }

    /// Returns a human-readable label for logging and metrics.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Metrics => "metrics",
            Self::Sketches => "sketch",
        }
    }
}

impl std::fmt::Display for ParquetSplitKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// Unique identifier for a parquet split (metrics or sketch).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ParquetSplitId(String);

impl ParquetSplitId {
    /// Create a new ParquetSplitId from a string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Generates a new unique split ID using a ULID (timestamp + randomness),
    /// prefixed according to the split kind.
    pub fn generate(kind: ParquetSplitKind) -> Self {
        Self(format!(
            "{}{}",
            kind.split_id_prefix(),
            ulid::Ulid::new().to_string().to_lowercase()
        ))
    }

    /// Get the string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ParquetSplitId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Time range covered by a split.
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

/// Metadata for a parquet split.
///
/// The `window` field stores the time window as `[start, start + duration)`.
/// For JSON serialization, it is decomposed into `window_start` and
/// `window_duration_secs` for backward compatibility with pre-Phase-31 code.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "ParquetSplitMetadataSerde", into = "ParquetSplitMetadataSerde")]
pub struct ParquetSplitMetadata {
    /// What kind of split this is (metrics or sketches).
    pub kind: ParquetSplitKind,

    /// Partition to which the split belongs.
    ///
    /// Computed from the index config's `partition_key` routing expression.
    /// Splits with different `partition_id` values should not be merged together.
    #[serde(default)]
    pub partition_id: u64,

    /// Unique split identifier.
    pub split_id: ParquetSplitId,

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

    /// Parquet file path relative to storage root.
    pub parquet_file: String,

    /// Time window as `[start, start + duration)` in epoch seconds.
    /// None for pre-Phase-31 splits (backward compat).
    pub window: Option<Range<i64>>,

    /// Sort schema as Husky-style string (e.g., "metric_name|host|timestamp/V2").
    /// Empty string for pre-Phase-31 splits.
    pub sort_fields: String,

    /// Number of merge operations this split has been through.
    /// 0 for newly ingested splits.
    pub num_merge_ops: u32,

    /// RowKeys (sort-key min/max boundaries) as serialized proto bytes
    /// (`sortschema::RowKeys` in `event_store_sortschema.proto`).
    /// None for pre-Phase-31 splits or splits without sort schema.
    pub row_keys_proto: Option<Vec<u8>>,

    /// Per-column zonemap regex strings, keyed by column name.
    /// Empty for pre-Phase-31 splits.
    pub zonemap_regexes: HashMap<String, String>,

    /// Number of leading sort schema columns whose transitions align with
    /// row group boundaries in the Parquet file.
    ///
    /// `0` = no alignment claimed (legacy default; safe for any boundaries).
    /// `1` = first sort column (e.g., `metric_name`) — RG boundaries match
    /// transitions in this column.
    /// `N` (1 ≤ N ≤ sort_schema.len()) = aligned with first `N` sort columns.
    /// A single-RG file vacuously satisfies any prefix; writers set `N` =
    /// sort schema length so the streaming reader's fast path applies.
    ///
    /// **Compaction scope**: only splits with the same value of this field
    /// merge together. Mixing prefix levels in a single merge is forbidden.
    #[serde(default)]
    pub rg_partition_prefix_len: u32,
}

/// Serde helper struct that uses `window_start` / `window_duration_secs` field
/// names for JSON backward compatibility while the in-memory representation uses
/// `Option<Range<i64>>`.
#[derive(Serialize, Deserialize)]
struct ParquetSplitMetadataSerde {
    #[serde(default)]
    kind: ParquetSplitKind,
    #[serde(default)]
    partition_id: u64,
    split_id: ParquetSplitId,
    index_uid: String,
    time_range: TimeRange,
    num_rows: u64,
    size_bytes: u64,
    metric_names: HashSet<String>,
    low_cardinality_tags: HashMap<String, HashSet<String>>,
    high_cardinality_tag_keys: HashSet<String>,
    created_at: SystemTime,

    #[serde(default)]
    parquet_file: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    window_start: Option<i64>,

    #[serde(default)]
    window_duration_secs: u32,

    #[serde(default)]
    sort_fields: String,

    #[serde(default)]
    num_merge_ops: u32,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    row_keys_proto: Option<Vec<u8>>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    zonemap_regexes: HashMap<String, String>,

    #[serde(default, skip_serializing_if = "is_zero_u32")]
    rg_partition_prefix_len: u32,
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

impl From<ParquetSplitMetadataSerde> for ParquetSplitMetadata {
    fn from(s: ParquetSplitMetadataSerde) -> Self {
        let window = match (s.window_start, s.window_duration_secs) {
            (Some(start), dur) if dur > 0 => Some(start..start + dur as i64),
            _ => None,
        };
        Self {
            kind: s.kind,
            partition_id: s.partition_id,
            split_id: s.split_id,
            index_uid: s.index_uid,
            time_range: s.time_range,
            num_rows: s.num_rows,
            size_bytes: s.size_bytes,
            metric_names: s.metric_names,
            low_cardinality_tags: s.low_cardinality_tags,
            high_cardinality_tag_keys: s.high_cardinality_tag_keys,
            created_at: s.created_at,
            parquet_file: s.parquet_file,
            window,
            sort_fields: s.sort_fields,
            num_merge_ops: s.num_merge_ops,
            row_keys_proto: s.row_keys_proto,
            zonemap_regexes: s.zonemap_regexes,
            rg_partition_prefix_len: s.rg_partition_prefix_len,
        }
    }
}

impl From<ParquetSplitMetadata> for ParquetSplitMetadataSerde {
    fn from(m: ParquetSplitMetadata) -> Self {
        let (window_start, window_duration_secs) = match &m.window {
            Some(w) => (Some(w.start), (w.end - w.start) as u32),
            None => (None, 0),
        };
        Self {
            kind: m.kind,
            partition_id: m.partition_id,
            split_id: m.split_id,
            index_uid: m.index_uid,
            time_range: m.time_range,
            num_rows: m.num_rows,
            size_bytes: m.size_bytes,
            metric_names: m.metric_names,
            low_cardinality_tags: m.low_cardinality_tags,
            high_cardinality_tag_keys: m.high_cardinality_tag_keys,
            created_at: m.created_at,
            parquet_file: m.parquet_file,
            window_start,
            window_duration_secs,
            sort_fields: m.sort_fields,
            num_merge_ops: m.num_merge_ops,
            row_keys_proto: m.row_keys_proto,
            zonemap_regexes: m.zonemap_regexes,
            rg_partition_prefix_len: m.rg_partition_prefix_len,
        }
    }
}

impl ParquetSplitMetadata {
    /// Returns the parquet filename for this split, relative to the storage root.
    /// Always `{split_id}.parquet`.
    pub fn parquet_filename(&self) -> String {
        format!("{}.parquet", self.split_id)
    }

    /// Returns the split ID as a string.
    pub fn split_id_str(&self) -> &str {
        self.split_id.as_str()
    }

    /// Returns the size in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Cardinality threshold for routing tags to Postgres vs Parquet.
    /// Tags with < CARDINALITY_THRESHOLD unique values go to Postgres.
    /// Tags with >= CARDINALITY_THRESHOLD unique values use Parquet bloom filters.
    pub const CARDINALITY_THRESHOLD: usize = 1000;

    /// Returns the window start in epoch seconds, or `None` for pre-Phase-31 splits.
    pub fn window_start(&self) -> Option<i64> {
        self.window.as_ref().map(|w| w.start)
    }

    /// Returns the window duration in seconds, or 0 for pre-Phase-31 splits.
    pub fn window_duration_secs(&self) -> u32 {
        match &self.window {
            Some(w) => (w.end - w.start) as u32,
            None => 0,
        }
    }

    /// Create a builder for metrics splits.
    pub fn metrics_builder() -> ParquetSplitMetadataBuilder {
        ParquetSplitMetadataBuilder::new(ParquetSplitKind::Metrics)
    }

    /// Create a builder for sketch splits.
    pub fn sketches_builder() -> ParquetSplitMetadataBuilder {
        ParquetSplitMetadataBuilder::new(ParquetSplitKind::Sketches)
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

/// Builder for ParquetSplitMetadata.
///
/// Use [`ParquetSplitMetadata::metrics_builder`] or
/// [`ParquetSplitMetadata::sketches_builder`] to create an instance.
pub struct ParquetSplitMetadataBuilder {
    kind: ParquetSplitKind,
    partition_id: u64,
    split_id: Option<ParquetSplitId>,
    index_uid: Option<String>,
    time_range: Option<TimeRange>,
    num_rows: u64,
    size_bytes: u64,
    metric_names: HashSet<String>,
    low_cardinality_tags: HashMap<String, HashSet<String>>,
    high_cardinality_tag_keys: HashSet<String>,
    parquet_file: String,
    window_start: Option<i64>,
    window_duration_secs: u32,
    sort_fields: String,
    num_merge_ops: u32,
    row_keys_proto: Option<Vec<u8>>,
    zonemap_regexes: HashMap<String, String>,
    rg_partition_prefix_len: u32,
}

// The builder still accepts window_start and window_duration_secs separately
// to remain compatible with callers that compute them independently (e.g.,
// split_writer). The `build()` method fuses them into `Option<Range<i64>>`.

impl ParquetSplitMetadataBuilder {
    fn new(kind: ParquetSplitKind) -> Self {
        Self {
            kind,
            partition_id: 0,
            split_id: None,
            index_uid: None,
            time_range: None,
            num_rows: 0,
            size_bytes: 0,
            metric_names: HashSet::new(),
            low_cardinality_tags: HashMap::new(),
            high_cardinality_tag_keys: HashSet::new(),
            parquet_file: String::new(),
            window_start: None,
            window_duration_secs: 0,
            sort_fields: String::new(),
            num_merge_ops: 0,
            row_keys_proto: None,
            zonemap_regexes: HashMap::new(),
            rg_partition_prefix_len: 0,
        }
    }

    pub fn partition_id(mut self, partition_id: u64) -> Self {
        self.partition_id = partition_id;
        self
    }

    pub fn split_id(mut self, id: ParquetSplitId) -> Self {
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

    pub fn parquet_file(mut self, path: impl Into<String>) -> Self {
        self.parquet_file = path.into();
        self
    }

    pub fn window_start_secs(mut self, epoch_secs: i64) -> Self {
        self.window_start = Some(epoch_secs);
        self
    }

    pub fn window_duration_secs(mut self, dur: u32) -> Self {
        self.window_duration_secs = dur;
        self
    }

    pub fn sort_fields(mut self, schema: impl Into<String>) -> Self {
        self.sort_fields = schema.into();
        self
    }

    pub fn num_merge_ops(mut self, ops: u32) -> Self {
        self.num_merge_ops = ops;
        self
    }

    pub fn row_keys_proto(mut self, bytes: Vec<u8>) -> Self {
        self.row_keys_proto = Some(bytes);
        self
    }

    pub fn add_zonemap_regex(
        mut self,
        column: impl Into<String>,
        regex: impl Into<String>,
    ) -> Self {
        self.zonemap_regexes.insert(column.into(), regex.into());
        self
    }

    /// Set the row group partition prefix length.
    ///
    /// `0` (default) means no alignment claim. Higher values mean RG
    /// boundaries align with the first `N` sort schema columns. See
    /// [`ParquetSplitMetadata::rg_partition_prefix_len`].
    pub fn rg_partition_prefix_len(mut self, prefix_len: u32) -> Self {
        self.rg_partition_prefix_len = prefix_len;
        self
    }

    pub fn build(self) -> ParquetSplitMetadata {
        // TW-2 (ADR-003): window_duration must evenly divide 3600.
        // Enforced at build time so no invalid metadata propagates to storage.
        quickwit_dst::check_invariant!(
            quickwit_dst::invariants::InvariantId::TW2,
            self.window_duration_secs == 0
                || quickwit_dst::invariants::window::is_valid_window_duration(
                    self.window_duration_secs
                ),
            ": window_duration_secs={} does not divide 3600",
            self.window_duration_secs
        );

        // TW-1 (ADR-003, partial): window_start and window_duration_secs are paired.
        // If one is set, the other must be too. Pre-Phase-31 splits have both at defaults.
        quickwit_dst::check_invariant!(
            quickwit_dst::invariants::InvariantId::TW1,
            (self.window_start.is_none() && self.window_duration_secs == 0)
                || (self.window_start.is_some() && self.window_duration_secs > 0),
            ": window_start and window_duration_secs must be set together (window_start={:?}, \
             window_duration_secs={})",
            self.window_start,
            self.window_duration_secs
        );

        // Fuse the two builder fields into a single Range.
        let window = match (self.window_start, self.window_duration_secs) {
            (Some(start), dur) if dur > 0 => Some(start..start + dur as i64),
            _ => None,
        };

        ParquetSplitMetadata {
            kind: self.kind,
            partition_id: self.partition_id,
            split_id: self
                .split_id
                .unwrap_or_else(|| ParquetSplitId::generate(self.kind)),
            index_uid: self.index_uid.expect("index_uid is required"),
            time_range: self.time_range.expect("time_range is required"),
            num_rows: self.num_rows,
            size_bytes: self.size_bytes,
            metric_names: self.metric_names,
            low_cardinality_tags: self.low_cardinality_tags,
            high_cardinality_tag_keys: self.high_cardinality_tag_keys,
            created_at: SystemTime::now(),
            parquet_file: self.parquet_file,
            window,
            sort_fields: self.sort_fields,
            num_merge_ops: self.num_merge_ops,
            row_keys_proto: self.row_keys_proto,
            zonemap_regexes: self.zonemap_regexes,
            rg_partition_prefix_len: self.rg_partition_prefix_len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_id_generation() {
        let id1 = ParquetSplitId::generate(ParquetSplitKind::Metrics);
        // Sleep 1ms to ensure different timestamp
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = ParquetSplitId::generate(ParquetSplitKind::Metrics);
        assert_ne!(id1.as_str(), id2.as_str());
        assert!(id1.as_str().starts_with("metrics_"));
    }

    #[test]
    fn test_sketch_split_id_generation() {
        let id = ParquetSplitId::generate(ParquetSplitKind::Sketches);
        assert!(id.as_str().starts_with("sketches_"));
    }

    #[test]
    fn test_sketch_split_id_uniqueness() {
        let id1 = ParquetSplitId::generate(ParquetSplitKind::Sketches);
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = ParquetSplitId::generate(ParquetSplitKind::Sketches);
        assert_ne!(id1.as_str(), id2.as_str());
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
        let metadata = ParquetSplitMetadata::metrics_builder()
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .add_metric_name("cpu.usage")
            .add_low_cardinality_tag(TAG_SERVICE, "web")
            .add_low_cardinality_tag(TAG_SERVICE, "api")
            .add_low_cardinality_tag(TAG_ENV, "prod")
            .add_high_cardinality_tag_key(TAG_HOST)
            .build();

        assert_eq!(metadata.kind, ParquetSplitKind::Metrics);
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
    fn test_sketch_metadata_builder() {
        let metadata = ParquetSplitMetadata::sketches_builder()
            .index_uid("sketch-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .add_metric_name("req.latency")
            .add_low_cardinality_tag(TAG_SERVICE, "api")
            .build();

        assert_eq!(metadata.kind, ParquetSplitKind::Sketches);
        assert!(metadata.split_id.as_str().starts_with("sketches_"));
        assert!(metadata.metric_names.contains("req.latency"));
        assert_eq!(metadata.time_range.start_secs, 1000);
    }

    #[test]
    fn test_sketch_metadata_serialization_roundtrip() {
        let metadata = ParquetSplitMetadata::sketches_builder()
            .index_uid("test:00000000000000000000000000")
            .time_range(TimeRange::new(100, 200))
            .num_rows(500)
            .size_bytes(1024)
            .add_metric_name("latency")
            .build();

        let json = serde_json::to_string(&metadata).unwrap();
        let recovered: ParquetSplitMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered.split_id.as_str(), metadata.split_id.as_str());
        assert_eq!(recovered.num_rows, 500);
        assert_eq!(recovered.kind, ParquetSplitKind::Sketches);
    }

    #[test]
    fn test_backwards_compatible_deserialization() {
        // Simulate JSON without the "kind" field (old format)
        let json = r#"{
            "split_id": "metrics_test123",
            "index_uid": "test:00000000000000000000000000",
            "time_range": {"start_secs": 100, "end_secs": 200},
            "num_rows": 42,
            "size_bytes": 1024,
            "metric_names": [],
            "low_cardinality_tags": {},
            "high_cardinality_tag_keys": [],
            "created_at": {"secs_since_epoch": 0, "nanos_since_epoch": 0}
        }"#;
        let recovered: ParquetSplitMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(recovered.kind, ParquetSplitKind::Metrics);
    }

    #[test]
    fn test_cardinality_promotion() {
        let mut metadata = ParquetSplitMetadata::metrics_builder()
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
        let metadata = ParquetSplitMetadata::metrics_builder()
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
    fn test_split_kind_properties() {
        assert_eq!(ParquetSplitKind::Metrics.split_id_prefix(), "metrics_");
        assert_eq!(ParquetSplitKind::Sketches.split_id_prefix(), "sketches_");
        assert_eq!(ParquetSplitKind::Metrics.table_name(), "metrics_splits");
        assert_eq!(ParquetSplitKind::Sketches.table_name(), "sketch_splits");
        assert_eq!(ParquetSplitKind::Metrics.label(), "metrics");
        assert_eq!(ParquetSplitKind::Sketches.label(), "sketch");
    }

    #[test]
    fn test_backward_compat_deserialize_pre_phase31_json() {
        // Simulate a JSON string from pre-Phase-31 code (no compaction fields).
        let pre_phase31_json = r#"{
            "split_id": "metrics_abc123",
            "index_uid": "test-index:00000000000000000000000000",
            "time_range": {"start_secs": 1000, "end_secs": 2000},
            "num_rows": 500,
            "size_bytes": 1024,
            "metric_names": ["cpu.usage"],
            "low_cardinality_tags": {},
            "high_cardinality_tag_keys": [],
            "created_at": {"secs_since_epoch": 1700000000, "nanos_since_epoch": 0},
            "parquet_file": "split1.parquet"
        }"#;

        let metadata: ParquetSplitMetadata =
            serde_json::from_str(pre_phase31_json).expect("should deserialize pre-Phase-31 JSON");

        // New fields should be at their defaults.
        assert!(metadata.window.is_none());
        assert!(metadata.window_start().is_none());
        assert_eq!(metadata.window_duration_secs(), 0);
        assert_eq!(metadata.sort_fields, "");
        assert_eq!(metadata.num_merge_ops, 0);
        assert!(metadata.row_keys_proto.is_none());
        assert!(metadata.zonemap_regexes.is_empty());

        // Existing fields should be intact.
        assert_eq!(metadata.split_id.as_str(), "metrics_abc123");
        assert_eq!(metadata.index_uid, "test-index:00000000000000000000000000");
        assert_eq!(metadata.num_rows, 500);
    }

    #[test]
    fn test_round_trip_with_compaction_fields() {
        let metadata = ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new("roundtrip-compaction"))
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .num_rows(100)
            .size_bytes(500)
            .window_start_secs(1700000000)
            .window_duration_secs(3600)
            .sort_fields("metric_name|host|timestamp/V2")
            .num_merge_ops(3)
            .row_keys_proto(vec![0x08, 0x01, 0x10, 0x02])
            .add_zonemap_regex("metric_name", "cpu\\..*")
            .add_zonemap_regex("host", "host-\\d+")
            .build();

        let json = serde_json::to_string(&metadata).expect("should serialize");
        let recovered: ParquetSplitMetadata =
            serde_json::from_str(&json).expect("should deserialize");

        assert_eq!(recovered.window, Some(1700000000..1700003600));
        assert_eq!(recovered.window_start(), Some(1700000000));
        assert_eq!(recovered.window_duration_secs(), 3600);
        assert_eq!(recovered.sort_fields, "metric_name|host|timestamp/V2");
        assert_eq!(recovered.num_merge_ops, 3);
        assert_eq!(recovered.row_keys_proto, Some(vec![0x08, 0x01, 0x10, 0x02]));
        assert_eq!(recovered.zonemap_regexes.len(), 2);
        assert_eq!(
            recovered.zonemap_regexes.get("metric_name").unwrap(),
            "cpu\\..*"
        );
        assert_eq!(recovered.zonemap_regexes.get("host").unwrap(), "host-\\d+");
    }

    #[test]
    fn test_rg_partition_prefix_len_default_zero() {
        let metadata = ParquetSplitMetadata::metrics_builder()
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .build();
        assert_eq!(metadata.rg_partition_prefix_len, 0);
    }

    #[test]
    fn test_rg_partition_prefix_len_round_trip() {
        let metadata = ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new("prefix-roundtrip"))
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .rg_partition_prefix_len(3)
            .build();

        let json = serde_json::to_string(&metadata).expect("serialize");
        let recovered: ParquetSplitMetadata = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(recovered.rg_partition_prefix_len, 3);
    }

    #[test]
    fn test_rg_partition_prefix_len_absent_in_legacy_json() {
        // JSON predating the field entirely should deserialize as 0.
        let legacy_json = r#"{
            "split_id": "metrics_legacy",
            "index_uid": "test-index:00000000000000000000000000",
            "time_range": {"start_secs": 100, "end_secs": 200},
            "num_rows": 1,
            "size_bytes": 1,
            "metric_names": [],
            "low_cardinality_tags": {},
            "high_cardinality_tag_keys": [],
            "created_at": {"secs_since_epoch": 0, "nanos_since_epoch": 0}
        }"#;
        let metadata: ParquetSplitMetadata =
            serde_json::from_str(legacy_json).expect("legacy JSON should deserialize");
        assert_eq!(metadata.rg_partition_prefix_len, 0);
    }

    #[test]
    fn test_rg_partition_prefix_len_zero_omitted_from_json() {
        let metadata = ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new("prefix-zero"))
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .build();
        let json = serde_json::to_string(&metadata).expect("serialize");
        assert!(
            !json.contains("rg_partition_prefix_len"),
            "default value 0 should be omitted from JSON to keep payloads compact"
        );
    }

    #[test]
    fn test_skip_serializing_empty_compaction_fields() {
        let metadata = ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new("skip-test"))
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .build();

        let json = serde_json::to_string(&metadata).expect("should serialize");

        // Optional fields with skip_serializing_if should be absent.
        assert!(
            !json.contains("\"window_start\""),
            "window_start should not appear when None"
        );
        assert!(
            !json.contains("\"row_keys_proto\""),
            "row_keys_proto should not appear when None"
        );
        assert!(
            !json.contains("\"zonemap_regexes\""),
            "zonemap_regexes should not appear when empty"
        );
    }
}
