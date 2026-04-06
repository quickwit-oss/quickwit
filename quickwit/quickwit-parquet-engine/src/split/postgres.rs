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

//! PostgreSQL model for metrics_splits table.
//!
//! This module provides the database model and conversion logic for storing
//! MetricsSplitMetadata in the Postgres metastore for Tier 1 pruning.

use std::collections::{HashMap, HashSet};

#[cfg(feature = "postgres")]
use sea_query::Iden;

use super::metadata::{
    MetricsSplitMetadata, MetricsSplitState, TAG_DATACENTER, TAG_ENV, TAG_HOST, TAG_REGION,
    TAG_SERVICE,
};

/// Sea-query table identifier for metrics_splits.
#[cfg_attr(feature = "postgres", derive(Iden))]
#[derive(Clone, Copy)]
pub enum MetricsSplits {
    Table,
    SplitId,
    SplitState,
    IndexId,
    TimeRangeStart,
    TimeRangeEnd,
    MetricNames,
    TagService,
    TagEnv,
    TagDatacenter,
    TagRegion,
    TagHost,
    HighCardinalityTagKeys,
    NumRows,
    SizeBytes,
    SplitMetadataJson,
    CreateTimestamp,
    UpdateTimestamp,
}

/// PostgreSQL row model for metrics_splits table.
/// Used for reading rows from the database.
#[derive(Debug, Clone)]
pub struct PgMetricsSplit {
    pub split_id: String,
    pub split_state: String,
    pub index_uid: String,
    pub time_range_start: i64,
    pub time_range_end: i64,
    pub metric_names: Vec<String>,
    pub tag_service: Option<Vec<String>>,
    pub tag_env: Option<Vec<String>>,
    pub tag_datacenter: Option<Vec<String>>,
    pub tag_region: Option<Vec<String>>,
    pub tag_host: Option<Vec<String>>,
    pub high_cardinality_tag_keys: Vec<String>,
    pub num_rows: i64,
    pub size_bytes: i64,
    pub split_metadata_json: String,
    pub update_timestamp: i64,
    pub window_start: Option<i64>,
    pub window_duration_secs: Option<i32>,
    pub sort_fields: String,
    pub num_merge_ops: i32,
    pub row_keys: Option<Vec<u8>>,
    pub zonemap_regexes: serde_json::Value,
}

/// Insertable row for metrics_splits table.
/// Used for writing rows to the database.
#[derive(Debug, Clone)]
pub struct InsertableMetricsSplit {
    pub split_id: String,
    pub split_state: String,
    pub index_uid: String,
    pub time_range_start: i64,
    pub time_range_end: i64,
    pub metric_names: Vec<String>,
    pub tag_service: Option<Vec<String>>,
    pub tag_env: Option<Vec<String>>,
    pub tag_datacenter: Option<Vec<String>>,
    pub tag_region: Option<Vec<String>>,
    pub tag_host: Option<Vec<String>>,
    pub high_cardinality_tag_keys: Vec<String>,
    pub num_rows: i64,
    pub size_bytes: i64,
    pub split_metadata_json: String,
    pub window_start: Option<i64>,
    pub window_duration_secs: i32,
    pub sort_fields: String,
    pub num_merge_ops: i32,
    pub row_keys: Option<Vec<u8>>,
    pub zonemap_regexes: serde_json::Value,
}

impl InsertableMetricsSplit {
    /// Convert MetricsSplitMetadata to an insertable row.
    pub fn from_metadata(
        metadata: &MetricsSplitMetadata,
        state: MetricsSplitState,
    ) -> Result<Self, serde_json::Error> {
        let split_metadata_json = serde_json::to_string(metadata)?;

        Ok(Self {
            split_id: metadata.split_id.as_str().to_string(),
            split_state: state.as_str().to_string(),
            index_uid: metadata.index_uid.clone(),
            time_range_start: metadata.time_range.start_secs as i64,
            time_range_end: metadata.time_range.end_secs as i64,
            metric_names: metadata.metric_names.iter().cloned().collect(),
            tag_service: extract_tag_values(&metadata.low_cardinality_tags, TAG_SERVICE),
            tag_env: extract_tag_values(&metadata.low_cardinality_tags, TAG_ENV),
            tag_datacenter: extract_tag_values(&metadata.low_cardinality_tags, TAG_DATACENTER),
            tag_region: extract_tag_values(&metadata.low_cardinality_tags, TAG_REGION),
            tag_host: extract_tag_values(&metadata.low_cardinality_tags, TAG_HOST),
            high_cardinality_tag_keys: metadata.high_cardinality_tag_keys.iter().cloned().collect(),
            num_rows: metadata.num_rows as i64,
            size_bytes: metadata.size_bytes as i64,
            split_metadata_json,
            window_start: metadata.window_start(),
            window_duration_secs: metadata.window_duration_secs() as i32,
            sort_fields: metadata.sort_fields.clone(),
            num_merge_ops: metadata.num_merge_ops as i32,
            row_keys: metadata.row_keys_proto.clone(),
            zonemap_regexes: serde_json::to_value(&metadata.zonemap_regexes)
                .unwrap_or_else(|_| serde_json::json!({})),
        })
    }
}

/// Extract tag values for a specific key, returning None if key doesn't exist.
fn extract_tag_values(tags: &HashMap<String, HashSet<String>>, key: &str) -> Option<Vec<String>> {
    tags.get(key).map(|values| values.iter().cloned().collect())
}

impl PgMetricsSplit {
    /// Convert database row to MetricsSplitMetadata.
    /// Falls back to deserializing from JSON if row data is incomplete.
    pub fn to_metadata(&self) -> Result<MetricsSplitMetadata, serde_json::Error> {
        // Primary path: deserialize from JSON (authoritative)
        let metadata: MetricsSplitMetadata = serde_json::from_str(&self.split_metadata_json)?;

        // SS-5: Verify consistency between JSON blob and SQL columns.
        debug_assert_eq!(metadata.split_id.as_str(), self.split_id);
        debug_assert_eq!(metadata.time_range.start_secs, self.time_range_start as u64);
        debug_assert_eq!(metadata.time_range.end_secs, self.time_range_end as u64);
        debug_assert_eq!(metadata.window_start(), self.window_start);
        debug_assert_eq!(
            metadata.window_duration_secs(),
            self.window_duration_secs.unwrap_or(0) as u32
        );
        debug_assert_eq!(metadata.sort_fields, self.sort_fields);
        debug_assert_eq!(metadata.num_merge_ops, self.num_merge_ops as u32);

        Ok(metadata)
    }

    /// Parse the split state from the database string.
    pub fn split_state(&self) -> Option<MetricsSplitState> {
        match self.split_state.as_str() {
            "Staged" => Some(MetricsSplitState::Staged),
            "Published" => Some(MetricsSplitState::Published),
            "MarkedForDeletion" => Some(MetricsSplitState::MarkedForDeletion),
            _ => None,
        }
    }
}

/// A complete metrics split record from the database.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricsSplitRecord {
    /// The split's state.
    pub state: MetricsSplitState,
    /// Timestamp when the split was last updated.
    pub update_timestamp: i64,
    /// The split's immutable metadata.
    pub metadata: MetricsSplitMetadata,
}

impl TryFrom<PgMetricsSplit> for MetricsSplitRecord {
    type Error = String;

    fn try_from(row: PgMetricsSplit) -> Result<Self, Self::Error> {
        let state = row
            .split_state()
            .ok_or_else(|| format!("unknown split state: {}", row.split_state))?;
        let metadata = row
            .to_metadata()
            .map_err(|e| format!("failed to deserialize metadata: {}", e))?;

        Ok(Self {
            state,
            update_timestamp: row.update_timestamp,
            metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::split::metadata::{MetricsSplitMetadata, SplitId, TimeRange};

    #[test]
    fn test_insertable_from_metadata() {
        let metadata = MetricsSplitMetadata::builder()
            .split_id(SplitId::new("test-split-001"))
            .index_uid("otel-metrics-v0_1:00000000000000000000000000")
            .time_range(TimeRange::new(1700000000, 1700003600))
            .num_rows(50000)
            .size_bytes(1024 * 1024)
            .add_metric_name("cpu.usage")
            .add_metric_name("memory.used")
            .add_low_cardinality_tag(TAG_SERVICE, "web")
            .add_low_cardinality_tag(TAG_SERVICE, "api")
            .add_low_cardinality_tag(TAG_ENV, "prod")
            .add_high_cardinality_tag_key(TAG_HOST)
            .build();

        let insertable =
            InsertableMetricsSplit::from_metadata(&metadata, MetricsSplitState::Staged)
                .expect("conversion should succeed");

        assert_eq!(insertable.split_id, "test-split-001");
        assert_eq!(insertable.split_state, "Staged");
        assert_eq!(
            insertable.index_uid,
            "otel-metrics-v0_1:00000000000000000000000000"
        );
        assert_eq!(insertable.time_range_start, 1700000000);
        assert_eq!(insertable.time_range_end, 1700003600);
        assert_eq!(insertable.metric_names.len(), 2);
        assert!(insertable.metric_names.contains(&"cpu.usage".to_string()));
        assert_eq!(insertable.tag_service.as_ref().unwrap().len(), 2);
        assert_eq!(insertable.tag_env.as_ref().unwrap().len(), 1);
        assert!(insertable.tag_datacenter.is_none());
        assert_eq!(insertable.high_cardinality_tag_keys, vec!["host"]);
        assert_eq!(insertable.num_rows, 50000);
        assert_eq!(insertable.size_bytes, 1024 * 1024);
    }

    #[test]
    fn test_insertable_from_metadata_with_compaction_fields() {
        let metadata = MetricsSplitMetadata::builder()
            .split_id(SplitId::new("compaction-test"))
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .num_rows(100)
            .size_bytes(500)
            .window_start_secs(1700000000)
            .window_duration_secs(3600)
            .sort_fields("metric_name|host|timestamp/V2")
            .num_merge_ops(2)
            .row_keys_proto(vec![0x08, 0x01])
            .add_zonemap_regex("metric_name", "cpu\\..*")
            .build();

        let insertable =
            InsertableMetricsSplit::from_metadata(&metadata, MetricsSplitState::Published)
                .expect("conversion should succeed");

        assert_eq!(insertable.window_start, Some(1700000000));
        assert_eq!(insertable.window_duration_secs, 3600);
        assert_eq!(insertable.sort_fields, "metric_name|host|timestamp/V2");
        assert_eq!(insertable.num_merge_ops, 2);
        assert_eq!(insertable.row_keys, Some(vec![0x08, 0x01]));
        assert!(insertable.zonemap_regexes.is_object());
        assert_eq!(
            insertable.zonemap_regexes["metric_name"],
            serde_json::json!("cpu\\..*")
        );
    }

    #[test]
    fn test_insertable_from_metadata_pre_phase31_defaults() {
        let metadata = MetricsSplitMetadata::builder()
            .split_id(SplitId::new("pre-phase31"))
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .build();

        let insertable =
            InsertableMetricsSplit::from_metadata(&metadata, MetricsSplitState::Staged)
                .expect("conversion should succeed");

        assert!(insertable.window_start.is_none());
        assert_eq!(
            insertable.window_duration_secs, 0,
            "pre-Phase-31 splits should have 0 window_duration_secs"
        );
        assert_eq!(insertable.sort_fields, "");
        assert_eq!(insertable.num_merge_ops, 0);
        assert!(insertable.row_keys.is_none());
        assert_eq!(insertable.zonemap_regexes, serde_json::json!({}));
    }

    #[test]
    fn test_pg_split_to_metadata_roundtrip() {
        let original = MetricsSplitMetadata::builder()
            .split_id(SplitId::new("roundtrip-test"))
            .index_uid("test-index:00000000000000000000000000")
            .time_range(TimeRange::new(1000, 2000))
            .num_rows(100)
            .size_bytes(500)
            .add_metric_name("test.metric")
            .add_low_cardinality_tag(TAG_SERVICE, "test-service")
            .build();

        let insertable =
            InsertableMetricsSplit::from_metadata(&original, MetricsSplitState::Published)
                .expect("conversion should succeed");

        let pg_row = PgMetricsSplit {
            split_id: insertable.split_id,
            split_state: insertable.split_state,
            index_uid: insertable.index_uid,
            time_range_start: insertable.time_range_start,
            time_range_end: insertable.time_range_end,
            metric_names: insertable.metric_names,
            tag_service: insertable.tag_service,
            tag_env: insertable.tag_env,
            tag_datacenter: insertable.tag_datacenter,
            tag_region: insertable.tag_region,
            tag_host: insertable.tag_host,
            high_cardinality_tag_keys: insertable.high_cardinality_tag_keys,
            num_rows: insertable.num_rows,
            size_bytes: insertable.size_bytes,
            split_metadata_json: insertable.split_metadata_json,
            update_timestamp: 1704067200,
            window_start: insertable.window_start,
            window_duration_secs: Some(insertable.window_duration_secs),
            sort_fields: insertable.sort_fields,
            num_merge_ops: insertable.num_merge_ops,
            row_keys: insertable.row_keys,
            zonemap_regexes: insertable.zonemap_regexes,
        };

        let recovered = pg_row.to_metadata().expect("should deserialize");
        assert_eq!(recovered.split_id.as_str(), original.split_id.as_str());
        assert_eq!(recovered.index_uid, original.index_uid);
        assert_eq!(recovered.time_range, original.time_range);
        assert_eq!(recovered.num_rows, original.num_rows);
    }
}
