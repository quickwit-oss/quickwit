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

//! Default table configuration for Parquet pipeline product types.
//!
//! When a table (index) does not specify an explicit sort fields configuration,
//! these defaults are applied based on the product type. The `ParquetWriter`
//! resolves these sort field names to physical `ParquetField` columns at
//! construction time; columns not yet in the schema (e.g., `timeseries_id`)
//! are recorded in metadata but skipped during physical sort.

use serde::{Deserialize, Serialize};

/// Product types supported by the Parquet pipeline.
///
/// Each product type has a default sort fields schema that matches the common
/// query predicates for that signal type. See ADR-002 for the rationale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProductType {
    Metrics,
    Logs,
    Traces,
}

impl ProductType {
    /// Default sort fields string for this product type.
    ///
    /// The metrics default includes `timeseries_id` as a tiebreaker before
    /// `timestamp_secs`. Since `timeseries_id` is not yet a physical column,
    /// the writer skips it during sort but records it in the metadata string.
    /// When the column is added to the schema, sorting will include it
    /// automatically.
    ///
    /// Logs and traces defaults are placeholders — they will be refined
    /// when the Parquet pipeline is extended to those signal types.
    pub fn default_sort_fields(self) -> &'static str {
        match self {
            Self::Metrics => "metric_name|tag_service|tag_env|tag_datacenter|tag_region|tag_host|timeseries_id|timestamp_secs/V2",
            // Placeholder: column names TBD when logs Parquet schema is defined.
            Self::Logs => "service_name|level|host|timestamp_secs/V2",
            // Placeholder: column names TBD when traces Parquet schema is defined.
            Self::Traces => "service_name|operation_name|trace_id|timestamp_secs/V2",
        }
    }
}

/// Table-level configuration for the Parquet pipeline.
///
/// Stored per-index. When `sort_fields` is `None`, the default for the
/// product type is used (see `ProductType::default_sort_fields()`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    /// The product type determines schema defaults.
    pub product_type: ProductType,

    /// Explicit sort fields override. When `None`, the product-type default
    /// is used. When `Some`, this exact schema string is applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_fields: Option<String>,

    /// Window duration in seconds for time-windowed compaction.
    /// Default: 900 (15 minutes). Must divide 3600.
    #[serde(default = "default_window_duration_secs")]
    pub window_duration_secs: u32,
}

fn default_window_duration_secs() -> u32 {
    900
}

impl TableConfig {
    /// Create a new TableConfig for the given product type with defaults.
    pub fn new(product_type: ProductType) -> Self {
        Self {
            product_type,
            sort_fields: None,
            window_duration_secs: default_window_duration_secs(),
        }
    }

    /// Get the effective sort fields string for this table.
    ///
    /// Returns the explicit override if set, otherwise the product-type default.
    pub fn effective_sort_fields(&self) -> &str {
        match &self.sort_fields {
            Some(sf) => sf.as_str(),
            None => self.product_type.default_sort_fields(),
        }
    }
}

impl Default for TableConfig {
    fn default() -> Self {
        Self::new(ProductType::Metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sort_fields::parse_sort_fields;

    #[test]
    fn test_metrics_default_sort_fields_parses() {
        let schema = parse_sort_fields(ProductType::Metrics.default_sort_fields())
            .expect("metrics default sort fields must parse");
        assert_eq!(schema.column.len(), 8);
        // Proto names are bare (suffixes stripped by parser).
        assert_eq!(schema.column[0].name, "metric_name");
        assert_eq!(schema.column[1].name, "tag_service");
        assert_eq!(schema.column[6].name, "timeseries_id");
        assert_eq!(schema.column[7].name, "timestamp_secs");
    }

    #[test]
    fn test_logs_default_sort_fields_parses() {
        let schema = parse_sort_fields(ProductType::Logs.default_sort_fields())
            .expect("logs default sort fields must parse");
        assert_eq!(schema.column.len(), 4);
        assert_eq!(schema.column[0].name, "service_name");
    }

    #[test]
    fn test_traces_default_sort_fields_parses() {
        let schema = parse_sort_fields(ProductType::Traces.default_sort_fields())
            .expect("traces default sort fields must parse");
        assert_eq!(schema.column.len(), 4);
        assert_eq!(schema.column[0].name, "service_name");
    }

    #[test]
    fn test_effective_sort_fields_uses_default() {
        let config = TableConfig::new(ProductType::Metrics);
        assert_eq!(
            config.effective_sort_fields(),
            ProductType::Metrics.default_sort_fields()
        );
    }

    #[test]
    fn test_effective_sort_fields_uses_override() {
        let mut config = TableConfig::new(ProductType::Metrics);
        config.sort_fields = Some("custom__s|timestamp/V2".to_string());
        assert_eq!(config.effective_sort_fields(), "custom__s|timestamp/V2");
    }

    #[test]
    fn test_default_window_duration() {
        let config = TableConfig::default();
        assert_eq!(config.window_duration_secs, 900);
    }

    #[test]
    fn test_table_config_serde_roundtrip() {
        let config = TableConfig::new(ProductType::Traces);
        let json = serde_json::to_string(&config).unwrap();
        let recovered: TableConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered.product_type, ProductType::Traces);
        assert!(recovered.sort_fields.is_none());
        assert_eq!(recovered.window_duration_secs, 900);
    }

    #[test]
    fn test_table_config_serde_with_override() {
        let mut config = TableConfig::new(ProductType::Metrics);
        config.sort_fields = Some("host__s|timestamp/V2".to_string());
        config.window_duration_secs = 3600;

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("host__s|timestamp/V2"));

        let recovered: TableConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(
            recovered.sort_fields.as_deref(),
            Some("host__s|timestamp/V2")
        );
        assert_eq!(recovered.window_duration_secs, 3600);
    }
}
