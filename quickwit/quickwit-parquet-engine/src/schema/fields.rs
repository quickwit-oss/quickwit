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

//! Parquet field definitions with column metadata, sort order constants, and validation.

use anyhow::{Result, bail};
use arrow::datatypes::{DataType, Field, Fields};
use parquet::variant::VariantType;

/// Required field names that must exist in every batch.
pub const REQUIRED_FIELDS: &[&str] = &["metric_name", "metric_type", "timestamp_secs", "value"];

/// Sort order column names. Columns not present in a batch are skipped.
pub const SORT_ORDER: &[&str] = &[
    "metric_name",
    "service",
    "env",
    "datacenter",
    "region",
    "host",
    "timeseries_id",
    "timestamp_secs",
];

/// All fields in the parquet schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParquetField {
    MetricName,
    MetricType,
    MetricUnit,
    TimestampSecs,
    StartTimestampSecs,
    Value,
    TimeseriesId,
    TagService,
    TagEnv,
    TagDatacenter,
    TagRegion,
    TagHost,
    Attributes,
    ServiceName,
    ResourceAttributes,
}

impl ParquetField {
    /// Field name as stored in Parquet.
    pub fn name(&self) -> &'static str {
        match self {
            Self::MetricName => "metric_name",
            Self::MetricType => "metric_type",
            Self::MetricUnit => "metric_unit",
            Self::TimestampSecs => "timestamp_secs",
            Self::StartTimestampSecs => "start_timestamp_secs",
            Self::Value => "value",
            Self::TimeseriesId => "timeseries_id",
            Self::TagService => "tag_service",
            Self::TagEnv => "tag_env",
            Self::TagDatacenter => "tag_datacenter",
            Self::TagRegion => "tag_region",
            Self::TagHost => "tag_host",
            Self::Attributes => "attributes",
            Self::ServiceName => "service_name",
            Self::ResourceAttributes => "resource_attributes",
        }
    }

    /// Whether this field is nullable.
    pub fn nullable(&self) -> bool {
        matches!(
            self,
            Self::MetricUnit
                | Self::StartTimestampSecs
                | Self::TagService
                | Self::TagEnv
                | Self::TagDatacenter
                | Self::TagRegion
                | Self::TagHost
                | Self::Attributes
                | Self::ResourceAttributes
        )
    }

    /// Arrow DataType for this field.
    /// Use dictionary encoding for high-cardinality strings.
    pub fn arrow_type(&self) -> DataType {
        match self {
            // Dictionary-encoded strings for high cardinality
            Self::MetricName | Self::ServiceName => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
            // Dictionary-encoded optional tags
            Self::TagService
            | Self::TagEnv
            | Self::TagDatacenter
            | Self::TagRegion
            | Self::TagHost => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
            // Enum stored as UInt8
            Self::MetricType => DataType::UInt8,
            // Timestamps as UInt64 seconds
            Self::TimestampSecs | Self::StartTimestampSecs => DataType::UInt64,
            // Metric value
            Self::Value => DataType::Float64,
            // Deterministic hash of timeseries identity columns
            Self::TimeseriesId => DataType::Int64,
            // Plain string for metric unit
            Self::MetricUnit => DataType::Utf8,
            // VARIANT type for semi-structured attributes
            // Uses the Parquet Variant binary encoding format
            Self::Attributes | Self::ResourceAttributes => {
                // VARIANT is stored as a struct with metadata and value BinaryView fields
                // VariantArrayBuilder produces BinaryView, not Binary
                DataType::Struct(Fields::from(vec![
                    Field::new("metadata", DataType::BinaryView, false),
                    Field::new("value", DataType::BinaryView, false),
                ]))
            }
        }
    }

    /// Convert to Arrow Field.
    pub fn to_arrow_field(&self) -> Field {
        let field = Field::new(self.name(), self.arrow_type(), self.nullable());

        // Add VARIANT extension type metadata for attributes fields
        match self {
            Self::Attributes | Self::ResourceAttributes => field.with_extension_type(VariantType),
            _ => field,
        }
    }

    /// All fields in schema order.
    pub fn all() -> &'static [ParquetField] {
        &[
            Self::MetricName,
            Self::MetricType,
            Self::MetricUnit,
            Self::TimestampSecs,
            Self::StartTimestampSecs,
            Self::Value,
            Self::TimeseriesId,
            Self::TagService,
            Self::TagEnv,
            Self::TagDatacenter,
            Self::TagRegion,
            Self::TagHost,
            Self::Attributes,
            Self::ServiceName,
            Self::ResourceAttributes,
        ]
    }

    /// Get the column index in the schema.
    pub fn column_index(&self) -> usize {
        Self::all().iter().position(|f| f == self).unwrap()
    }

    /// Look up a ParquetField by its Parquet column name.
    ///
    /// Used by the sort fields resolver to map sort schema column names
    /// to physical schema columns.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::all().iter().find(|f| f.name() == name).copied()
    }
}

/// Arrow type for required fields by name.
pub fn required_field_type(name: &str) -> Option<DataType> {
    match name {
        "metric_name" => Some(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        )),
        "metric_type" => Some(DataType::UInt8),
        "timestamp_secs" => Some(DataType::UInt64),
        "value" => Some(DataType::Float64),
        _ => None,
    }
}

/// Validate that a batch schema contains all required fields with correct types.
pub fn validate_required_fields(schema: &arrow::datatypes::Schema) -> Result<()> {
    for &name in REQUIRED_FIELDS {
        match schema.index_of(name) {
            Ok(idx) => {
                let expected_type = required_field_type(name).unwrap();
                let actual_type = schema.field(idx).data_type();
                if *actual_type != expected_type {
                    bail!(
                        "field '{}' has type {:?}, expected {:?}",
                        name,
                        actual_type,
                        expected_type
                    );
                }
            }
            Err(_) => bail!("missing required field '{}'", name),
        }
    }
    Ok(())
}
