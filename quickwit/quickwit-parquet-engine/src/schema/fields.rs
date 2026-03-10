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

//! Parquet field definitions with column metadata.

use arrow::datatypes::{DataType, Field, Fields};
use parquet::variant::VariantType;

/// All fields in the parquet schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParquetField {
    MetricName,
    MetricType,
    MetricUnit,
    TimestampSecs,
    StartTimestampSecs,
    Value,
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

    /// Sort order for metrics data (used for pruning).
    /// Order: metric_name, common tags (service, env, datacenter, region, host), timestamp.
    pub fn sort_order() -> &'static [ParquetField] {
        &[
            Self::MetricName,
            Self::TagService,
            Self::TagEnv,
            Self::TagDatacenter,
            Self::TagRegion,
            Self::TagHost,
            Self::TimestampSecs,
        ]
    }

    /// Get the column index in the schema.
    pub fn column_index(&self) -> usize {
        Self::all().iter().position(|f| f == self).unwrap()
    }
}
