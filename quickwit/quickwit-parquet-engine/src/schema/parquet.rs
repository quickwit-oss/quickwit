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

//! Parquet schema construction for metrics.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, SchemaRef};
use parquet::variant::VariantType;

/// The fixed 14-field schema from before dynamic schema support was added.
///
/// TODO: Preserved for backwards compatibility with callers (e.g. quickwit-datafusion)
/// that need a concrete schema reference. New Parquet files written by this
/// pipeline use a dynamic schema derived from the data; this schema describes
/// the old fixed layout.
fn legacy_fixed_schema() -> SchemaRef {
    let variant_struct = DataType::Struct(Fields::from(vec![
        Field::new("metadata", DataType::BinaryView, false),
        Field::new("value", DataType::BinaryView, false),
    ]));
    let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

    Arc::new(ArrowSchema::new(vec![
        Field::new("metric_name", dict.clone(), false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("start_timestamp_secs", DataType::UInt64, true),
        Field::new("value", DataType::Float64, false),
        Field::new("tag_service", dict.clone(), true),
        Field::new("tag_env", dict.clone(), true),
        Field::new("tag_datacenter", dict.clone(), true),
        Field::new("tag_region", dict.clone(), true),
        Field::new("tag_host", dict.clone(), true),
        Field::new("attributes", variant_struct.clone(), true).with_extension_type(VariantType),
        Field::new("service_name", dict, false),
        Field::new("resource_attributes", variant_struct, true).with_extension_type(VariantType),
    ]))
}

/// Parquet schema for storage.
#[derive(Debug, Clone)]
pub struct ParquetSchema {
    arrow_schema: SchemaRef,
}

impl ParquetSchema {
    /// Returns the fixed 14-field legacy schema.
    ///
    /// Preserved for backwards compatibility. The pipeline now writes dynamic
    /// schemas derived from the data; prefer `from_arrow_schema` when you have
    /// a batch schema available.
    pub fn new() -> Self {
        Self {
            arrow_schema: legacy_fixed_schema(),
        }
    }

    /// Create a ParquetSchema from an existing Arrow schema.
    pub fn from_arrow_schema(schema: SchemaRef) -> Self {
        Self {
            arrow_schema: schema,
        }
    }

    /// Get the Arrow schema.
    pub fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    /// Get field by name.
    pub fn field(&self, name: &str) -> Option<&arrow::datatypes::Field> {
        self.arrow_schema.field_with_name(name).ok()
    }

    /// Number of fields in schema.
    pub fn num_fields(&self) -> usize {
        self.arrow_schema.fields().len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use super::*;

    fn create_test_schema() -> SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new(
                "metric_name",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
            Field::new(
                "service",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]))
    }

    #[test]
    fn test_schema_creation() {
        let schema = ParquetSchema::from_arrow_schema(create_test_schema());
        assert_eq!(schema.num_fields(), 5);
    }

    #[test]
    fn test_legacy_schema_has_14_fields() {
        let schema = ParquetSchema::new();
        assert_eq!(schema.num_fields(), 14);
        // Spot-check a few key field names from the old fixed schema
        assert!(schema.field("metric_name").is_some());
        assert!(schema.field("tag_service").is_some());
        assert!(schema.field("service_name").is_some());
        assert!(schema.field("attributes").is_some());
        assert!(schema.field("resource_attributes").is_some());
    }

    #[test]
    fn test_field_lookup() {
        let schema = ParquetSchema::from_arrow_schema(create_test_schema());
        let field = schema.field("metric_name").unwrap();
        assert!(!field.is_nullable());
    }
}
