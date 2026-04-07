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

use arrow::datatypes::SchemaRef;

/// Parquet schema for storage.
#[derive(Debug, Clone)]
pub struct ParquetSchema {
    arrow_schema: SchemaRef,
}

impl ParquetSchema {
    /// Create a ParquetSchema from an Arrow schema.
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
    fn test_field_lookup() {
        let schema = ParquetSchema::from_arrow_schema(create_test_schema());
        let field = schema.field("metric_name").unwrap();
        assert!(!field.is_nullable());
    }
}
