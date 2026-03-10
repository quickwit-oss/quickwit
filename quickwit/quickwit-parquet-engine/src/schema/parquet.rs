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

use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use parquet::arrow::ArrowSchemaConverter;
use parquet::schema::types::SchemaDescriptor;

use super::fields::ParquetField;

/// Parquet schema for storage.
#[derive(Debug, Clone)]
pub struct ParquetSchema {
    arrow_schema: SchemaRef,
}

impl ParquetSchema {
    /// Create a new ParquetSchema.
    pub fn new() -> Self {
        let fields: Vec<_> = ParquetField::all()
            .iter()
            .map(|f| f.to_arrow_field())
            .collect();

        let arrow_schema = Arc::new(ArrowSchema::new(fields));
        Self { arrow_schema }
    }

    /// Get the Arrow schema.
    pub fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    /// Convert to Parquet schema descriptor.
    pub fn parquet_schema(&self) -> Result<SchemaDescriptor, parquet::errors::ParquetError> {
        ArrowSchemaConverter::new().convert(&self.arrow_schema)
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

impl Default for ParquetSchema {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = ParquetSchema::new();
        assert_eq!(schema.num_fields(), 14);
    }

    #[test]
    fn test_field_lookup() {
        let schema = ParquetSchema::new();
        let field = schema.field("metric_name").unwrap();
        assert!(!field.is_nullable());
    }

    #[test]
    fn test_parquet_conversion() {
        let schema = ParquetSchema::new();
        let parquet_schema = schema.parquet_schema();
        assert!(parquet_schema.is_ok());
    }
}
