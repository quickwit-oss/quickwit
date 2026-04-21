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

//! DDSketch parquet schema construction.

use std::sync::Arc;

use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};

use super::sketch_fields::SketchParquetField;

/// Parquet schema for DDSketch storage.
#[derive(Debug, Clone)]
pub struct SketchParquetSchema {
    arrow_schema: SchemaRef,
}

impl SketchParquetSchema {
    /// Returns the 9-field required-only schema.
    ///
    /// The pipeline now writes dynamic schemas derived from the data (tag
    /// columns are discovered at runtime); prefer `from_arrow_schema` when you
    /// have a batch schema available.
    pub fn new() -> Self {
        let fields: Vec<_> = SketchParquetField::all()
            .iter()
            .map(|f| f.to_arrow_field())
            .collect();
        let arrow_schema = Arc::new(ArrowSchema::new(fields));
        Self { arrow_schema }
    }

    /// Create a SketchParquetSchema from an existing Arrow schema.
    pub fn from_arrow_schema(schema: SchemaRef) -> Self {
        Self {
            arrow_schema: schema,
        }
    }

    /// Get the Arrow schema.
    pub fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    /// Number of fields in schema.
    pub fn num_fields(&self) -> usize {
        self.arrow_schema.fields().len()
    }

    /// Get field by name.
    pub fn field(&self, name: &str) -> Option<&arrow::datatypes::Field> {
        self.arrow_schema.field_with_name(name).ok()
    }
}

impl Default for SketchParquetSchema {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sketch_schema_creation() {
        let schema = SketchParquetSchema::new();
        assert_eq!(schema.num_fields(), 9);
    }

    #[test]
    fn test_sketch_field_lookup() {
        let schema = SketchParquetSchema::new();
        assert!(schema.field("keys").is_some());
        assert!(schema.field("counts").is_some());
        assert!(schema.field("metric_name").is_some());
        assert!(schema.field("nonexistent").is_none());
    }
}
