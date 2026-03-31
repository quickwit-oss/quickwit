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

//! Parquet schema definitions for storage.
//!
//! Extends the existing Arrow schema from `arrow_metrics.rs` with Parquet-specific
//! column types optimized for storage and query efficiency.

mod fields;
mod parquet;
pub mod sketch_fields;
pub mod sketch_schema;

pub use fields::{
    REQUIRED_FIELDS, SKETCH_REQUIRED_FIELDS, SKETCH_SORT_ORDER, SORT_ORDER, required_field_type,
    sketch_required_field_type, validate_required_fields, validate_required_sketch_fields,
};
pub use parquet::ParquetSchema;
pub use sketch_fields::SketchParquetField;
pub use sketch_schema::SketchParquetSchema;
