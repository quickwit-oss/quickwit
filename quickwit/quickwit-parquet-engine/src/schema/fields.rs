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

//! Parquet field definitions with sort order constants and validation.

use anyhow::{Result, bail};
use arrow::datatypes::DataType;

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
    "timestamp_secs",
];

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
