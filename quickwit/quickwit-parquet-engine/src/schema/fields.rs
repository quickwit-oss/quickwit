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

use std::sync::Arc;

use anyhow::{Result, bail};
use arrow::datatypes::{DataType, Field};

/// Required field names that must exist in every metrics batch.
pub const REQUIRED_FIELDS: &[&str] = &["metric_name", "metric_type", "timestamp_secs", "value"];

/// Required field names that must exist in every sketch batch.
pub const SKETCH_REQUIRED_FIELDS: &[&str] = &[
    "metric_name",
    "timestamp_secs",
    "count",
    "sum",
    "min",
    "max",
    "flags",
    "keys",
    "counts",
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

/// Arrow type for required sketch fields by name.
pub fn sketch_required_field_type(name: &str) -> Option<DataType> {
    match name {
        "metric_name" => Some(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        )),
        "timestamp_secs" => Some(DataType::UInt64),
        "count" => Some(DataType::UInt64),
        "sum" | "min" | "max" => Some(DataType::Float64),
        "flags" => Some(DataType::UInt32),
        "keys" => Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int16,
            false,
        )))),
        "counts" => Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::UInt64,
            false,
        )))),
        _ => None,
    }
}

/// Validate that a batch schema contains all required sketch fields with correct types.
pub fn validate_required_sketch_fields(schema: &arrow::datatypes::Schema) -> Result<()> {
    for &name in SKETCH_REQUIRED_FIELDS {
        match schema.index_of(name) {
            Ok(idx) => {
                let expected_type = sketch_required_field_type(name).unwrap();
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
            Err(_) => bail!("missing required sketch field '{}'", name),
        }
    }
    Ok(())
}
