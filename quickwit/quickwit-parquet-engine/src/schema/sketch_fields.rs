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

//! DDSketch parquet required field definitions.
//!
//! Tag columns are dynamic (each unique tag key becomes its own column),
//! so only the required sketch-specific fields are defined here.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

/// Required fields in the DDSketch parquet schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SketchParquetField {
    MetricName,
    TimestampSecs,
    Count,
    Sum,
    Min,
    Max,
    Flags,
    Keys,
    Counts,
}

impl SketchParquetField {
    /// Field name as stored in Parquet.
    pub fn name(&self) -> &'static str {
        match self {
            Self::MetricName => "metric_name",
            Self::TimestampSecs => "timestamp_secs",
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Flags => "flags",
            Self::Keys => "keys",
            Self::Counts => "counts",
        }
    }

    /// Whether this field is nullable.
    pub fn is_nullable(&self) -> bool {
        false
    }

    /// Arrow DataType for this field.
    pub fn arrow_type(&self) -> DataType {
        match self {
            Self::MetricName => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
            Self::TimestampSecs => DataType::UInt64,
            Self::Count => DataType::UInt64,
            Self::Sum | Self::Min | Self::Max => DataType::Float64,
            Self::Flags => DataType::UInt32,
            Self::Keys => DataType::List(Arc::new(Field::new("item", DataType::Int16, false))),
            Self::Counts => DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
        }
    }

    /// Convert to Arrow Field.
    pub fn to_arrow_field(&self) -> Field {
        Field::new(self.name(), self.arrow_type(), self.is_nullable())
    }

    /// All required fields in schema order.
    pub fn all() -> &'static [SketchParquetField] {
        &[
            Self::MetricName,
            Self::TimestampSecs,
            Self::Count,
            Self::Sum,
            Self::Min,
            Self::Max,
            Self::Flags,
            Self::Keys,
            Self::Counts,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sketch_field_count() {
        assert_eq!(SketchParquetField::all().len(), 9);
    }

    #[test]
    fn test_sketch_field_names() {
        assert_eq!(SketchParquetField::MetricName.name(), "metric_name");
        assert_eq!(SketchParquetField::Keys.name(), "keys");
        assert_eq!(SketchParquetField::Counts.name(), "counts");
        assert_eq!(SketchParquetField::Flags.name(), "flags");
        assert_eq!(SketchParquetField::Count.name(), "count");
        assert_eq!(SketchParquetField::Sum.name(), "sum");
        assert_eq!(SketchParquetField::Min.name(), "min");
        assert_eq!(SketchParquetField::Max.name(), "max");
    }

    #[test]
    fn test_sketch_field_nullability() {
        assert!(!SketchParquetField::MetricName.is_nullable());
        assert!(!SketchParquetField::TimestampSecs.is_nullable());
        assert!(!SketchParquetField::Count.is_nullable());
        assert!(!SketchParquetField::Keys.is_nullable());
        assert!(!SketchParquetField::Counts.is_nullable());
    }

    #[test]
    fn test_sketch_field_arrow_types() {
        use arrow::datatypes::DataType;
        assert!(matches!(
            SketchParquetField::Keys.arrow_type(),
            DataType::List(_)
        ));
        assert!(matches!(
            SketchParquetField::Counts.arrow_type(),
            DataType::List(_)
        ));
        assert_eq!(SketchParquetField::Count.arrow_type(), DataType::UInt64);
        assert_eq!(SketchParquetField::Sum.arrow_type(), DataType::Float64);
        assert_eq!(SketchParquetField::Flags.arrow_type(), DataType::UInt32);
    }
}
