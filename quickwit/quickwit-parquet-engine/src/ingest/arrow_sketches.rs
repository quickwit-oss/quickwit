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

//! Arrow-based batch building for DDSketch data with dynamic schema discovery.
//!
//! Follows the same two-pass pattern as `ArrowMetricsBatchBuilder`: accumulate
//! data points, discover the union of tag keys, then build the RecordBatch.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Builder, Int16Builder, ListBuilder, RecordBatch, StringDictionaryBuilder,
    UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};

/// A single DDSketch data point with tags.
#[derive(Debug, Clone)]
pub struct SketchDataPoint {
    pub metric_name: String,
    pub timestamp_secs: u64,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub flags: u32,
    pub keys: Vec<i16>,
    pub counts: Vec<u64>,
    pub tags: HashMap<String, String>,
}

/// Builder for creating Arrow RecordBatch from SketchDataPoints.
///
/// Accumulates data points and discovers the schema dynamically at `finish()`
/// time. Uses dictionary encoding for string columns (metric_name, all tags)
/// to achieve significant compression for low cardinality values.
pub struct ArrowSketchBatchBuilder {
    data_points: Vec<SketchDataPoint>,
}

impl ArrowSketchBatchBuilder {
    /// Creates a new builder with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data_points: Vec::with_capacity(capacity),
        }
    }

    /// Appends a SketchDataPoint to the batch.
    pub fn append(&mut self, data_point: SketchDataPoint) {
        self.data_points.push(data_point);
    }

    /// Finalizes and returns the RecordBatch.
    ///
    /// Performs two passes:
    /// 1. Schema discovery: scans all data points to collect the union of tag keys.
    /// 2. Array building: creates per-column builders and populates them.
    pub fn finish(self) -> RecordBatch {
        let num_rows = self.data_points.len();

        // Pass 1: discover all tag keys across all data points.
        let mut tag_keys: BTreeSet<&str> = BTreeSet::new();
        for dp in &self.data_points {
            for key in dp.tags.keys() {
                tag_keys.insert(key.as_str());
            }
        }
        let sorted_tag_keys: Vec<String> = tag_keys.into_iter().map(str::to_owned).collect();

        // Build the Arrow schema dynamically
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let mut fields = Vec::with_capacity(9 + sorted_tag_keys.len());
        fields.push(Field::new("metric_name", dict_type.clone(), false));
        fields.push(Field::new("timestamp_secs", DataType::UInt64, false));
        fields.push(Field::new("count", DataType::UInt64, false));
        fields.push(Field::new("sum", DataType::Float64, false));
        fields.push(Field::new("min", DataType::Float64, false));
        fields.push(Field::new("max", DataType::Float64, false));
        fields.push(Field::new("flags", DataType::UInt32, false));
        fields.push(Field::new(
            "keys",
            DataType::List(Arc::new(Field::new("item", DataType::Int16, false))),
            false,
        ));
        fields.push(Field::new(
            "counts",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
            false,
        ));

        for tag_key in &sorted_tag_keys {
            fields.push(Field::new(tag_key, dict_type.clone(), true));
        }

        let schema = Arc::new(ArrowSchema::new(fields));

        // Pass 2: build arrays
        let mut metric_name_builder: StringDictionaryBuilder<Int32Type> =
            StringDictionaryBuilder::new();
        let mut timestamp_secs_builder = UInt64Builder::with_capacity(num_rows);
        let mut count_builder = UInt64Builder::with_capacity(num_rows);
        let mut sum_builder = Float64Builder::with_capacity(num_rows);
        let mut min_builder = Float64Builder::with_capacity(num_rows);
        let mut max_builder = Float64Builder::with_capacity(num_rows);
        let mut flags_builder = UInt32Builder::with_capacity(num_rows);
        let mut keys_builder = ListBuilder::new(Int16Builder::new())
            .with_field(Field::new("item", DataType::Int16, false));
        let mut counts_builder = ListBuilder::new(UInt64Builder::new())
            .with_field(Field::new("item", DataType::UInt64, false));

        let mut tag_builders: Vec<StringDictionaryBuilder<Int32Type>> = sorted_tag_keys
            .iter()
            .map(|_| StringDictionaryBuilder::new())
            .collect();

        for dp in &self.data_points {
            metric_name_builder.append_value(&dp.metric_name);
            timestamp_secs_builder.append_value(dp.timestamp_secs);
            count_builder.append_value(dp.count);
            sum_builder.append_value(dp.sum);
            min_builder.append_value(dp.min);
            max_builder.append_value(dp.max);
            flags_builder.append_value(dp.flags);

            let keys_inner = keys_builder.values();
            for &k in &dp.keys {
                keys_inner.append_value(k);
            }
            keys_builder.append(true);

            let counts_inner = counts_builder.values();
            for &c in &dp.counts {
                counts_inner.append_value(c);
            }
            counts_builder.append(true);

            for (tag_idx, tag_key) in sorted_tag_keys.iter().enumerate() {
                match dp.tags.get(tag_key) {
                    Some(tag_val) => tag_builders[tag_idx].append_value(tag_val),
                    None => tag_builders[tag_idx].append_null(),
                }
            }
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(9 + sorted_tag_keys.len());
        arrays.push(Arc::new(metric_name_builder.finish()));
        arrays.push(Arc::new(timestamp_secs_builder.finish()));
        arrays.push(Arc::new(count_builder.finish()));
        arrays.push(Arc::new(sum_builder.finish()));
        arrays.push(Arc::new(min_builder.finish()));
        arrays.push(Arc::new(max_builder.finish()));
        arrays.push(Arc::new(flags_builder.finish()));
        arrays.push(Arc::new(keys_builder.finish()));
        arrays.push(Arc::new(counts_builder.finish()));

        for tag_builder in &mut tag_builders {
            arrays.push(Arc::new(tag_builder.finish()));
        }

        RecordBatch::try_new(schema, arrays).expect(
            "all arrays were built from the same schema discovered above; schema mismatch is a \
             bug in ArrowSketchBatchBuilder",
        )
    }

    /// Returns the number of rows appended so far.
    pub fn len(&self) -> usize {
        self.data_points.len()
    }

    /// Returns true if no rows have been appended.
    pub fn is_empty(&self) -> bool {
        self.data_points.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn make_test_sketch_point() -> SketchDataPoint {
        let mut tags = HashMap::new();
        tags.insert("service".to_string(), "api".to_string());
        tags.insert("env".to_string(), "prod".to_string());
        tags.insert("host".to_string(), "host-001".to_string());

        SketchDataPoint {
            metric_name: "req.latency".to_string(),
            timestamp_secs: 1704067200,
            count: 100,
            sum: 5000.0,
            min: 1.0,
            max: 200.0,
            flags: 0,
            keys: vec![100, 200, 300],
            counts: vec![50, 30, 20],
            tags,
        }
    }

    #[test]
    fn test_sketch_batch_builder_single_row() {
        let dp = make_test_sketch_point();
        let mut builder = ArrowSketchBatchBuilder::with_capacity(1);
        builder.append(dp);

        assert_eq!(builder.len(), 1);
        assert!(!builder.is_empty());

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 1);
        // 9 fixed columns + 3 tag columns (env, host, service)
        assert_eq!(batch.num_columns(), 12);

        // Verify schema field names
        let schema = batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec![
                "metric_name",
                "timestamp_secs",
                "count",
                "sum",
                "min",
                "max",
                "flags",
                "keys",
                "counts",
                "env",
                "host",
                "service",
            ]
        );
    }

    #[test]
    fn test_sketch_batch_builder_multiple_rows() {
        let mut builder = ArrowSketchBatchBuilder::with_capacity(100);

        for idx in 0..100 {
            let mut tags = HashMap::new();
            tags.insert("service".to_string(), format!("svc-{}", idx % 10));
            tags.insert("env".to_string(), "prod".to_string());

            let dp = SketchDataPoint {
                metric_name: "test.latency".to_string(),
                timestamp_secs: 1704067200 + idx as u64,
                count: 50,
                sum: 1000.0,
                min: 0.5,
                max: 100.0,
                flags: 0,
                keys: vec![100, 200],
                counts: vec![30, 20],
                tags,
            };
            builder.append(dp);
        }

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 100);
        // 9 fixed + 2 tags
        assert_eq!(batch.num_columns(), 11);
    }

    #[test]
    fn test_sketch_batch_dynamic_schema_discovery() {
        let mut builder = ArrowSketchBatchBuilder::with_capacity(2);

        // First point has tags: env, host
        let mut tags1 = HashMap::new();
        tags1.insert("env".to_string(), "prod".to_string());
        tags1.insert("host".to_string(), "host-1".to_string());
        builder.append(SketchDataPoint {
            metric_name: "m1".to_string(),
            timestamp_secs: 1000,
            count: 10,
            sum: 100.0,
            min: 1.0,
            max: 50.0,
            flags: 0,
            keys: vec![100],
            counts: vec![10],
            tags: tags1,
        });

        // Second point has tags: env, region (different set)
        let mut tags2 = HashMap::new();
        tags2.insert("env".to_string(), "staging".to_string());
        tags2.insert("region".to_string(), "us-west".to_string());
        builder.append(SketchDataPoint {
            metric_name: "m2".to_string(),
            timestamp_secs: 1001,
            count: 20,
            sum: 200.0,
            min: 2.0,
            max: 80.0,
            flags: 0,
            keys: vec![200],
            counts: vec![20],
            tags: tags2,
        });

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 2);
        // 9 fixed + 3 tags (env, host, region)
        assert_eq!(batch.num_columns(), 12);

        let schema = batch.schema();
        let tag_names: Vec<&str> = schema
            .fields()
            .iter()
            .skip(9)
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(tag_names, vec!["env", "host", "region"]);
    }

    #[test]
    fn test_sketch_batch_empty() {
        let builder = ArrowSketchBatchBuilder::with_capacity(0);
        assert!(builder.is_empty());

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 0);
        // 9 fixed columns, no tags
        assert_eq!(batch.num_columns(), 9);
    }

    #[test]
    fn test_sketch_batch_list_columns() {
        let mut builder = ArrowSketchBatchBuilder::with_capacity(2);

        builder.append(SketchDataPoint {
            metric_name: "m".to_string(),
            timestamp_secs: 1000,
            count: 10,
            sum: 100.0,
            min: 1.0,
            max: 50.0,
            flags: 0,
            keys: vec![100, 200, 300],
            counts: vec![5, 3, 2],
            tags: HashMap::new(),
        });

        // Different length lists
        builder.append(SketchDataPoint {
            metric_name: "m".to_string(),
            timestamp_secs: 1001,
            count: 5,
            sum: 50.0,
            min: 2.0,
            max: 30.0,
            flags: 0,
            keys: vec![150],
            counts: vec![5],
            tags: HashMap::new(),
        });

        let batch = builder.finish();
        assert_eq!(batch.num_rows(), 2);

        // Verify keys column is List type
        let schema = batch.schema();
        let keys_field = schema.field_with_name("keys").unwrap();
        assert!(matches!(keys_field.data_type(), DataType::List(_)));

        // Verify counts column is List type
        let counts_field = schema.field_with_name("counts").unwrap();
        assert!(matches!(counts_field.data_type(), DataType::List(_)));
    }

    #[test]
    fn test_sketch_batch_ipc_round_trip() {
        use super::super::processor::{ipc_to_record_batch, record_batch_to_ipc};

        let mut builder = ArrowSketchBatchBuilder::with_capacity(5);
        for idx in 0..5 {
            let mut tags = HashMap::new();
            tags.insert("service".to_string(), format!("svc-{}", idx % 3));

            builder.append(SketchDataPoint {
                metric_name: "test.sketch".to_string(),
                timestamp_secs: 1000 + idx as u64,
                count: 50,
                sum: 500.0,
                min: 1.0,
                max: 100.0,
                flags: 0,
                keys: vec![100, 200],
                counts: vec![30, 20],
                tags,
            });
        }

        let original_batch = builder.finish();

        let ipc_bytes = record_batch_to_ipc(&original_batch).unwrap();
        assert!(!ipc_bytes.is_empty());

        let recovered_batch = ipc_to_record_batch(&ipc_bytes).unwrap();

        assert_eq!(recovered_batch.num_rows(), original_batch.num_rows());
        assert_eq!(recovered_batch.num_columns(), original_batch.num_columns());
        assert_eq!(recovered_batch.schema(), original_batch.schema());
    }
}
