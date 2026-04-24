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

use std::hash::Hasher;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Int32Type};
use arrow::record_batch::RecordBatch;

use super::RoutingExprContext;
#[cfg(test)]
use super::RoutingExpr;
#[cfg(test)]
use serde_json::Value as JsonValue;

/// Context for evaluating routing expressions against a single row of an Arrow `RecordBatch`.
///
/// Hashing is deliberately consistent with the JSON-backed `RoutingExprContext`
/// implementation so identical logical values produce the same `partition_id`
/// whether they arrive as JSON or Arrow IPC.
pub struct ArrowRowContext<'a> {
    batch: &'a RecordBatch,
    row_idx: usize,
}

impl<'a> ArrowRowContext<'a> {
    /// Creates an Arrow-backed routing context for one row in a `RecordBatch`.
    pub fn new(batch: &'a RecordBatch, row_idx: usize) -> Self {
        Self { batch, row_idx }
    }
}

impl<'a> RoutingExprContext for ArrowRowContext<'a> {
    fn hash_attribute<H: Hasher>(&self, attr_name: &[String], hasher: &mut H) {
        // Metrics/sketches have flat schemas — attr_name is always a single column name.
        let col_name = &attr_name[0];
        let col_idx = match self.batch.schema().index_of(col_name) {
            Ok(idx) => idx,
            Err(_) => {
                hasher.write_u8(0u8);
                return;
            }
        };
        let column = self.batch.column(col_idx);
        if column.is_null(self.row_idx) {
            hasher.write_u8(0u8);
            return;
        }
        // Extract the string value. Routing expressions reference string tag columns;
        // non-string columns are treated as absent.
        let string_value = match column.data_type() {
            DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8 => {
                let dict = column
                    .as_any()
                    .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
                    .expect("dictionary column should be DictionaryArray<Int32>");
                let values = dict.values().as_string::<i32>();
                let key = dict.keys().value(self.row_idx) as usize;
                Some(values.value(key))
            }
            DataType::Utf8 => {
                let arr = column.as_string::<i32>();
                Some(arr.value(self.row_idx))
            }
            _ => None,
        };
        match string_value {
            Some(s) => {
                // Match JSON impl: 1u8 (present) + hash_json_val for String (3u8 + len + bytes).
                hasher.write_u8(1u8);
                hasher.write_u8(3u8);
                hasher.write_u64(s.len() as u64);
                hasher.write(s.as_bytes());
            }
            None => {
                hasher.write_u8(0u8);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringDictionaryBuilder;
    use arrow::datatypes::{Field, Schema as ArrowSchema};

    use super::*;

    #[test]
    fn test_arrow_row_context_hash_matches_json() {
        let routing_expr = RoutingExpr::new("hash_mod((metric_name,host), 100)").unwrap();

        let json_ctx: serde_json::Map<String, JsonValue> = serde_json::from_str(
            r#"{"metric_name": "cpu.usage", "host": "server-01", "env": "prod"}"#,
        )
        .unwrap();
        let json_hash = routing_expr.eval_hash(&json_ctx);

        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("host", dict_type.clone(), true),
            Field::new("env", dict_type, true),
        ]));

        let mut metric_name_builder = StringDictionaryBuilder::<Int32Type>::new();
        metric_name_builder.append_value("cpu.usage");
        let mut host_builder = StringDictionaryBuilder::<Int32Type>::new();
        host_builder.append_value("server-01");
        let mut env_builder = StringDictionaryBuilder::<Int32Type>::new();
        env_builder.append_value("prod");

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(metric_name_builder.finish()),
                Arc::new(host_builder.finish()),
                Arc::new(env_builder.finish()),
            ],
        )
        .unwrap();

        let arrow_ctx = ArrowRowContext::new(&batch, 0);
        let arrow_hash = routing_expr.eval_hash(&arrow_ctx);

        assert_eq!(
            json_hash, arrow_hash,
            "Arrow and JSON contexts must produce identical partition hashes"
        );
    }
}
