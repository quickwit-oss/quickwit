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

//! DataFusion UDFs for querying DDSketch parquet rows.
//!
//! `dd_sketch(keys, counts, count, min, max, flags)` is the decomposable aggregate:
//! it merges sparse DDSketch bucket arrays and scalar bounds into a single
//! struct. `dd_quantile(sketch, q)` is the final scalar projection over that
//! merged sketch.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Float64Builder, Int16Array, ListArray, StructArray,
    UInt32Array, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Float64Type, Int16Type, UInt32Type, UInt64Type};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

const SUPPORTED_SKETCH_FLAGS: u32 = 0;

#[derive(Debug, Clone)]
pub(crate) struct SketchConfig {
    gamma: f64,
    bias: f64,
}

impl SketchConfig {
    fn quantile_for_key(&self, key: i16) -> f64 {
        if key == 0 {
            return 0.0;
        }
        let abs_key = key.unsigned_abs() as f64;
        let value = self.gamma.powf(abs_key - self.bias);
        if key > 0 { value } else { -value }
    }
}

impl Default for SketchConfig {
    fn default() -> Self {
        const EPSILON: f64 = 1.0 / 128.0;
        const MIN_VALUE: f64 = 1e-9;
        let gamma = 1.0 + 2.0 * EPSILON;
        let gamma_ln = gamma.ln();
        let emin = (MIN_VALUE.ln() / gamma_ln).floor() as i64;
        let bias = (-emin + 1) as f64;
        Self { gamma, bias }
    }
}

pub(crate) fn merged_sketch_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new(
            "keys",
            DataType::List(Arc::new(Field::new("item", DataType::Int16, false))),
            false,
        ),
        Field::new(
            "counts",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
            false,
        ),
        Field::new("total_count", DataType::UInt64, false),
        Field::new("global_min", DataType::Float64, false),
        Field::new("global_max", DataType::Float64, false),
        Field::new("flags", DataType::UInt32, false),
    ]))
}

fn validate_flags(context: &str, flags: u32) -> DFResult<()> {
    if flags == SUPPORTED_SKETCH_FLAGS {
        Ok(())
    } else {
        Err(DataFusionError::Execution(format!(
            "{context}: unsupported sketch flags {flags}"
        )))
    }
}

fn validate_bounds(context: &str, min: f64, max: f64) -> DFResult<()> {
    if min.is_finite() && max.is_finite() && min <= max {
        Ok(())
    } else {
        Err(DataFusionError::Execution(format!(
            "{context}: invalid sketch bounds: min={min}, max={max}"
        )))
    }
}

struct QuantileRow<'a> {
    keys: &'a [i16],
    counts: &'a [u64],
    total_count: u64,
    global_min: f64,
    global_max: f64,
    flags: u32,
    quantile: f64,
}

fn quantile_for_row(config: &SketchConfig, row: QuantileRow<'_>) -> DFResult<Option<f64>> {
    if !(0.0..=1.0).contains(&row.quantile) {
        return Err(DataFusionError::Execution(format!(
            "dd_quantile: quantile must be between 0.0 and 1.0, got {}",
            row.quantile
        )));
    }
    validate_flags("dd_quantile", row.flags)?;

    if row.keys.len() != row.counts.len() {
        return Err(DataFusionError::Execution(format!(
            "dd_quantile: keys/counts length mismatch: keys has {} elements but counts has {}",
            row.keys.len(),
            row.counts.len()
        )));
    }
    let mut bucket_total = 0u64;
    for count in row.counts {
        bucket_total = bucket_total.checked_add(*count).ok_or_else(|| {
            DataFusionError::Execution("dd_quantile: total bucket count overflow".to_string())
        })?;
    }
    if bucket_total != row.total_count {
        return Err(DataFusionError::Execution(format!(
            "dd_quantile: total_count {} does not match sum(counts) {bucket_total}",
            row.total_count
        )));
    }

    if row.total_count == 0 {
        return Ok(None);
    }
    validate_bounds("dd_quantile", row.global_min, row.global_max)?;

    if row.quantile == 0.0 {
        return Ok(Some(row.global_min));
    }
    if row.quantile == 1.0 {
        return Ok(Some(row.global_max));
    }

    let rank = (row.quantile * (row.total_count as f64 - 1.0)).floor() as u64 + 1;
    let mut cumulative = 0u64;
    for (key, count) in row.keys.iter().zip(row.counts.iter()) {
        cumulative = cumulative.checked_add(*count).ok_or_else(|| {
            DataFusionError::Execution("dd_quantile: cumulative count overflow".to_string())
        })?;
        if cumulative >= rank {
            return Ok(Some(
                config
                    .quantile_for_key(*key)
                    .clamp(row.global_min, row.global_max),
            ));
        }
    }

    Err(DataFusionError::Execution(format!(
        "dd_quantile: bucket counts do not reach rank {rank}"
    )))
}

pub(crate) struct DdSketchAccumulator {
    merged_buckets: BTreeMap<i16, u64>,
    total_count: u64,
    global_min: f64,
    global_max: f64,
    flags: u32,
}

impl Debug for DdSketchAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DdSketchAccumulator")
            .field("total_count", &self.total_count)
            .field("global_min", &self.global_min)
            .field("global_max", &self.global_max)
            .field("buckets_len", &self.merged_buckets.len())
            .finish()
    }
}

impl Default for DdSketchAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl DdSketchAccumulator {
    fn new() -> Self {
        Self {
            merged_buckets: BTreeMap::new(),
            total_count: 0,
            global_min: f64::INFINITY,
            global_max: f64::NEG_INFINITY,
            flags: SUPPORTED_SKETCH_FLAGS,
        }
    }

    fn update_single(
        &mut self,
        keys: &[i16],
        counts: &[u64],
        count: u64,
        min: f64,
        max: f64,
        flags: u32,
    ) -> DFResult<()> {
        if keys.len() != counts.len() {
            return Err(DataFusionError::Execution(format!(
                "dd_sketch: keys/counts length mismatch: keys has {} elements but counts has {}",
                keys.len(),
                counts.len()
            )));
        }

        let mut bucket_total = 0u64;
        for cnt in counts {
            bucket_total = bucket_total.checked_add(*cnt).ok_or_else(|| {
                DataFusionError::Execution(
                    "dd_sketch: bucket count overflow while validating row".to_string(),
                )
            })?;
        }

        if bucket_total != count {
            return Err(DataFusionError::Execution(format!(
                "dd_sketch: row count {count} does not match sum(counts) {bucket_total}"
            )));
        }

        validate_flags("dd_sketch", flags)?;
        if count == 0 {
            return Ok(());
        }
        validate_bounds("dd_sketch", min, max)?;

        for (key, cnt) in keys.iter().zip(counts.iter()) {
            let current = self.merged_buckets.entry(*key).or_insert(0);
            *current = current.checked_add(*cnt).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "dd_sketch: bucket count overflow while merging key {key}"
                ))
            })?;
        }

        self.total_count = self.total_count.checked_add(count).ok_or_else(|| {
            DataFusionError::Execution(
                "dd_sketch: total count overflow while merging rows".to_string(),
            )
        })?;
        self.global_min = self.global_min.min(min);
        self.global_max = self.global_max.max(max);
        self.flags = flags;
        Ok(())
    }

    fn state_arrays(&self) -> (ArrayRef, ArrayRef, ArrayRef, ArrayRef, ArrayRef, ArrayRef) {
        let mut keys = Vec::with_capacity(self.merged_buckets.len());
        let mut counts = Vec::with_capacity(self.merged_buckets.len());
        for (key, count) in &self.merged_buckets {
            keys.push(*key);
            counts.push(*count);
        }

        let keys_array = Arc::new(Int16Array::from(keys)) as ArrayRef;
        let counts_array = Arc::new(UInt64Array::from(counts)) as ArrayRef;

        let keys_list = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int16, false)),
            OffsetBuffer::from_lengths([keys_array.len()]),
            keys_array,
            None,
        )) as ArrayRef;
        let counts_list = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::UInt64, false)),
            OffsetBuffer::from_lengths([counts_array.len()]),
            counts_array,
            None,
        )) as ArrayRef;

        (
            keys_list,
            counts_list,
            Arc::new(UInt64Array::from(vec![self.total_count])) as ArrayRef,
            Arc::new(Float64Array::from(vec![self.global_min])) as ArrayRef,
            Arc::new(Float64Array::from(vec![self.global_max])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![self.flags])) as ArrayRef,
        )
    }
}

impl Accumulator for DdSketchAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let keys_list = values[0].as_list::<i32>();
        let counts_list = values[1].as_list::<i32>();
        let count_array = values[2].as_primitive::<UInt64Type>();
        let min_array = values[3].as_primitive::<Float64Type>();
        let max_array = values[4].as_primitive::<Float64Type>();
        let flags_array = values[5].as_primitive::<UInt32Type>();

        let key_offsets = keys_list.value_offsets();
        let key_values = keys_list.values().as_primitive::<Int16Type>().values();
        let count_offsets = counts_list.value_offsets();
        let count_values = counts_list.values().as_primitive::<UInt64Type>().values();

        for row_idx in 0..keys_list.len() {
            if keys_list.is_null(row_idx)
                || counts_list.is_null(row_idx)
                || count_array.is_null(row_idx)
                || min_array.is_null(row_idx)
                || max_array.is_null(row_idx)
                || flags_array.is_null(row_idx)
            {
                continue;
            }

            let key_start = key_offsets[row_idx] as usize;
            let key_end = key_offsets[row_idx + 1] as usize;
            let count_start = count_offsets[row_idx] as usize;
            let count_end = count_offsets[row_idx + 1] as usize;

            self.update_single(
                &key_values[key_start..key_end],
                &count_values[count_start..count_end],
                count_array.value(row_idx),
                min_array.value(row_idx),
                max_array.value(row_idx),
                flags_array.value(row_idx),
            )?;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let (keys_list, counts_list, total_count_arr, min_arr, max_arr, flags_arr) =
            self.state_arrays();
        let fields = Fields::from(vec![
            Field::new("keys", keys_list.data_type().clone(), false),
            Field::new("counts", counts_list.data_type().clone(), false),
            Field::new("total_count", DataType::UInt64, false),
            Field::new("global_min", DataType::Float64, false),
            Field::new("global_max", DataType::Float64, false),
            Field::new("flags", DataType::UInt32, false),
        ]);

        Ok(ScalarValue::Struct(Arc::new(StructArray::try_new(
            fields,
            vec![
                keys_list,
                counts_list,
                total_count_arr,
                min_arr,
                max_arr,
                flags_arr,
            ],
            None,
        )?)))
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let (keys_list, counts_list, total_count_arr, min_arr, max_arr, flags_arr) =
            self.state_arrays();
        Ok(vec![
            ScalarValue::List(Arc::new(keys_list.as_list::<i32>().clone())),
            ScalarValue::List(Arc::new(counts_list.as_list::<i32>().clone())),
            ScalarValue::UInt64(Some(total_count_arr.as_primitive::<UInt64Type>().value(0))),
            ScalarValue::Float64(Some(min_arr.as_primitive::<Float64Type>().value(0))),
            ScalarValue::Float64(Some(max_arr.as_primitive::<Float64Type>().value(0))),
            ScalarValue::UInt32(Some(flags_arr.as_primitive::<UInt32Type>().value(0))),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.update_batch(states)
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.merged_buckets.len() * 58
    }
}

#[derive(Debug)]
struct DdSketchUdaf {
    signature: Signature,
}

impl PartialEq for DdSketchUdaf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for DdSketchUdaf {}

impl std::hash::Hash for DdSketchUdaf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        "dd_sketch".hash(state);
    }
}

impl DdSketchUdaf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::List(Arc::new(Field::new("item", DataType::Int16, false))),
                    DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
                    DataType::UInt64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::UInt32,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for DdSketchUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "dd_sketch"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(merged_sketch_type())
    }

    fn accumulator(
        &self,
        _arg: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(DdSketchAccumulator::new()))
    }

    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> DFResult<Vec<Arc<Field>>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "merged_keys"),
                DataType::List(Arc::new(Field::new("item", DataType::Int16, false))),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "merged_counts"),
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "total_count"),
                DataType::UInt64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "global_min"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "global_max"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "flags"),
                DataType::UInt32,
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct DdQuantileUdf {
    signature: Signature,
    config: SketchConfig,
}

impl PartialEq for DdQuantileUdf {
    fn eq(&self, other: &Self) -> bool {
        self.config.gamma.to_bits() == other.config.gamma.to_bits()
            && self.config.bias.to_bits() == other.config.bias.to_bits()
    }
}

impl Eq for DdQuantileUdf {}

impl std::hash::Hash for DdQuantileUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.config.gamma.to_bits().hash(state);
        self.config.bias.to_bits().hash(state);
    }
}

impl DdQuantileUdf {
    fn new(config: SketchConfig) -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![merged_sketch_type(), DataType::Float64]),
                Volatility::Immutable,
            ),
            config,
        }
    }
}

impl ScalarUDFImpl for DdQuantileUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "dd_quantile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let sketch_array = args.args[0].to_array(args.number_rows)?;
        let quantile_array = args.args[1].to_array(args.number_rows)?;
        let quantiles = quantile_array.as_primitive::<Float64Type>();

        let sketches = sketch_array.as_struct();
        let keys_col = sketches.column(0).as_list::<i32>();
        let counts_col = sketches.column(1).as_list::<i32>();
        let total_count_col = sketches.column(2).as_primitive::<UInt64Type>();
        let min_col = sketches.column(3).as_primitive::<Float64Type>();
        let max_col = sketches.column(4).as_primitive::<Float64Type>();
        let flags_col = sketches.column(5).as_primitive::<UInt32Type>();

        let key_offsets = keys_col.value_offsets();
        let key_values = keys_col.values().as_primitive::<Int16Type>().values();
        let count_offsets = counts_col.value_offsets();
        let count_values = counts_col.values().as_primitive::<UInt64Type>().values();

        let mut results = Float64Builder::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            if sketches.is_null(row)
                || keys_col.is_null(row)
                || counts_col.is_null(row)
                || total_count_col.is_null(row)
                || min_col.is_null(row)
                || max_col.is_null(row)
                || flags_col.is_null(row)
                || quantiles.is_null(row)
            {
                results.append_null();
                continue;
            }

            let key_start = key_offsets[row] as usize;
            let key_end = key_offsets[row + 1] as usize;
            let count_start = count_offsets[row] as usize;
            let count_end = count_offsets[row + 1] as usize;

            let quantile_row = QuantileRow {
                keys: &key_values[key_start..key_end],
                counts: &count_values[count_start..count_end],
                total_count: total_count_col.value(row),
                global_min: min_col.value(row),
                global_max: max_col.value(row),
                flags: flags_col.value(row),
                quantile: quantiles.value(row),
            };
            match quantile_for_row(&self.config, quantile_row)? {
                Some(value) => results.append_value(value),
                None => results.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(results.finish())))
    }
}

pub(crate) fn create_dd_sketch_udaf() -> AggregateUDF {
    AggregateUDF::from(DdSketchUdaf::new())
}

pub(crate) fn create_dd_quantile_udf() -> ScalarUDF {
    ScalarUDF::from(DdQuantileUdf::new(SketchConfig::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn qrow<'a>(
        keys: &'a [i16],
        counts: &'a [u64],
        total_count: u64,
        global_min: f64,
        global_max: f64,
        flags: u32,
        quantile: f64,
    ) -> QuantileRow<'a> {
        QuantileRow {
            keys,
            counts,
            total_count,
            global_min,
            global_max,
            flags,
            quantile,
        }
    }

    #[test]
    fn merge_accumulator_coalesces_keys() {
        let mut acc = DdSketchAccumulator::new();
        acc.update_single(&[1338, 1784], &[131_072, 1], 131_073, 1.0, 1000.0, 0)
            .unwrap();
        acc.update_single(&[1338, 1784], &[128, 4], 132, 1.0, 1000.0, 0)
            .unwrap();

        assert_eq!(acc.total_count, 131_205);
        assert_eq!(acc.merged_buckets[&1338], 131_200);
        assert_eq!(acc.merged_buckets[&1784], 5);
    }

    #[test]
    fn merge_batch_combines_partial_states() {
        let mut left = DdSketchAccumulator::new();
        left.update_single(&[1338, 1400], &[3, 1], 4, 1.0, 2.0, 0)
            .unwrap();
        let mut right = DdSketchAccumulator::new();
        right
            .update_single(&[1338, 1450], &[2, 1], 3, 1.0, 6.0, 0)
            .unwrap();

        let left_state = left.state().unwrap();
        let right_state = right.state().unwrap();
        let state_arrays = (0..left_state.len())
            .map(|idx| {
                ScalarValue::iter_to_array(vec![left_state[idx].clone(), right_state[idx].clone()])
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let mut merged = DdSketchAccumulator::new();
        merged.merge_batch(&state_arrays).unwrap();

        let mut one_pass = DdSketchAccumulator::new();
        one_pass
            .update_single(&[1338, 1400], &[3, 1], 4, 1.0, 2.0, 0)
            .unwrap();
        one_pass
            .update_single(&[1338, 1450], &[2, 1], 3, 1.0, 6.0, 0)
            .unwrap();

        assert_eq!(merged.total_count, one_pass.total_count);
        assert_eq!(merged.global_min, one_pass.global_min);
        assert_eq!(merged.global_max, one_pass.global_max);
        assert_eq!(merged.flags, one_pass.flags);
        assert_eq!(merged.merged_buckets, one_pass.merged_buckets);
    }

    #[test]
    fn merge_accumulator_ignores_empty_sketch_bounds() {
        let mut acc = DdSketchAccumulator::new();
        acc.update_single(&[], &[], 0, -999.0, 999.0, 0).unwrap();
        acc.update_single(&[1338], &[2], 2, 1.0, 2.0, 0).unwrap();
        acc.update_single(&[999], &[0], 0, -123.0, 456.0, 0)
            .unwrap();

        assert_eq!(acc.total_count, 2);
        assert_eq!(acc.global_min, 1.0);
        assert_eq!(acc.global_max, 2.0);
        assert_eq!(acc.merged_buckets[&1338], 2);
        assert!(!acc.merged_buckets.contains_key(&999));
    }

    #[test]
    fn merge_accumulator_rejects_invalid_bounds_for_non_empty_sketch() {
        let mut acc = DdSketchAccumulator::new();
        let reversed = acc.update_single(&[1], &[1], 1, 2.0, 1.0, 0).unwrap_err();
        assert!(reversed.to_string().contains("invalid sketch bounds"));

        let nan = acc
            .update_single(&[1], &[1], 1, f64::NAN, 1.0, 0)
            .unwrap_err();
        assert!(nan.to_string().contains("invalid sketch bounds"));
    }

    #[test]
    fn merge_accumulator_rejects_unsupported_flags() {
        let mut acc = DdSketchAccumulator::new();
        let err = acc.update_single(&[1], &[1], 1, 1.0, 1.0, 1).unwrap_err();
        assert!(err.to_string().contains("unsupported sketch flags"));
    }

    #[test]
    fn merge_accumulator_rejects_inconsistent_summary_count() {
        let mut acc = DdSketchAccumulator::new();
        let err = acc
            .update_single(&[1, 2], &[3, 4], 99, 1.0, 2.0, 0)
            .unwrap_err();

        assert!(
            err.to_string().contains("does not match sum(counts)"),
            "{err}"
        );
    }

    #[test]
    fn quantile_for_key_matches_sparse_config_anchor() {
        let config = SketchConfig::default();

        assert!((config.quantile_for_key(1338) - 1.0).abs() < f64::EPSILON);
        assert!((config.quantile_for_key(1784) - 1000.0).abs() < 10.0);
        assert_eq!(config.quantile_for_key(0), 0.0);
    }

    #[test]
    fn quantile_for_row_validates_inputs() {
        let config = SketchConfig::default();
        let invalid_q =
            quantile_for_row(&config, qrow(&[1], &[1], 1, 1.0, 1.0, 0, 1.5)).unwrap_err();
        assert!(invalid_q.to_string().contains("quantile must be"));

        let reversed_bounds =
            quantile_for_row(&config, qrow(&[1], &[1], 1, 2.0, 1.0, 0, 0.5)).unwrap_err();
        assert!(
            reversed_bounds
                .to_string()
                .contains("invalid sketch bounds")
        );

        let nan_bounds =
            quantile_for_row(&config, qrow(&[1], &[1], 1, f64::NAN, 1.0, 0, 0.5)).unwrap_err();
        assert!(nan_bounds.to_string().contains("invalid sketch bounds"));

        let flags = quantile_for_row(&config, qrow(&[1], &[1], 1, 1.0, 1.0, 1, 0.5)).unwrap_err();
        assert!(flags.to_string().contains("unsupported sketch flags"));

        let mismatched_lengths =
            quantile_for_row(&config, qrow(&[1, 2], &[1], 1, 1.0, 1.0, 0, 0.5)).unwrap_err();
        assert!(
            mismatched_lengths
                .to_string()
                .contains("keys/counts length mismatch")
        );

        let mismatched_total =
            quantile_for_row(&config, qrow(&[1], &[2], 1, 1.0, 1.0, 0, 0.5)).unwrap_err();
        assert!(mismatched_total.to_string().contains("does not match"));
    }

    #[test]
    fn quantile_for_row_handles_bounds_and_empty_sketches() {
        let config = SketchConfig::default();

        assert_eq!(
            quantile_for_row(
                &config,
                qrow(&[], &[], 0, f64::INFINITY, f64::NEG_INFINITY, 0, 0.5)
            )
            .unwrap(),
            None
        );
        assert_eq!(
            quantile_for_row(&config, qrow(&[1338], &[2], 2, 1.0, 2.0, 0, 0.0)).unwrap(),
            Some(1.0)
        );
        assert_eq!(
            quantile_for_row(&config, qrow(&[1338], &[2], 2, 1.0, 2.0, 0, 1.0)).unwrap(),
            Some(2.0)
        );
    }
}
