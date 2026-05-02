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

//! Gap filling and interpolation for metrics rollup plans.
//!
//! The Substrait representation is an `ExtensionSingleRel` wrapping the plan
//! that produces one row per `(series/group, time bucket)`. The extension detail
//! is UTF-8 JSON matching [`GapFillConfig`].

use std::any::Any;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue, plan_err};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, Statistics,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_substrait::substrait::proto::ExtensionSingleRel;
use futures::TryStreamExt;
use serde::Deserialize;

pub(crate) const GAP_FILL_EXTENSION_TYPE_URL: &str =
    "type.googleapis.com/quickwit.metrics.v1.GapFill";

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FillMethod {
    Linear,
    Last,
    Zero,
    Null,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Deserialize)]
pub(crate) struct GapFillConfig {
    method: FillMethod,
    #[serde(default)]
    time_column: String,
    #[serde(default)]
    time_column_index: Option<usize>,
    #[serde(default)]
    value_column: String,
    #[serde(default)]
    value_column_index: Option<usize>,
    #[serde(default)]
    group_columns: Vec<String>,
    #[serde(default)]
    group_column_indices: Vec<usize>,
    step_secs: i64,
    #[serde(default = "default_limit_secs")]
    limit_secs: i64,
    #[serde(default)]
    start_secs: Option<i64>,
    #[serde(default)]
    end_secs: Option<i64>,
}

fn default_limit_secs() -> i64 {
    300
}

impl GapFillConfig {
    fn validate(&self) -> DFResult<()> {
        if self.step_secs <= 0 {
            return plan_err!(
                "gap fill step_secs must be positive, got {}",
                self.step_secs
            );
        }
        if self.limit_secs < 0 || self.limit_secs > 600 {
            return plan_err!(
                "gap fill limit_secs must be between 0 and 600, got {}",
                self.limit_secs
            );
        }
        if self.time_column.is_empty() && self.time_column_index.is_none() {
            return plan_err!("gap fill requires time_column or time_column_index");
        }
        if self.value_column.is_empty() && self.value_column_index.is_none() {
            return plan_err!("gap fill requires value_column or value_column_index");
        }
        if !self.group_columns.is_empty() && !self.group_column_indices.is_empty() {
            return plan_err!("gap fill accepts group_columns or group_column_indices, not both");
        }
        if let (Some(start), Some(end)) = (self.start_secs, self.end_secs)
            && start > end
        {
            return plan_err!("gap fill start_secs must be <= end_secs");
        }
        Ok(())
    }
}

pub(crate) fn try_consume_gap_fill_extension(
    rel: &ExtensionSingleRel,
    input: LogicalPlan,
) -> DFResult<Option<LogicalPlan>> {
    let Some(detail) = rel.detail.as_ref() else {
        return Ok(None);
    };
    if detail.type_url != GAP_FILL_EXTENSION_TYPE_URL {
        return Ok(None);
    }

    let config: GapFillConfig = serde_json::from_slice(&detail.value).map_err(|err| {
        DataFusionError::Plan(format!("failed to decode gap fill extension detail: {err}"))
    })?;
    config.validate()?;

    if matches!(config.method, FillMethod::Null) {
        return Ok(Some(input));
    }

    let input_schema = input.schema();
    let mut seen_group_columns = HashSet::new();
    for column in &config.group_columns {
        input_schema.field_with_unqualified_name(column)?;
        if !seen_group_columns.insert(column) {
            return plan_err!("gap fill group column '{column}' is listed more than once");
        }
    }
    validate_logical_column(
        input_schema,
        &config.time_column,
        config.time_column_index,
        "time",
    )?;
    validate_logical_column(
        input_schema,
        &config.value_column,
        config.value_column_index,
        "value",
    )?;
    validate_logical_indices(
        input_schema,
        &config.group_column_indices,
        "group_column_indices",
    )?;

    Ok(Some(LogicalPlan::Extension(Extension {
        node: Arc::new(GapFillNode { input, config }),
    })))
}

fn validate_logical_column(
    schema: &datafusion::common::DFSchema,
    name: &str,
    index: Option<usize>,
    role: &str,
) -> DFResult<()> {
    if let Some(index) = index {
        if index >= schema.fields().len() {
            return plan_err!(
                "gap fill {role}_column_index {index} is out of bounds for {} fields",
                schema.fields().len()
            );
        }
        Ok(())
    } else {
        schema.field_with_unqualified_name(name)?;
        Ok(())
    }
}

fn validate_logical_indices(
    schema: &datafusion::common::DFSchema,
    indices: &[usize],
    role: &str,
) -> DFResult<()> {
    let mut seen = HashSet::new();
    for index in indices {
        if *index >= schema.fields().len() {
            return plan_err!(
                "gap fill {role} contains out-of-bounds index {index} for {} fields",
                schema.fields().len()
            );
        }
        if !seen.insert(*index) {
            return plan_err!("gap fill {role} contains duplicate index {index}");
        }
    }
    Ok(())
}

fn resolve_group_indices(schema: &SchemaRef, config: &GapFillConfig) -> DFResult<Vec<usize>> {
    if !config.group_column_indices.is_empty() {
        for index in &config.group_column_indices {
            if *index >= schema.fields().len() {
                return plan_err!(
                    "gap fill group_column_indices contains out-of-bounds index {index} for {} fields",
                    schema.fields().len()
                );
            }
        }
        return Ok(config.group_column_indices.clone());
    }

    config
        .group_columns
        .iter()
        .map(|name| schema.index_of(name).map_err(DataFusionError::from))
        .collect()
}

fn resolve_physical_index(
    schema: &SchemaRef,
    name: &str,
    index: Option<usize>,
    role: &str,
) -> DFResult<usize> {
    if let Some(index) = index {
        if index >= schema.fields().len() {
            return plan_err!(
                "gap fill {role}_column_index {index} is out of bounds for {} fields",
                schema.fields().len()
            );
        }
        Ok(index)
    } else {
        schema.index_of(name).map_err(DataFusionError::from)
    }
}

fn display_column(name: &str, index: Option<usize>) -> String {
    match (name.is_empty(), index) {
        (false, _) => name.to_string(),
        (true, Some(index)) => format!("#{index}"),
        (true, None) => "<missing>".to_string(),
    }
}

fn display_groups(names: &[String], indices: &[usize]) -> String {
    if !names.is_empty() {
        format!("{names:?}")
    } else {
        format!("{indices:?}")
    }
}

#[derive(Clone, Eq, PartialEq, PartialOrd, Debug)]
struct GapFillNode {
    input: LogicalPlan,
    config: GapFillConfig,
}

impl Hash for GapFillNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.config.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for GapFillNode {
    fn name(&self) -> &str {
        "GapFill"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GapFill: method={:?}, time={}, value={}, groups={:?}, step_secs={}, limit_secs={}",
            self.config.method,
            display_column(&self.config.time_column, self.config.time_column_index),
            display_column(&self.config.value_column, self.config.value_column_index),
            display_groups(
                &self.config.group_columns,
                &self.config.group_column_indices
            ),
            self.config.step_secs,
            self.config.limit_secs
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        if inputs.len() != 1 {
            return plan_err!("GapFill expects one input, got {}", inputs.len());
        }
        Ok(Self {
            input: inputs[0].clone(),
            config: self.config.clone(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct GapFillExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for GapFillExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(gap_fill) = node.as_any().downcast_ref::<GapFillNode>() else {
            return Ok(None);
        };
        if physical_inputs.len() != 1 {
            return plan_err!("GapFill expects one physical input");
        }
        let input = Arc::clone(&physical_inputs[0]);
        let schema = input.schema();
        let group_indices = resolve_group_indices(&schema, &gap_fill.config)?;
        let time_index = resolve_physical_index(
            &schema,
            &gap_fill.config.time_column,
            gap_fill.config.time_column_index,
            "time",
        )?;
        let value_index = resolve_physical_index(
            &schema,
            &gap_fill.config.value_column,
            gap_fill.config.value_column_index,
            "value",
        )?;

        Ok(Some(Arc::new(GapFillExec::try_new(
            input,
            gap_fill.config.clone(),
            group_indices,
            time_index,
            value_index,
        )?)))
    }
}

#[derive(Debug)]
struct GapFillExec {
    input: Arc<dyn ExecutionPlan>,
    config: GapFillConfig,
    group_indices: Vec<usize>,
    time_index: usize,
    value_index: usize,
    cache: Arc<PlanProperties>,
}

impl GapFillExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        config: GapFillConfig,
        group_indices: Vec<usize>,
        time_index: usize,
        value_index: usize,
    ) -> DFResult<Self> {
        let schema = input.schema();
        validate_value_type(schema.field(value_index).data_type())?;
        time_unit_multiplier(schema.field(time_index).data_type())?;

        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            config,
            group_indices,
            time_index,
            value_index,
            cache,
        })
    }
}

impl DisplayAs for GapFillExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "GapFillExec: method={:?}, time={}, value={}, groups={:?}, step_secs={}, limit_secs={}",
                self.config.method,
                display_column(&self.config.time_column, self.config.time_column_index),
                display_column(&self.config.value_column, self.config.value_column_index),
                display_groups(
                    &self.config.group_columns,
                    &self.config.group_column_indices
                ),
                self.config.step_secs,
                self.config.limit_secs
            ),
            DisplayFormatType::TreeRender => write!(f, "GapFillExec"),
        }
    }
}

impl ExecutionPlan for GapFillExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        vec![datafusion::physical_plan::Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("GapFillExec expects one child, got {}", children.len());
        }
        Ok(Arc::new(GapFillExec::try_new(
            Arc::clone(&children[0]),
            self.config.clone(),
            self.group_indices.clone(),
            self.time_index,
            self.value_index,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return plan_err!("GapFillExec only has one partition, got partition {partition}");
        }

        let input = Arc::clone(&self.input);
        let schema = self.schema();
        let config = self.config.clone();
        let group_indices = self.group_indices.clone();
        let time_index = self.time_index;
        let value_index = self.value_index;
        let stream = futures::stream::once(async move {
            let input_stream = input.execute(0, context)?;
            let batches = collect(input_stream).await?;
            build_gap_filled_batch(
                schema,
                batches,
                config,
                group_indices,
                time_index,
                value_index,
            )
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream.try_filter(|batch| futures::future::ready(batch.num_rows() > 0)),
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

fn build_gap_filled_batch(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    config: GapFillConfig,
    group_indices: Vec<usize>,
    time_index: usize,
    value_index: usize,
) -> DFResult<RecordBatch> {
    if batches.is_empty() || batches.iter().all(|batch| batch.num_rows() == 0) {
        return Ok(RecordBatch::new_empty(schema));
    }

    let time_multiplier = time_unit_multiplier(schema.field(time_index).data_type())?;
    let step_units = config.step_secs * time_multiplier;
    let limit_units = config.limit_secs * time_multiplier;
    let end_units = config.end_secs.map(|end| end * time_multiplier);

    let mut rows = Vec::new();

    for batch in batches {
        for row_index in 0..batch.num_rows() {
            rows.push(RowState::try_from_batch(
                &batch,
                row_index,
                &group_indices,
                time_index,
                value_index,
            )?);
        }
    }
    rows.sort_by(compare_rows);

    let mut output_columns: Vec<Vec<ScalarValue>> =
        (0..schema.fields().len()).map(|_| Vec::new()).collect();
    let mut previous: Option<RowState> = None;

    for row in rows {
        if let Some(prev) = previous.as_ref() {
            if prev.group_values == row.group_values {
                append_gap_rows(
                    &schema,
                    &config,
                    prev,
                    Some(&row),
                    step_units,
                    limit_units,
                    &mut output_columns,
                    time_index,
                    value_index,
                )?;
            } else if let Some(end) = end_units {
                append_gap_rows(
                    &schema,
                    &config,
                    prev,
                    None,
                    step_units,
                    limit_units.min(end.saturating_sub(prev.time_units)),
                    &mut output_columns,
                    time_index,
                    value_index,
                )?;
            }
        }
        append_row(&row.values, &mut output_columns);
        previous = Some(row);
    }

    if let (Some(prev), Some(end)) = (previous.as_ref(), end_units) {
        append_gap_rows(
            &schema,
            &config,
            prev,
            None,
            step_units,
            limit_units.min(end.saturating_sub(prev.time_units)),
            &mut output_columns,
            time_index,
            value_index,
        )?;
    }

    let arrays = output_columns
        .into_iter()
        .map(|values| ScalarValue::iter_to_array(values.into_iter()))
        .collect::<DFResult<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).map_err(DataFusionError::from)
}

fn compare_rows(left: &RowState, right: &RowState) -> Ordering {
    for (left_group, right_group) in left.group_values.iter().zip(right.group_values.iter()) {
        match left_group
            .partial_cmp(right_group)
            .unwrap_or(Ordering::Equal)
        {
            Ordering::Equal => {}
            non_equal => return non_equal,
        }
    }
    left.time_units.cmp(&right.time_units)
}

struct RowState {
    values: Vec<ScalarValue>,
    group_values: Vec<ScalarValue>,
    time_units: i64,
    value: Option<f64>,
}

impl RowState {
    fn try_from_batch(
        batch: &RecordBatch,
        row_index: usize,
        group_indices: &[usize],
        time_index: usize,
        value_index: usize,
    ) -> DFResult<Self> {
        let values = (0..batch.num_columns())
            .map(|column_index| ScalarValue::try_from_array(batch.column(column_index), row_index))
            .collect::<DFResult<Vec<_>>>()?;
        let group_values = group_indices
            .iter()
            .map(|index| values[*index].clone())
            .collect();
        let time_units = time_to_units(&values[time_index])?;
        let value = value_to_f64(&values[value_index])?;
        Ok(Self {
            values,
            group_values,
            time_units,
            value,
        })
    }
}

fn append_gap_rows(
    schema: &SchemaRef,
    config: &GapFillConfig,
    previous: &RowState,
    next: Option<&RowState>,
    step_units: i64,
    limit_units: i64,
    output_columns: &mut [Vec<ScalarValue>],
    time_index: usize,
    value_index: usize,
) -> DFResult<()> {
    if limit_units <= 0 || previous.value.is_none() {
        return Ok(());
    }

    let mut time = previous.time_units.saturating_add(step_units);
    let exclusive_end = next.map(|row| row.time_units);
    while time - previous.time_units <= limit_units {
        if let Some(end) = exclusive_end
            && time >= end
        {
            break;
        }

        let Some(value) = interpolated_value(config.method, previous, next, time) else {
            break;
        };
        let mut row = generated_row(schema, previous, time, value, time_index, value_index)?;
        append_row(&row, output_columns);
        row.clear();
        time = time.saturating_add(step_units);
    }
    Ok(())
}

fn interpolated_value(
    method: FillMethod,
    previous: &RowState,
    next: Option<&RowState>,
    time: i64,
) -> Option<f64> {
    match method {
        FillMethod::Linear => {
            let previous_value = previous.value?;
            let Some(next) = next else {
                return Some(previous_value);
            };
            let next_value = next.value?;
            if next.time_units <= previous.time_units {
                return None;
            }
            let ratio = (time - previous.time_units) as f64
                / (next.time_units - previous.time_units) as f64;
            Some(previous_value + (next_value - previous_value) * ratio)
        }
        FillMethod::Last => previous.value,
        FillMethod::Zero => Some(0.0),
        FillMethod::Null => None,
    }
}

fn generated_row(
    schema: &SchemaRef,
    previous: &RowState,
    time: i64,
    value: f64,
    time_index: usize,
    value_index: usize,
) -> DFResult<Vec<ScalarValue>> {
    let mut row = Vec::with_capacity(schema.fields().len());
    for (column_index, field) in schema.fields().iter().enumerate() {
        if column_index == time_index {
            row.push(units_to_time(time, &previous.values[time_index])?);
        } else if column_index == value_index {
            row.push(f64_to_value(value, &previous.values[value_index])?);
        } else {
            row.push(previous.values[column_index].clone());
        }
        if !field.is_nullable() && row.last().is_some_and(ScalarValue::is_null) {
            return plan_err!(
                "gap fill generated NULL for non-nullable column '{}'",
                field.name()
            );
        }
    }
    Ok(row)
}

fn append_row(row: &[ScalarValue], output_columns: &mut [Vec<ScalarValue>]) {
    for (column, value) in output_columns.iter_mut().zip(row.iter()) {
        column.push(value.clone());
    }
}

fn validate_value_type(data_type: &arrow::datatypes::DataType) -> DFResult<()> {
    match data_type {
        arrow::datatypes::DataType::Float64 | arrow::datatypes::DataType::Float32 => Ok(()),
        other => plan_err!("gap fill value column must be Float32 or Float64, got {other}"),
    }
}

fn time_unit_multiplier(data_type: &arrow::datatypes::DataType) -> DFResult<i64> {
    match data_type {
        arrow::datatypes::DataType::Int64 | arrow::datatypes::DataType::UInt64 => Ok(1),
        arrow::datatypes::DataType::Timestamp(TimeUnit::Second, _) => Ok(1),
        arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(1_000),
        arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(1_000_000),
        arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(1_000_000_000),
        other => plan_err!("gap fill time column must be Int64, UInt64, or Timestamp, got {other}"),
    }
}

fn time_to_units(value: &ScalarValue) -> DFResult<i64> {
    match value {
        ScalarValue::Int64(Some(value))
        | ScalarValue::TimestampSecond(Some(value), _)
        | ScalarValue::TimestampMillisecond(Some(value), _)
        | ScalarValue::TimestampMicrosecond(Some(value), _)
        | ScalarValue::TimestampNanosecond(Some(value), _) => Ok(*value),
        ScalarValue::UInt64(Some(value)) => (*value).try_into().map_err(|_| {
            DataFusionError::Execution(format!("gap fill timestamp {value} overflows i64"))
        }),
        value if value.is_null() => plan_err!("gap fill time column must not contain NULLs"),
        other => plan_err!("unsupported gap fill time scalar {other:?}"),
    }
}

fn units_to_time(units: i64, template: &ScalarValue) -> DFResult<ScalarValue> {
    match template {
        ScalarValue::Int64(_) => Ok(ScalarValue::Int64(Some(units))),
        ScalarValue::UInt64(_) => Ok(ScalarValue::UInt64(Some(units.try_into().map_err(
            |_| DataFusionError::Execution(format!("gap fill timestamp {units} is negative")),
        )?))),
        ScalarValue::TimestampSecond(_, tz) => {
            Ok(ScalarValue::TimestampSecond(Some(units), tz.clone()))
        }
        ScalarValue::TimestampMillisecond(_, tz) => {
            Ok(ScalarValue::TimestampMillisecond(Some(units), tz.clone()))
        }
        ScalarValue::TimestampMicrosecond(_, tz) => {
            Ok(ScalarValue::TimestampMicrosecond(Some(units), tz.clone()))
        }
        ScalarValue::TimestampNanosecond(_, tz) => {
            Ok(ScalarValue::TimestampNanosecond(Some(units), tz.clone()))
        }
        other => plan_err!("unsupported gap fill time scalar {other:?}"),
    }
}

fn value_to_f64(value: &ScalarValue) -> DFResult<Option<f64>> {
    match value {
        ScalarValue::Float64(value) => Ok(*value),
        ScalarValue::Float32(value) => Ok(value.map(f64::from)),
        value if value.is_null() => Ok(None),
        other => plan_err!("unsupported gap fill value scalar {other:?}"),
    }
}

fn f64_to_value(value: f64, template: &ScalarValue) -> DFResult<ScalarValue> {
    match template {
        ScalarValue::Float64(_) => Ok(ScalarValue::Float64(Some(value))),
        ScalarValue::Float32(_) => Ok(ScalarValue::Float32(Some(value as f32))),
        other => plan_err!("unsupported gap fill value scalar {other:?}"),
    }
}
