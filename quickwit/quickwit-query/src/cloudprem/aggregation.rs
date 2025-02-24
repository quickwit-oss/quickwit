use std::collections::HashMap;

use anyhow::Context;
use prost::Message;
use quickwit_proto::cloudprem::aggregation::Aggregation as AggregationNode;
use quickwit_proto::cloudprem::{
    AggValue as EvpAggValue, Aggregation as EvpAggregation,
    AggregationResult as EvpAggregationResult, CloudPremError,
};
use tantivy::aggregation::agg_req::{
    Aggregation as TantivyAggregation, AggregationVariants, Aggregations as TantivyAggregations,
};
use tantivy::aggregation::agg_result::{
    AggregationResult as TantivyAggregationResult, AggregationResults as TantivyAggregationResults,
};
use tantivy::aggregation::{bucket, metric};

use super::{missing_required, unsupported_query_error};
use crate::InvalidQuery;

pub fn to_tantivy_aggregation(
    cloudprem_aggregation: EvpAggregation,
    start_ts_secs: i64,
) -> Result<TantivyAggregations, InvalidQuery> {
    let Some(aggregation) = cloudprem_aggregation.aggregation else {
        return Err(missing_required("aggregation"));
    };

    let mut tantivy_aggregations = TantivyAggregations::new();

    match aggregation {
        AggregationNode::AttributeGroupBy(attribute_group_by) => {
            let (output, tantivy_agg) =
                handle_attribute_group_by(*attribute_group_by, start_ts_secs)?;
            if tantivy_aggregations
                .insert(output.clone(), tantivy_agg)
                .is_some()
            {
                return Err(InvalidQuery::Other(anyhow::anyhow!(
                    "multiple aggs are named {output}",
                )));
            }
        }
        AggregationNode::TimeGroupBy(time_grouping) => {
            let (output, tantivy_agg) = handle_time_group_by(*time_grouping, start_ts_secs)?;
            if tantivy_aggregations
                .insert(output.clone(), tantivy_agg)
                .is_some()
            {
                return Err(InvalidQuery::Other(anyhow::anyhow!(
                    "multiple aggs are named {output}",
                )));
            }
        }
        AggregationNode::HistogramGroupBy(_) => {
            return Err(unsupported_query_error("histogram group by"));
        }
        AggregationNode::FlatFieldsGroupBy(_) => {
            return Err(unsupported_query_error("flat fields group by"));
        }
        AggregationNode::Computes(computes) => {
            for agg in computes.aggregation {
                let aggregations = to_tantivy_aggregation(agg, start_ts_secs)?;
                for (output, aggregation) in aggregations {
                    if tantivy_aggregations
                        .insert(output.clone(), aggregation)
                        .is_some()
                    {
                        return Err(InvalidQuery::Other(anyhow::anyhow!(
                            "multiple aggs are named {output}"
                        )));
                    }
                }
            }
            for time_agg in computes.time_grouping {
                let (output, tantivy_agg) = handle_time_group_by(time_agg, start_ts_secs)?;
                if tantivy_aggregations
                    .insert(output.clone(), tantivy_agg)
                    .is_some()
                {
                    return Err(InvalidQuery::Other(anyhow::anyhow!(
                        "multiple aggs are named {output}"
                    )));
                }
            }
        }
        AggregationNode::ListCompute(_) => return Err(unsupported_query_error("list compute")),
        AggregationNode::AnyCompute(_) => return Err(unsupported_query_error("any compute")),
        AggregationNode::MetricCompute(metric_compute) => {
            let (output, tantivy_agg) = handle_metric_compute(metric_compute)?;
            if tantivy_aggregations
                .insert(output.clone(), tantivy_agg)
                .is_some()
            {
                return Err(InvalidQuery::Other(anyhow::anyhow!(
                    "multiple aggs are named {output}",
                )));
            }
        }
    }

    Ok(tantivy_aggregations)
}

fn extract_field_name(
    expression: Option<&quickwit_proto::cloudprem::ExpressionNode>,
) -> Result<String, InvalidQuery> {
    use quickwit_proto::cloudprem::calc_node::CalcNode as InnerCalcNode;
    let Some(expression) = expression else {
        return Err(missing_required("expression"));
    };
    let Some(ref calc_node_bytes) = expression.calc_node else {
        return Err(missing_required("calc_node"));
    };

    let calc_node = quickwit_proto::cloudprem::CalcNode::decode(calc_node_bytes.value.as_ref())
        .context("failed decoding CalcNode")?;

    let Some(calc_node) = calc_node.calc_node else {
        return Err(missing_required("calc_node.calc_node"));
    };

    let InnerCalcNode::FieldRef(field_ref) = calc_node else {
        return Err(unsupported_query_error(&format!(
            "calc_node isn't a field ref: {calc_node:?}"
        )));
    };

    let field_name = field_ref.field_name;
    if field_name.starts_with(['#', '@']) {
        return Err(unsupported_query_error(&format!(
            "non-trivial field name: {field_name}"
        )));
    }
    Ok(field_name)
}

fn handle_attribute_group_by(
    attribute_group_by: quickwit_proto::cloudprem::AttributeGroupBy,
    start_ts_secs: i64,
) -> Result<(String, TantivyAggregation), InvalidQuery> {
    let field_name = extract_field_name(attribute_group_by.expression.as_ref())?;
    // TODO we should check if we can get a type from java or not
    let missing = attribute_group_by
        .missing
        .map(tantivy::aggregation::Key::Str);
    let terms_agg = bucket::TermsAggregation {
        field: field_name.clone(),
        size: Some(attribute_group_by.limit),
        segment_size: None,
        show_term_doc_count_error: None,
        min_doc_count: None,
        // TODO read order
        order: None,
        missing,
    };

    let Some(child) = attribute_group_by.child else {
        return Err(missing_required("attribute_fields.child"));
    };
    let tantivy_agg = TantivyAggregation {
        // TODO can we get a into() from *Aggregation to AggregationVariants instead?
        agg: AggregationVariants::Terms(terms_agg),
        sub_aggregation: to_tantivy_aggregation(*child, start_ts_secs)?,
    };
    // TODO i'm still searching if there is a proper "output" field somewhere we should be using,
    // there really ought to be
    Ok((field_name, tantivy_agg))
}

fn handle_time_group_by(
    time_grouping: quickwit_proto::cloudprem::TimeGrouping,
    start_ts_secs: i64,
) -> Result<(String, TantivyAggregation), InvalidQuery> {
    let (interval, offset) = if let Some(interval_ns) = time_grouping.interval_ns {
        let interval_ms = interval_ns / 1_000_000;
        (format!("{interval_ms}ms"), None)
    } else if let Some(rollup) = time_grouping.rollup {
        rollup_to_interval(&rollup, &time_grouping.time_zone, start_ts_secs)?
    } else {
        return Err(missing_required("time_grouping.interval_ns"));
    };

    let terms_agg = bucket::DateHistogramAggregationReq {
        field: time_grouping.path, /* TODO is this correct?, or should we hardcode to
                                    * timestamp field? */
        format: None,
        fixed_interval: Some(interval),
        offset,
        min_doc_count: None,
        hard_bounds: None,
        extended_bounds: None,
        keyed: false,
        interval: None,
        calendar_interval: None,
    };

    let Some(child) = time_grouping.child else {
        return Err(missing_required("time_grouping.child"));
    };
    let tantivy_agg = TantivyAggregation {
        // TODO can we get a into() from *Aggregation to AggregationVariants instead?
        agg: AggregationVariants::DateHistogram(terms_agg),
        sub_aggregation: to_tantivy_aggregation(*child, start_ts_secs)?,
    };

    Ok((time_grouping.output, tantivy_agg))
}

fn handle_metric_compute(
    metric_compute: quickwit_proto::cloudprem::MetricCompute,
) -> Result<(String, TantivyAggregation), InvalidQuery> {
    // TODO support more than count
    if metric_compute.r#type != "COUNT" {
        return Err(InvalidQuery::Other(anyhow::anyhow!(
            "unsupported metric aggregation: {}",
            metric_compute.r#type
        )));
    }

    let count_agg = metric::CountAggregation {
        // this field is always set
        field: "status".to_string(),
        missing: None,
    };

    let tantivy_agg = TantivyAggregation {
        // TODO can we get a into() from *Aggregation to AggregationVariants instead?
        agg: AggregationVariants::Count(count_agg),
        sub_aggregation: HashMap::new(),
    };

    Ok((metric_compute.id, tantivy_agg))
}

/// this function attemps at converting a rollup to interval+offset
///
/// We do this because tantivy doesn't support calendar intervals.
/// This has number of caveats:
/// - year/month can't be supported at all
/// - week is kinda strange, must compensate for tz, assumes week starts on monday
/// - day/hours must compensate for tz
/// - minute is fine
///
/// Additionally, daylight saving is *not* supported
/// Leap seconds *should* be handled for free by the fact we use unix ts
fn rollup_to_interval(
    rollup: &str,
    timezone: &str,
    ts_secs: i64,
) -> Result<(String, Option<String>), InvalidQuery> {
    // TODO i made a misstake in the rollup protobuf, it looked like an enum in java, but it
    // actually has other fields, so this code is wrong until we fix java + protobuf
    let offset_seconds = timezone_and_ts_to_offset(timezone, ts_secs)?;

    let res = match rollup {
        "YEAR" | "MONTH" => {
            return Err(unsupported_query_error(&format!(
                "time aggregation with rollup {rollup}"
            )));
        }
        "WEEK" => {
            // 1970-01-01 was a thursday, we need to add 4 days to be on monday
            let offset = (4 * 24 * 60 * 60 + offset_seconds).rem_euclid(7 * 24 * 60 * 60);
            ("7d".to_string(), Some(format!("{offset}s")))
        }
        "DAY" => {
            let offset = offset_seconds.rem_euclid(24 * 60 * 60);
            ("1d".to_string(), Some(format!("{offset}s")))
        }
        "HOUR" => {
            let offset = offset_seconds.rem_euclid(60 * 60);
            ("1h".to_string(), Some(format!("{offset}s")))
        }
        "MINUTE" => {
            let offset = offset_seconds.rem_euclid(60);
            ("1m".to_string(), Some(format!("{offset}s")))
        }
        other => {
            return Err(anyhow::anyhow!("invalid rollup: {other}").into());
        }
    };
    Ok(res)
}

fn timezone_and_ts_to_offset(timezone: &str, ts_secs: i64) -> Result<i32, InvalidQuery> {
    use chrono::{DateTime, Offset, TimeZone};
    use chrono_tz::Tz;

    let tz: Tz = timezone.parse().context("failed to parse timezone")?;
    let dt = DateTime::from_timestamp(ts_secs, 0)
        .context("invalid timestamp")?
        .naive_utc();
    let offset = tz.offset_from_utc_datetime(&dt);
    Ok(offset.fix().local_minus_utc())
}

pub fn aggregation_result_to_proto(
    result_json: &str,
) -> Result<Vec<EvpAggregationResult>, CloudPremError> {
    let aggregations: TantivyAggregationResults =
        serde_json::from_str(result_json).map_err(|err| {
            CloudPremError::Internal(format!("failed to deserialize agg result: {err}"))
        })?;

    let mut mapper = ResultMapper {
        results: Vec::new(),
    };
    mapper.consume_agg(aggregations)?;
    Ok(mapper.results)
}

struct ResultMapper {
    results: Vec<EvpAggregationResult>,
}

impl ResultMapper {
    fn consume_agg(&mut self, agg_result: TantivyAggregationResults) -> Result<(), CloudPremError> {
        let state = EvpAggregationResult::default();
        self.consume_agg_aux(agg_result, &state)
    }

    fn consume_agg_aux(
        &mut self,
        agg_result: TantivyAggregationResults,
        state: &EvpAggregationResult,
    ) -> Result<(), CloudPremError> {
        let mut to_emit = None;
        for (key, agg) in agg_result.0 {
            match agg {
                TantivyAggregationResult::BucketResult(bucket_result) => {
                    use tantivy::aggregation::agg_result::BucketResult;
                    match bucket_result {
                        BucketResult::Range { .. } => return Err(CloudPremError::Unimplemented),
                        BucketResult::Histogram { .. } => {
                            return Err(CloudPremError::Unimplemented)
                        }
                        BucketResult::Terms { buckets, .. } => {
                            let mut mut_state = state.clone();
                            mut_state.key.push(key);
                            for bucket in buckets {
                                mut_state.value.push(key_to_agg_value(bucket.key));
                                self.consume_agg_aux(bucket.sub_aggregation, &mut_state)?;
                                mut_state.value.pop();
                            }
                        }
                    }
                }
                TantivyAggregationResult::MetricResult(metric_result) => {
                    use tantivy::aggregation::agg_result::MetricResult;

                    let last_value = match metric_result {
                        MetricResult::Count(count) => count.value.unwrap_or_default() as u64,
                        _ => return Err(CloudPremError::Unimplemented),
                    };
                    let to_emit_mut = to_emit.get_or_insert_with(|| state.clone());
                    to_emit_mut.key.push(key);
                    to_emit_mut.value.push(u64_to_agg_value(last_value));
                }
            }
        }
        if let Some(to_emit) = to_emit {
            self.results.push(to_emit);
        }
        Ok(())
    }
}

fn u64_to_agg_value(val: u64) -> EvpAggValue {
    EvpAggValue {
        value: Some(quickwit_proto::cloudprem::agg_value::Value::Uint64Value(
            val,
        )),
    }
}

fn key_to_agg_value(val: tantivy::aggregation::Key) -> EvpAggValue {
    use quickwit_proto::cloudprem::agg_value::Value;
    use tantivy::aggregation::Key;
    let evp_val = match val {
        Key::Str(s) => Value::StringValue(s),
        Key::I64(int) => Value::Int64Value(int),
        Key::U64(uint) => Value::Uint64Value(uint),
        Key::F64(int) => Value::Float64Value(int),
    };
    EvpAggValue {
        value: Some(evp_val),
    }
}
