use std::collections::HashMap;

use anyhow::Context;
use prost::Message;
use quickwit_proto::cloudprem::aggregation::Aggregation as AggregationNode;
use quickwit_proto::cloudprem::{
    AggValue as EvpAggValue, Aggregation as EvpAggregation,
    AggregationResult as EvpAggregationResult,
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
            // TODO support more than count
            let output = metric_compute.id;
            if metric_compute.r#type != "COUNT" {
                return Err(InvalidQuery::Other(anyhow::anyhow!(
                    "unsupported metric aggregation: {}",
                    metric_compute.r#type
                )));
            }

            // we just want to count all matching docs, maybe setting a column with low cardinality
            // but always set is faster?
            let count_agg = metric::CountAggregation {
                field: "_i_dont_exist_".to_string(),
                missing: Some(1.0),
            };

            let tantivy_agg = TantivyAggregation {
                // TODO can we get a into() from *Aggregation to AggregationVariants instead?
                agg: AggregationVariants::Count(count_agg),
                sub_aggregation: HashMap::new(),
            };
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
) -> Result<Vec<EvpAggregationResult>, InvalidQuery> {
    let aggregations: TantivyAggregationResults = serde_json::from_str(result_json).unwrap();

    let iter = AggregationMapper::new(aggregations);

    Ok(iter.collect())
}

type MapIter = std::collections::hash_map::IntoIter<String, TantivyAggregationResult>;

enum ValueIter {
    Terms(std::vec::IntoIter<tantivy::aggregation::agg_result::BucketEntry>),
}

impl Iterator for ValueIter {
    type Item = (tantivy::aggregation::Key, TantivyAggregationResults);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ValueIter::Terms(terms) => {
                let entry = terms.next()?;
                Some((entry.key, entry.sub_aggregation))
            }
        }
    }
}

enum StackEntry {
    KeyIter(MapIter),
    ValueIter(ValueIter),
}

// TODO verify this does what we want. if we have a GroupBy(abc)+Count+Max aggregation,
// this will output [GroupBy(abc), Count] and [GroupBy(abc), Max], which may not be what we want
// (this should be correct for aggregations with a single metric though, which is enought for most
// of the ui) TODO that code might be better expressed by tree traversal+callback than trying to
// build an iterator
struct AggregationMapper {
    stack: Vec<StackEntry>,
    // at all time, each KeyIter entry on the stack must have a corresponding key,
    // possibly a dummy one before iteration starts
    keys: Vec<String>,
    // at all time, each ValueIter entry on the stack must have a corresponding value,
    // possibly a dummy one before iteration starts
    values: Vec<EvpAggValue>,
}

impl AggregationMapper {
    fn new(aggregations: TantivyAggregationResults) -> Self {
        AggregationMapper {
            stack: vec![StackEntry::KeyIter(aggregations.0.into_iter())],
            keys: vec![String::new()],
            values: Vec::new(),
        }
    }
}

impl Iterator for AggregationMapper {
    type Item = EvpAggregationResult;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let Some(currently_itered) = self.stack.last_mut() else {
                return None; // iteration ended
            };
            match currently_itered {
                StackEntry::KeyIter(iter) => {
                    // first, pop the key we placed at last iteration of this key iter
                    // on it's very first round, the value poped is a dummy empty string
                    self.keys.pop();
                    let Some((key, agg)) = iter.next() else {
                        self.stack.pop();
                        continue;
                    };
                    self.keys.push(key);
                    match agg {
                        TantivyAggregationResult::BucketResult(bucket_result) => {
                            use tantivy::aggregation::agg_result::BucketResult;
                            match bucket_result {
                                BucketResult::Range { .. } => todo!(),
                                BucketResult::Histogram { .. } => todo!(),
                                BucketResult::Terms { buckets, .. } => {
                                    let value_iter = ValueIter::Terms(buckets.into_iter());
                                    self.stack.push(StackEntry::ValueIter(value_iter));
                                    self.values.push(EvpAggValue { value: None });
                                }
                            }
                        }
                        TantivyAggregationResult::MetricResult(metric_result) => {
                            use tantivy::aggregation::agg_result::MetricResult;
                            let last_value = match metric_result {
                                MetricResult::Count(count) => {
                                    count.value.unwrap_or_default() as u64
                                }
                                _ => todo!(),
                            };
                            let mut values = self.values.clone();
                            values.push(u64_to_agg_value(last_value));
                            return Some(EvpAggregationResult {
                                key: self.keys.clone(),
                                value: values,
                            });
                        }
                    }
                }
                StackEntry::ValueIter(iter) => {
                    self.values.pop();
                    let Some((value, agg)) = iter.next() else {
                        self.stack.pop();
                        continue;
                    };
                    self.values.push(key_to_agg_value(value));

                    let key_iter = agg.0.into_iter();
                    self.stack.push(StackEntry::KeyIter(key_iter));
                    self.keys.push(String::new());
                }
            }
        }
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
