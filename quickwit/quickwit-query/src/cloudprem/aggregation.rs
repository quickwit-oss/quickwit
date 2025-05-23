use std::collections::HashMap;

use anyhow::Context;
use prost::Message;
use quickwit_proto::cloudprem::aggregation::Aggregation as AggregationNode;
use quickwit_proto::cloudprem::rollup::RollupType;
use quickwit_proto::cloudprem::{
    AggValue as EvpAggValue, Aggregation as EvpAggregation,
    AggregationResult as EvpAggregationResult, CloudPremError, Rollup,
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
use crate::aggregations::AggregationResults as QuickwitAggregationResults;

const CALC_NODE_TYPE_URL: &str = "type.googleapis.com/calcfieldspb.CalcNode";

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

    if calc_node_bytes.type_url != CALC_NODE_TYPE_URL {
        return Err(unsupported_query_error(&format!(
            "calc_node uses unknown type '{}'",
            calc_node_bytes.type_url
        )));
    }
    // TODO this can be cleaner once we upgrade to prost 0.12+
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
    let (interval, offset): (String, Option<String>) =
        if let Some(interval_ns) = time_grouping.interval_ns {
            let interval_ms = interval_ns / 1_000_000;
            (format!("{interval_ms}ms"), None)
        } else if let Some(rollup) = time_grouping.rollup {
            rollup_to_interval(&rollup, start_ts_secs)?
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
    let field = extract_field_name(metric_compute.expression.as_ref())?;

    // TODO support more aggregations?
    let agg = match metric_compute.r#type.as_str() {
        "COUNT" => {
            let count_agg = metric::CountAggregation {
                // this field is always set, and we expect it to be almost always downloaded anyway
                field: "timestamp".to_string(),
                missing: Some(1.0),
            };
            // TODO can we get a into() from *Aggregation to AggregationVariants instead?
            AggregationVariants::Count(count_agg)
        }
        "CARDINALITY_SKETCH" => {
            let cardinality = metric::CardinalityAggregationReq {
                field,
                missing: None,
            };
            AggregationVariants::Cardinality(cardinality)
        }
        "SUM" => {
            let sum = metric::SumAggregation {
                field,
                missing: None,
            };
            AggregationVariants::Sum(sum)
        }
        "MAX" => {
            let max = metric::MaxAggregation {
                field,
                missing: None,
            };
            AggregationVariants::Max(max)
        }
        "MIN" => {
            let min = metric::MinAggregation {
                field,
                missing: None,
            };
            AggregationVariants::Min(min)
        }
        "AVG" => {
            let avg = metric::AverageAggregation {
                field,
                missing: None,
            };
            AggregationVariants::Average(avg)
        }
        other => {
            return Err(InvalidQuery::Other(anyhow::anyhow!(
                "unsupported metric aggregation: {other:?}"
            )));
        }
    };

    let tantivy_agg = TantivyAggregation {
        // TODO can we get a into() from *Aggregation to AggregationVariants instead?
        agg,
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
    rollup: &Rollup,
    ts_secs: i64,
) -> Result<(String, Option<String>), InvalidQuery> {
    let offset_seconds = timezone_and_ts_to_offset(&rollup.time_zone, ts_secs)?;

    let (base_interval_sec, base_offset_sec) = match rollup.r#type() {
        RollupType::Invalid | RollupType::Year | RollupType::Month => {
            return Err(unsupported_query_error(&format!(
                "time aggregation with rollup {rollup:?}"
            )));
        }
        RollupType::Week => {
            // 1970-01-01 was a thursday, we need to add 4 days to be on monday
            let offset = 4 * 24 * 3600 + offset_seconds;
            (7 * 24 * 3600, offset)
        }
        RollupType::Day => (24 * 3600, offset_seconds),
        RollupType::Hour => (3600, offset_seconds),
        RollupType::Minute => (60, offset_seconds),
    };
    let interval_sec = base_interval_sec * rollup.quantity;
    // cast is safe for intervals under 68 years
    // TODO handle alignment, unsure what the syntax is
    let offset_sec = base_offset_sec.rem_euclid(interval_sec as i32);

    Ok((format!("{interval_sec}s"), Some(format!("{offset_sec}s"))))
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
    result_postcard: &[u8],
) -> Result<Vec<EvpAggregationResult>, CloudPremError> {
    let aggregations: QuickwitAggregationResults =
        postcard::from_bytes(result_postcard).map_err(|err| {
            CloudPremError::Internal(format!("failed to deserialize agg result: {err}"))
        })?;

    let mut mapper = ResultMapper {
        results: Vec::new(),
    };
    mapper.consume_agg(aggregations.into())?;
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
        for (_key, agg) in agg_result.0 {
            match agg {
                TantivyAggregationResult::BucketResult(bucket_result) => {
                    use tantivy::aggregation::agg_result::BucketResult;
                    match bucket_result {
                        BucketResult::Range { buckets } => {
                            let mut mut_state = state.clone();
                            for bucket in bucket_iter(buckets) {
                                mut_state.key.push(bucket.key.to_string());
                                self.consume_agg_aux(bucket.sub_aggregation, &mut_state)?;
                                mut_state.key.pop();
                            }
                        }
                        BucketResult::Histogram { buckets } => {
                            let mut mut_state = state.clone();
                            for bucket in bucket_iter(buckets) {
                                mut_state.key.push(
                                    bucket
                                        .key_as_string
                                        .unwrap_or_else(|| bucket.key.to_string()),
                                );
                                self.consume_agg_aux(bucket.sub_aggregation, &mut_state)?;
                                mut_state.key.pop();
                            }
                        }
                        BucketResult::Terms { buckets, .. } => {
                            let mut mut_state = state.clone();
                            for bucket in buckets {
                                mut_state.key.push(bucket.key.to_string());
                                self.consume_agg_aux(bucket.sub_aggregation, &mut_state)?;
                                mut_state.key.pop();
                            }
                        }
                    }
                }
                TantivyAggregationResult::MetricResult(metric_result) => {
                    use tantivy::aggregation::agg_result::MetricResult;
                    // TODO we need to guarantee the order of append somehow

                    let to_emit_mut = to_emit.get_or_insert_with(|| state.clone());

                    match metric_result {
                        MetricResult::Count(metric_res)
                        | MetricResult::Min(metric_res)
                        | MetricResult::Max(metric_res)
                        | MetricResult::Sum(metric_res) => {
                            to_emit_mut.value.push(u64_to_agg_value(
                                metric_res.value.unwrap_or_default() as u64,
                            ));
                        }
                        MetricResult::Cardinality(cardinality) => {
                            to_emit_mut.value.push(generate_sketch(
                                cardinality.value.unwrap_or_default() as u64,
                            ));
                        }
                        MetricResult::Average(avg) => {
                            to_emit_mut
                                .value
                                .push(generate_avg(avg.value.unwrap_or_default()));
                        }
                        _ => return Err(CloudPremError::Unimplemented),
                    };
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

fn bucket_iter<T>(
    buckets: tantivy::aggregation::agg_result::BucketEntries<T>,
) -> impl Iterator<Item = T> {
    use either::Either;
    use tantivy::aggregation::agg_result::BucketEntries;
    match buckets {
        BucketEntries::Vec(vec) => Either::Left(vec.into_iter()),
        BucketEntries::HashMap(map) => Either::Right(map.into_values()),
    }
}

fn generate_sketch(count: u64) -> EvpAggValue {
    const VERSION: u8 = 0x10;
    const EMPTY: u8 = 0x01;
    const EXPLICIT: u8 = 0x02;
    // const SPARSE: u8 = 0x01;
    // const FULL: u8 = 0x01;

    const WIDTH_5_COUNT_2_11: u8 = 0b100_01011;
    #[allow(clippy::unusual_byte_groupings)]
    const CUTOFF: u8 = 0b0_1_111111; // pad, sparseEnabled, explicitCUtoff=63 (implementation defined)

    let hll = if count == 0 || count > 256 {
        vec![VERSION | EMPTY, WIDTH_5_COUNT_2_11, CUTOFF]
    } else {
        let mut res: Vec<u8> = Vec::with_capacity(count as usize * 8 + 3);
        res.extend_from_slice(&[VERSION | EXPLICIT, WIDTH_5_COUNT_2_11, CUTOFF]);
        for i in 0..count {
            res.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, i as u8]);
        }
        res
    };

    EvpAggValue {
        value: Some(quickwit_proto::cloudprem::agg_value::Value::HllValue(hll)),
    }
}

fn generate_avg(avg_float: f64) -> EvpAggValue {
    // this result is non-mergeable, but it's alright otherwise
    let avg_value = quickwit_proto::cloudprem::Avg {
        sum: avg_float,
        count: 1,
    };
    EvpAggValue {
        value: Some(quickwit_proto::cloudprem::agg_value::Value::AvgValue(
            avg_value,
        )),
    }
}

#[cfg(test)]
mod test_helpers {

    use quickwit_proto::cloudprem::AggValue;
    use quickwit_proto::cloudprem::agg_value::Value;

    // this module is here to make tests easier to read and write
    pub trait IntoValue {
        fn to_value(&self) -> AggValue {
            AggValue {
                value: Some(self.val()),
            }
        }
        fn val(&self) -> Value;
    }

    impl IntoValue for &str {
        fn val(&self) -> Value {
            Value::StringValue(self.to_string())
        }
    }
    impl IntoValue for u64 {
        fn val(&self) -> Value {
            Value::Uint64Value(*self)
        }
    }
    impl IntoValue for i64 {
        fn val(&self) -> Value {
            Value::Int64Value(*self)
        }
    }
    impl IntoValue for f64 {
        fn val(&self) -> Value {
            Value::Float64Value(*self)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use prost_types::Any;
    use quickwit_proto::cloudprem::aggregation::Aggregation as AggregationEnum;
    use quickwit_proto::cloudprem::sort_by_expr_and_agg::SortType;
    use quickwit_proto::cloudprem::*;
    use tantivy::aggregation::agg_req::{Aggregation as TantivyAgg, AggregationVariants};
    use tantivy::aggregation::bucket::*;
    use tantivy::aggregation::metric::*;

    use super::test_helpers::IntoValue;
    use super::{
        aggregation_result_to_proto, generate_sketch, rollup_to_interval, to_tantivy_aggregation,
    };
    use crate::aggregations::{
        AggregationResult as QuickwitAggregationResult,
        AggregationResults as QuickwitAggregationResults, BucketEntries, BucketEntry, BucketResult,
        Key, MetricResult,
    };

    #[test]
    fn test_count_request() {
        let evp_agg = Aggregation {
            aggregation: Some(AggregationEnum::Computes(Computes {
                aggregation: vec![Aggregation {
                    aggregation: Some(AggregationEnum::MetricCompute(MetricCompute {
                        expression: Some(ExpressionNode {
                            calc_node: Some(Any {
                                type_url: "type.googleapis.com/calcfieldspb.CalcNode".to_string(),
                                value: vec![18, 7, 10, 5, 99, 111, 117, 110, 116],
                            }),
                        }),
                        id: "count:count".to_string(),
                        r#type: "COUNT".to_string(),
                    })),
                }],
                time_grouping: vec![],
            })),
        };

        let expected = [(
            "count:count".to_string(),
            TantivyAgg {
                agg: AggregationVariants::Count(CountAggregation {
                    field: "timestamp".to_string(),
                    missing: Some(1.0),
                }),
                sub_aggregation: HashMap::new(),
            },
        )]
        .into_iter()
        .collect();

        let res = to_tantivy_aggregation(evp_agg, 0).unwrap();

        assert_eq!(res, expected);
    }

    #[test]
    fn test_count_by_facet_request() {
        let evp_agg = Aggregation {
            aggregation: Some(AggregationEnum::AttributeGroupBy(Box::new(
                AttributeGroupBy {
                    expression: Some(ExpressionNode {
                        calc_node: Some(Any {
                            type_url: "type.googleapis.com/calcfieldspb.CalcNode".to_string(),
                            value: vec![18, 8, 10, 6, 115, 116, 97, 116, 117, 115],
                        }),
                    }),
                    limit: 50,
                    sort: Some(SortByExprAndAgg {
                        ascending: false,
                        expr_and_agg: Some(ExprAndAgg {
                            expr: Some(ExpressionNode {
                                calc_node: Some(Any {
                                    type_url: "type.googleapis.com/calcfieldspb.CalcNode"
                                        .to_string(),
                                    value: vec![18, 7, 10, 5, 99, 111, 117, 110, 116],
                                }),
                            }),
                            agg_function: "count".to_string(),
                        }),
                        r#type: SortType::Metric as i32,
                    }),
                    missing: None,
                    total: None,
                    child: Some(Box::new(Aggregation {
                        aggregation: Some(AggregationEnum::Computes(Computes {
                            aggregation: vec![Aggregation {
                                aggregation: Some(AggregationEnum::MetricCompute(MetricCompute {
                                    expression: Some(ExpressionNode {
                                        calc_node: Some(Any {
                                            type_url: "type.googleapis.com/calcfieldspb.CalcNode"
                                                .to_string(),
                                            value: vec![18, 7, 10, 5, 99, 111, 117, 110, 116],
                                        }),
                                    }),
                                    id: "count:count".to_string(),
                                    r#type: "COUNT".to_string(),
                                })),
                            }],
                            time_grouping: vec![],
                        })),
                    })),
                },
            ))),
        };

        let expected = [(
            "status".to_string(),
            TantivyAgg {
                agg: AggregationVariants::Terms(TermsAggregation {
                    field: "status".to_string(),
                    size: Some(50),
                    segment_size: None,
                    show_term_doc_count_error: None,
                    min_doc_count: None,
                    order: None,
                    missing: None,
                }),
                sub_aggregation: [(
                    "count:count".to_string(),
                    TantivyAgg {
                        agg: AggregationVariants::Count(CountAggregation {
                            field: "timestamp".to_string(),
                            missing: Some(1.0),
                        }),
                        sub_aggregation: HashMap::new(),
                    },
                )]
                .into_iter()
                .collect(),
            },
        )]
        .into_iter()
        .collect();

        let res = to_tantivy_aggregation(evp_agg, 0).unwrap();

        assert_eq!(res, expected);
    }

    #[test]
    fn test_timeline_aggregation_request() {
        let evp_agg = Aggregation {
            aggregation: Some(AggregationEnum::AttributeGroupBy(Box::new(
                AttributeGroupBy {
                    expression: Some(ExpressionNode {
                        calc_node: Some(Any {
                            type_url: "type.googleapis.com/calcfieldspb.CalcNode".to_string(),
                            value: vec![18, 8, 10, 6, 115, 116, 97, 116, 117, 115],
                        }),
                    }),
                    limit: 10,
                    sort: Some(SortByExprAndAgg {
                        ascending: false,
                        expr_and_agg: Some(ExprAndAgg {
                            expr: Some(ExpressionNode {
                                calc_node: Some(Any {
                                    type_url: "type.googleapis.com/calcfieldspb.CalcNode"
                                        .to_string(),
                                    value: vec![18, 7, 10, 5, 99, 111, 117, 110, 116],
                                }),
                            }),
                            agg_function: "count".to_string(),
                        }),
                        r#type: SortType::Metric as i32,
                    }),
                    missing: None,
                    total: None,
                    child: Some(Box::new(Aggregation {
                        aggregation: Some(AggregationEnum::Computes(Computes {
                            aggregation: vec![],
                            time_grouping: vec![TimeGrouping {
                                output: "time:28800000".to_string(),
                                path: "timestamp".to_string(),
                                time_zone: "Z".to_string(),
                                interval_ns: Some(28800000000000),
                                rollup: None,
                                child: Some(Box::new(Aggregation {
                                    aggregation: Some(AggregationEnum::Computes(Computes {
                                        aggregation: vec![Aggregation {
                                            aggregation: Some(AggregationEnum::MetricCompute(
                                                MetricCompute {
                                                    expression: Some(ExpressionNode {
                                                        calc_node: Some(Any {
                                                            type_url: "type.googleapis.com/\
                                                                       calcfieldspb.CalcNode"
                                                                .to_string(),
                                                            value: vec![
                                                                18, 7, 10, 5, 99, 111, 117, 110,
                                                                116,
                                                            ],
                                                        }),
                                                    }),
                                                    id: "count:count:timeseries:28800000"
                                                        .to_string(),
                                                    r#type: "COUNT".to_string(),
                                                },
                                            )),
                                        }],
                                        time_grouping: vec![],
                                    })),
                                })),
                            }],
                        })),
                    })),
                },
            ))),
        };
        let expected = [(
            "status".to_string(),
            TantivyAgg {
                agg: AggregationVariants::Terms(TermsAggregation {
                    field: "status".to_string(),
                    size: Some(10),
                    segment_size: None,
                    show_term_doc_count_error: None,
                    min_doc_count: None,
                    order: None,
                    missing: None,
                }),
                sub_aggregation: [(
                    "time:28800000".to_string(),
                    TantivyAgg {
                        agg: AggregationVariants::DateHistogram(DateHistogramAggregationReq {
                            interval: None,
                            calendar_interval: None,
                            field: "timestamp".to_string(),
                            format: None,
                            fixed_interval: Some("28800000ms".to_string()),
                            offset: None,
                            min_doc_count: None,
                            hard_bounds: None,
                            extended_bounds: None,
                            keyed: false,
                        }),
                        sub_aggregation: [(
                            "count:count:timeseries:28800000".to_string(),
                            TantivyAgg {
                                agg: AggregationVariants::Count(CountAggregation {
                                    field: "timestamp".to_string(),
                                    missing: Some(1.0),
                                }),
                                sub_aggregation: HashMap::new(),
                            },
                        )]
                        .into_iter()
                        .collect(),
                    },
                )]
                .into_iter()
                .collect(),
            },
        )]
        .into_iter()
        .collect();

        let res = to_tantivy_aggregation(evp_agg, 0).unwrap();

        assert_eq!(res, expected);
    }

    fn metric<F, V, U>(key: &str, metric_kind: F, value: V) -> QuickwitAggregationResults
    where
        F: Fn(U) -> MetricResult,
        U: From<V>,
    {
        QuickwitAggregationResults(vec![(
            key.to_string(),
            QuickwitAggregationResult::MetricResult(metric_kind(value.into())),
        )])
    }

    fn terms(key: &str, buckets: Vec<BucketEntry>) -> QuickwitAggregationResults {
        QuickwitAggregationResults(vec![(
            key.to_string(),
            QuickwitAggregationResult::BucketResult(BucketResult::Terms {
                buckets,
                sum_other_doc_count: 0,
                doc_count_error_upper_bound: Some(0),
            }),
        )])
    }

    fn histogram(key: &str, buckets: Vec<BucketEntry>) -> QuickwitAggregationResults {
        QuickwitAggregationResults(vec![(
            key.to_string(),
            QuickwitAggregationResult::BucketResult(BucketResult::Histogram {
                buckets: BucketEntries::Vec(buckets),
            }),
        )])
    }

    #[test]
    fn test_count_response() {
        let qw_reply =
            postcard::to_stdvec(&metric("count:count", MetricResult::Count, 2.0)).unwrap();
        let expected = vec![AggregationResult {
            key: vec![],
            value: vec![2u64.to_value()],
        }];

        let res = aggregation_result_to_proto(&qw_reply).unwrap();
        assert_eq!(res, expected);
    }

    #[test]
    fn test_map_reply_admin_test() {
        let qw_reply = postcard::to_stdvec(&terms(
            "status",
            vec![BucketEntry {
                key_as_string: None,
                key: Key::Str("info".to_string()),
                doc_count: 2,
                sub_aggregation: histogram(
                    "time:28800000",
                    vec![BucketEntry {
                        key_as_string: Some("2025-01-30T08:00:00Z".to_string()),
                        key: Key::F64(1738224000000.0),
                        doc_count: 2,
                        sub_aggregation: metric(
                            "count:count:timeseries:28800000",
                            MetricResult::Count,
                            2.0,
                        ),
                    }],
                ),
            }],
        ))
        .unwrap();
        let expected = vec![AggregationResult {
            key: vec!["info".to_string(), "2025-01-30T08:00:00Z".to_string()],
            value: vec![2u64.to_value()],
        }];

        let res = aggregation_result_to_proto(&qw_reply).unwrap();
        assert_eq!(res, expected);
    }

    #[test]
    fn test_generate_sketch() {
        {
            let sketch_empty = generate_sketch(0);
            let quickwit_proto::cloudprem::agg_value::Value::HllValue(buffer) =
                sketch_empty.value.unwrap()
            else {
                panic!();
            };
            assert_eq!(buffer[0], 0x11); // version+empty
            assert_eq!(buffer.len(), 3);
            // other bytes are configuration only meaningful on sparse/full variants
        }
        {
            let sketch_too_full = generate_sketch(1024);
            let quickwit_proto::cloudprem::agg_value::Value::HllValue(buffer) =
                sketch_too_full.value.unwrap()
            else {
                panic!();
            };
            assert_eq!(buffer[0], 0x11); // version+empty
            assert_eq!(buffer.len(), 3);
        }
        {
            let sketch_too_full = generate_sketch(5);
            let quickwit_proto::cloudprem::agg_value::Value::HllValue(buffer) =
                sketch_too_full.value.unwrap()
            else {
                panic!();
            };
            assert_eq!(buffer[0], 0x12); // version+explicit
            assert_eq!(buffer.len(), 3 + 5 * 8);
            for i in 0..5 {
                for j in 0..6 {
                    assert_eq!(buffer[3 + i * 8 + j], 0);
                }
                assert_eq!(buffer[3 + i * 8 + 7], i as u8);
            }
        }
    }

    #[test]
    fn test_rollup() {
        rollup_to_interval(
            &Rollup {
                r#type: 0, // invalid
                quantity: 3,
                time_zone: "UTC".to_string(),
                alignment: None,
            },
            0,
        )
        .unwrap_err();
        rollup_to_interval(
            &Rollup {
                r#type: 1, // year
                quantity: 3,
                time_zone: "UTC".to_string(),
                alignment: None,
            },
            0,
        )
        .unwrap_err();
        rollup_to_interval(
            &Rollup {
                r#type: 2, // month
                quantity: 3,
                time_zone: "UTC".to_string(),
                alignment: None,
            },
            0,
        )
        .unwrap_err();

        let (interval, offset) = rollup_to_interval(
            &Rollup {
                r#type: 3, // week
                quantity: 3,
                time_zone: "UTC".to_string(),
                alignment: None,
            },
            0,
        )
        .unwrap();
        assert_eq!(interval, format!("{}s", 3 * 7 * 24 * 60 * 60));
        assert_eq!(offset.unwrap(), format!("{}s", 4 * 24 * 60 * 60)); // 4 days from thurdsay to monday

        let (interval, offset) = rollup_to_interval(
            &Rollup {
                r#type: 4, // day
                quantity: 3,
                time_zone: "UTC".to_string(),
                alignment: None,
            },
            0,
        )
        .unwrap();
        assert_eq!(interval, format!("{}s", 3 * 24 * 60 * 60));
        assert_eq!(offset.unwrap(), "0s");

        let (interval, offset) = rollup_to_interval(
            &Rollup {
                r#type: 5, // hour
                quantity: 3,
                time_zone: "UTC".to_string(),
                alignment: None,
            },
            0,
        )
        .unwrap();
        assert_eq!(interval, format!("{}s", 3 * 60 * 60));
        assert_eq!(offset.unwrap(), "0s");

        let (interval, offset) = rollup_to_interval(
            &Rollup {
                r#type: 6, // minute
                quantity: 3,
                time_zone: "UTC".to_string(),
                alignment: None,
            },
            0,
        )
        .unwrap();
        assert_eq!(interval, format!("{}s", 3 * 60));
        assert_eq!(offset.unwrap(), "0s");
    }
}
