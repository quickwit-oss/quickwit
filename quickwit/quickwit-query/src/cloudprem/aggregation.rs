use anyhow::Context;
use prost::Message;
use quickwit_proto::cloudprem::aggregation::Aggregation as AggregationNode;
use quickwit_proto::cloudprem::Aggregation as EvpAggregation;
use tantivy::aggregation::agg_req::{
    Aggregation as TantivyAggregation, AggregationVariants, Aggregations as TantivyAggregations,
};
use tantivy::aggregation::bucket;

use super::{missing_required, unsupported_query_error};
use crate::InvalidQuery;

fn assert_eq<T: PartialEq>(left: T, right: T, msg: &str) -> Result<(), InvalidQuery> {
    if left == right {
        Ok(())
    } else {
        Err(InvalidQuery::Other(anyhow::anyhow!(
            "assertion failed: {msg}"
        )))
    }
}

pub fn to_tantivy_aggregation(
    cloudprem_aggregation: EvpAggregation,
    start_ts_secs: i64,
) -> Result<TantivyAggregations, InvalidQuery> {
    let Some(aggregation) = cloudprem_aggregation.aggregation else {
        return Err(missing_required("aggregation"));
    };

    let mut tantivy_aggregations = TantivyAggregations::new();

    match aggregation {
        AggregationNode::AttributeGroupBy(_) => {
            return Err(unsupported_query_error("attribute group by"))
        }
        AggregationNode::TimeGroupBy(time_grouping) => {
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

            if tantivy_aggregations
                .insert(time_grouping.output.clone(), tantivy_agg)
                .is_some()
            {
                return Err(InvalidQuery::Other(anyhow::anyhow!(
                    "multiple aggs are named {}",
                    time_grouping.output
                )));
            }
        }
        AggregationNode::HistogramGroupBy(_) => {
            // this we don't care short term
            return Err(unsupported_query_error("histogram group by"));
        }
        AggregationNode::FlatFieldsGroupBy(flat_group_by) => {
            assert_eq(
                flat_group_by.fields.len(),
                flat_group_by.outputs.len(),
                "fields and output are different lenght",
            )?;
            match flat_group_by.fields.len() {
                0 => {
                    return Err(InvalidQuery::Other(anyhow::anyhow!(
                        "empty flat group field list"
                    )))
                }
                1 => (),
                _ => {
                    return Err(unsupported_query_error(
                        "only single field flat group by is supported",
                    ))
                }
            }

            let field = flat_group_by.fields[0].clone();
            let field_name = extract_field_name(field.expression.as_ref())?;
            // TODO we should check if we can get a type from java or not
            let missing = field.missing.map(tantivy::aggregation::Key::Str);
            let terms_agg = bucket::TermsAggregation {
                field: field_name,
                size: Some(flat_group_by.limit),
                segment_size: None,
                show_term_doc_count_error: None,
                min_doc_count: None,
                // TODO read order
                order: None,
                missing,
            };

            let Some(child) = flat_group_by.child else {
                return Err(missing_required("flat_fields.child"));
            };
            let tantivy_agg = TantivyAggregation {
                // TODO can we get a into() from *Aggregation to AggregationVariants instead?
                agg: AggregationVariants::Terms(terms_agg),
                sub_aggregation: to_tantivy_aggregation(*child, start_ts_secs)?,
            };
            let output = flat_group_by.outputs[0].clone();
            if tantivy_aggregations.insert(output, tantivy_agg).is_some() {
                return Err(InvalidQuery::Other(anyhow::anyhow!(
                    "multiple aggs are named {}",
                    flat_group_by.outputs[0]
                )));
            }
        }
        AggregationNode::Computes(_) => return Err(unsupported_query_error("computes")),
        AggregationNode::ListCompute(_) => return Err(unsupported_query_error("list compute")),
        AggregationNode::AnyCompute(_) => return Err(unsupported_query_error("any compute")),
        AggregationNode::MetricCompute(_) => return Err(unsupported_query_error("metric compute")),
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
