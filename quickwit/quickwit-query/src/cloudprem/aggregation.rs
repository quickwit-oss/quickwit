use std::collections::hash_map::Entry;

use anyhow::Context;
use prost::Message;
use quickwit_proto::cloudprem::aggregation::Aggregation as AggregationNode;
use quickwit_proto::cloudprem::rollup::RollupType;
use quickwit_proto::cloudprem::{
    AggValue as EvpAggValue, Aggregation as EvpAggregation,
    AggregationResult as EvpAggregationResult, CloudPremError, ExpressionNode, Rollup,
    TimeGrouping,
};
use tantivy::aggregation::agg_req::{
    Aggregation as TantivyAggregation, AggregationVariants, Aggregations as TantivyAggregations,
};
use tantivy::aggregation::agg_result::{
    AggregationResult as TantivyAggregationResult, AggregationResults as TantivyAggregationResults,
};
use tantivy::aggregation::bucket::{CustomOrder, IncludeExcludeParam, Order, OrderTarget};
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::{bucket, metric};

use super::{internal_error, missing_required, unsupported_query_error};
use crate::InvalidQuery;
use crate::aggregations::AggregationResults as QuickwitAggregationResults;

const CALC_NODE_TYPE_URL: &str = "type.googleapis.com/calcfieldspb.CalcNode";

struct AggregationMapper {
    start_ts_secs: i64,
}

pub fn to_tantivy_aggregation(
    cloudprem_aggregation: EvpAggregation,
    start_ts_secs: i64,
) -> Result<TantivyAggregations, InvalidQuery> {
    AggregationMapper { start_ts_secs }.handle_generic_aggregation(cloudprem_aggregation)
}

fn sanitize_metric_id(aggregation_key: &mut String) {
    *aggregation_key = aggregation_key.replace(".", "_DOT_");
}

fn sanitize_time_grouping(time_grouping: &mut TimeGrouping) {
    if let Some(child) = time_grouping.child.as_mut() {
        sanitize_metric_id_aggregations(child);
    }
    sanitize_metric_id(&mut time_grouping.output);
}

/// This function renames all metric ids in the aggregation, replace `.` by
/// __DOT__. Elasticsearch's DSL uses dot as a way to address
/// nested fields. tantivy implements partially that logic (in a broken way, but well).
pub fn sanitize_metric_id_aggregations(aggregation: &mut EvpAggregation) {
    let Some(agg) = aggregation.aggregation.as_mut() else {
        return;
    };
    match agg {
        AggregationNode::AttributeGroupBy(group_by) => {
            if let Some(child) = &mut group_by.child {
                sanitize_metric_id_aggregations(&mut *child);
            }
        }
        AggregationNode::Computes(computes) => {
            for compute in &mut computes.aggregation {
                sanitize_metric_id_aggregations(compute);
            }
            for time_grouping in &mut computes.time_grouping {
                sanitize_time_grouping(time_grouping);
            }
        }
        AggregationNode::TimeGroupBy(time_group_by) => {
            sanitize_time_grouping(&mut *time_group_by);
        }
        AggregationNode::MetricCompute(metric_compute) => {
            sanitize_metric_id(&mut metric_compute.id);
        }
        AggregationNode::FlatFieldsGroupBy(_)
        | AggregationNode::ListCompute(_)
        | AggregationNode::AnyCompute(_)
        | AggregationNode::HistogramGroupBy(_) => {}
    };
}

/// Add the given aggregation if it is not already present, and
/// - if present, returns the name of the aggregation
/// - if absent, adds the aggregation with the `name_if_absent` and return that.
fn add_aggregation_if_absent(
    group_by_sort_agg: TantivyAggregation,
    sub_aggregations: &mut TantivyAggregations,
    name_if_absent: String,
) -> String {
    for (key, sub_agg) in sub_aggregations.iter() {
        if sub_agg == &group_by_sort_agg {
            return key.clone();
        }
    }
    sub_aggregations.insert(name_if_absent.clone(), group_by_sort_agg);
    name_if_absent
}

/// Here we build term group by sort order objects.
///
/// If this order is based on a metric, we check if it is already present in the sub-aggregations
/// and reuse it. It is usually present, because event query requests it as a regular metric to
/// be able to merge results together.
///
/// If it is not present (for instance, in presence of nested group bys) event query
/// will add the metric as a leaf metric.
fn build_group_by_sort_order(
    sort: &Option<quickwit_proto::cloudprem::SortByExprAndAgg>,
    sub_aggs: &mut TantivyAggregations,
) -> Result<Option<CustomOrder>, InvalidQuery> {
    let Some(sort) = sort.as_ref() else {
        return Ok(None);
    };
    let Some(sort_expr_and_agg) = sort.expr_and_agg.as_ref() else {
        return Ok(None);
    };
    let order = if sort.ascending {
        Order::Asc
    } else {
        Order::Desc
    };
    let agg_type = sort_expr_and_agg.agg_function.to_ascii_uppercase();
    let metric_opt = build_metric(&sort_expr_and_agg.expr, &agg_type)?;
    let Some(metric) = metric_opt else {
        if order == Order::Desc {
            // This is the default anyway.
            return Ok(None);
        }
        return Ok(Some(CustomOrder {
            target: bucket::OrderTarget::Count,
            order,
        }));
    };
    let metric_name = add_aggregation_if_absent(metric, sub_aggs, "__sort_by_key".to_string());
    Ok(Some(CustomOrder {
        target: OrderTarget::SubAggregation(metric_name),
        order,
    }))
}

impl AggregationMapper {
    fn handle_generic_aggregation(
        &self,
        cloudprem_aggregation: EvpAggregation,
    ) -> Result<TantivyAggregations, InvalidQuery> {
        let Some(aggregation) = cloudprem_aggregation.aggregation else {
            return Err(missing_required("aggregation"));
        };

        let mut tantivy_aggregations_per_key: Vec<(String, TantivyAggregation)> = Vec::new();

        match aggregation {
            AggregationNode::AttributeGroupBy(attribute_group_by) => {
                tantivy_aggregations_per_key
                    .extend(self.handle_attribute_group_by(*attribute_group_by)?);
            }
            AggregationNode::TimeGroupBy(time_grouping) => {
                tantivy_aggregations_per_key.push(self.handle_time_group_by(*time_grouping)?);
            }
            AggregationNode::HistogramGroupBy(_) => {
                return Err(unsupported_query_error("histogram group by"));
            }
            AggregationNode::FlatFieldsGroupBy(_) => {
                return Err(unsupported_query_error("flat fields group by"));
            }
            AggregationNode::Computes(computes) => {
                for agg in computes.aggregation {
                    // a root compute node doesn't actually correspond to having a parent
                    // aggregation in tantivy's definition of aggregation, so we
                    // propagate the flag rather than setting it to false
                    tantivy_aggregations_per_key.extend(self.handle_generic_aggregation(agg)?);
                }
                for time_agg in computes.time_grouping {
                    tantivy_aggregations_per_key.push(self.handle_time_group_by(time_agg)?);
                }
            }
            AggregationNode::ListCompute(_) => return Err(unsupported_query_error("list compute")),
            AggregationNode::AnyCompute(_) => return Err(unsupported_query_error("any compute")),
            AggregationNode::MetricCompute(metric_compute) => {
                tantivy_aggregations_per_key.extend(self.handle_metric_compute(metric_compute)?);
            }
        }

        let mut tantivy_aggregations = TantivyAggregations::default();

        for (agg_key, aggregation) in tantivy_aggregations_per_key {
            match tantivy_aggregations.entry(agg_key) {
                Entry::Occupied(occupied_entry) => {
                    let redundant_agg_key = occupied_entry.key();
                    return Err(InvalidQuery::Other(anyhow::anyhow!(
                        "multiple aggs are named {redundant_agg_key}",
                    )));
                }
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(aggregation);
                }
            }
        }

        Ok(tantivy_aggregations)
    }

    fn handle_attribute_group_by(
        &self,
        attribute_group_by: quickwit_proto::cloudprem::AttributeGroupBy,
    ) -> Result<Vec<(String, TantivyAggregation)>, InvalidQuery> {
        let field_name = extract_field_name(attribute_group_by.expression.as_ref())?;
        // TODO we should check if we can get a type from java or not
        let missing = attribute_group_by
            .missing
            .map(tantivy::aggregation::Key::Str);
        let include = attribute_group_by
            .include
            .filter(|s| !s.is_empty())
            .map(|include| {
                // Replace `*` with `.*` to allow wildcard matching.
                let regex = include.replace('*', ".*");
                if regex.contains("*") {
                    // If the original include had a `*`, we don't surround with `.*`
                    IncludeExcludeParam::Regex(regex)
                } else {
                    IncludeExcludeParam::Regex(format!(".*{}.*", include))
                }
            });

        let Some(child) = attribute_group_by.child else {
            return Err(missing_required("attribute_fields.child"));
        };

        let mut sub_aggregation = self.handle_generic_aggregation(*child)?;
        let mut named_aggregations = Vec::new();

        // We requested a total field.
        //
        // In this case, want to add sub_aggregations as a sibling of our
        // term aggregation.
        if attribute_group_by.total.is_some() {
            for (sub_agg_name, sub_agg) in &sub_aggregation {
                let total_sub_agg_name =
                    total_subaggregation_sibling_names(&field_name, sub_agg_name);
                named_aggregations.push((total_sub_agg_name, sub_agg.clone()));
            }
        }

        let group_by_order: Option<CustomOrder> =
            build_group_by_sort_order(&attribute_group_by.sort, &mut sub_aggregation)?;
        let terms_agg = bucket::TermsAggregation {
            field: field_name.clone(),
            size: Some(attribute_group_by.limit),
            segment_size: None,
            show_term_doc_count_error: None,
            min_doc_count: None,
            order: group_by_order,
            missing,
            include,
            exclude: None,
        };
        let tantivy_agg = TantivyAggregation {
            // TODO can we get a into() from *Aggregation to AggregationVariants instead?
            agg: AggregationVariants::Terms(terms_agg),
            sub_aggregation,
        };

        named_aggregations.push((field_name, tantivy_agg));

        // TODO i'm still searching if there is a proper "output" field somewhere we should be
        // using, there really ought to be
        Ok(named_aggregations)
    }

    fn handle_time_group_by(
        &self,
        time_grouping: quickwit_proto::cloudprem::TimeGrouping,
    ) -> Result<(String, TantivyAggregation), InvalidQuery> {
        let (interval, offset): (String, Option<String>) =
            if let Some(interval_ns) = time_grouping.interval_ns {
                let interval_ms = interval_ns / 1_000_000;
                (format!("{interval_ms}ms"), None)
            } else if let Some(rollup) = time_grouping.rollup {
                rollup_to_interval(&rollup, self.start_ts_secs)?
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
            sub_aggregation: self.handle_generic_aggregation(*child)?,
        };

        Ok((time_grouping.output, tantivy_agg))
    }

    fn handle_metric_compute(
        &self,
        metric_compute: quickwit_proto::cloudprem::MetricCompute,
    ) -> Result<Option<(String, TantivyAggregation)>, InvalidQuery> {
        let metric_agg_opt =
            build_metric(&metric_compute.expression, metric_compute.r#type.as_str())?;
        Ok(metric_agg_opt.map(|metric_agg| (metric_compute.id, metric_agg)))
    }
}

fn build_metric(
    expression: &Option<ExpressionNode>,
    aggregation_type: &str,
) -> Result<Option<TantivyAggregation>, InvalidQuery> {
    let field = extract_field_name(expression.as_ref())?;
    // TODO support more aggregations?
    let agg = match aggregation_type {
        "COUNT" => {
            // count aggregation are either handled by the parent aggregation, or in the case of
            // a "root count", by the usual matching-doc counting mechanism.
            // either way, we don't want counts to appear in tantivy aggregation
            // ast
            return Ok(None);
        }
        "CARDINALITY_SKETCH" | "CARDINALITY" => {
            let cardinality = metric::CardinalityAggregationReq {
                field,
                missing: None,
            };
            // TODO can we get a into() from *Aggregation to AggregationVariants instead?
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
        // QUANTILE_SKETCH: request a percentile aggregation backed by a DDSketch.
        // The specific percentile value (50.0) is a placeholder — the DDSketch
        // collects the full distribution regardless. Event-query extracts the
        // actual percentile it needs from the returned sketch.
        "QUANTILE_SKETCH" => {
            let percentiles = metric::PercentilesAggregationReq {
                field,
                percents: Some(vec![50.0]),
                keyed: false,
                missing: None,
            };
            AggregationVariants::Percentiles(percentiles)
        }
        other
            if let Some(percentile) = other
                .strip_prefix("PC")
                .and_then(|percentage| percentage.parse().ok()) =>
        {
            let percentiles = metric::PercentilesAggregationReq {
                field,
                percents: Some(vec![percentile]),
                keyed: false,
                missing: None,
            };
            AggregationVariants::Percentiles(percentiles)
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
        sub_aggregation: Default::default(),
    };

    Ok(Some(tantivy_agg))
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

/// Event query has a feature that is not available in elasticsearch.
/// It offers the possibility to, in group by, accumulate all documents being
/// seen in a "virtual bucket".
///
/// To emulate it, we simply create "sibling aggregation" in tantivy.
/// There are no aggregation in ES allowing to group these siblings together
/// under a given name. A possible trick is to use a FilterQuery with a match all
/// filter... But here we simply rely on a strange prefix to create this namespace.
fn total_subaggregation_sibling_names(agg_name: &str, sub_aggregation: &str) -> String {
    format!("{}_TOTAL::{}", agg_name, sub_aggregation)
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

// --- Finalized aggregation result handling (original path) ---

pub fn aggregation_result_to_proto(
    aggregation_results: QuickwitAggregationResults,
    aggregations_def: &quickwit_proto::cloudprem::Aggregation,
    parent_count: u64,
) -> Result<Vec<EvpAggregationResult>, CloudPremError> {
    let mut mapper = ResultMapper {
        results: Vec::new(),
    };
    mapper.consume_agg(aggregation_results.into(), aggregations_def, parent_count)?;
    Ok(mapper.results)
}

struct ResultMapper {
    results: Vec<EvpAggregationResult>,
}

impl ResultMapper {
    fn consume_agg(
        &mut self,
        mut agg_result: TantivyAggregationResults,
        aggregations_def: &quickwit_proto::cloudprem::Aggregation,
        parent_count: u64,
    ) -> Result<(), CloudPremError> {
        let mut state = EvpAggregationResult::default();
        self.consume_agg_aux(
            &mut agg_result,
            &mut state,
            aggregations_def
                .aggregation
                .as_ref()
                .ok_or_else(|| missing_required("aggregation"))?,
            parent_count,
        )?;
        // handle the case of a root metric aggregation
        if !state.value.is_empty() {
            self.results.push(state);
        }
        Ok(())
    }

    fn consume_agg_aux(
        &mut self,
        agg_result: &mut TantivyAggregationResults,
        state: &mut EvpAggregationResult,
        aggregations_def: &quickwit_proto::cloudprem::aggregation::Aggregation,
        parent_count: u64,
    ) -> Result<(), CloudPremError> {
        match aggregations_def {
            AggregationNode::AttributeGroupBy(attribute_group_by) => {
                self.handle_attribute_group_by(agg_result, state, attribute_group_by)?;
            }
            AggregationNode::TimeGroupBy(time_grouping) => {
                self.handle_time_group_by(agg_result, state, time_grouping)?;
            }
            AggregationNode::HistogramGroupBy(_) => {
                return Err(unsupported_query_error("histogram group by").into());
            }
            AggregationNode::FlatFieldsGroupBy(_) => {
                return Err(unsupported_query_error("flat fields group by").into());
            }
            AggregationNode::Computes(computes) => {
                for agg in &computes.aggregation {
                    let agg = agg
                        .aggregation
                        .as_ref()
                        .ok_or_else(|| missing_required("attribute_fields.child.aggregation"))?;
                    self.consume_agg_aux(agg_result, state, agg, parent_count)?;
                }
                for time_grouping in &computes.time_grouping {
                    self.handle_time_group_by(agg_result, state, time_grouping)?;
                }
            }
            AggregationNode::ListCompute(_) => {
                return Err(unsupported_query_error("list compute").into());
            }
            AggregationNode::AnyCompute(_) => {
                return Err(unsupported_query_error("any compute").into());
            }
            AggregationNode::MetricCompute(metric_compute) => {
                self.handle_metric_compute(agg_result, state, metric_compute, parent_count)?;
            }
        }
        Ok(())
    }

    fn handle_attribute_group_by(
        &mut self,
        agg_result: &mut TantivyAggregationResults,
        state: &mut EvpAggregationResult,
        attribute_group_by: &quickwit_proto::cloudprem::AttributeGroupBy,
    ) -> Result<(), CloudPremError> {
        use tantivy::aggregation::agg_result::BucketResult;

        let key = extract_field_name(attribute_group_by.expression.as_ref())?;
        let agg = agg_result
            .0
            .remove(&key)
            .ok_or_else(|| internal_error("result content missmatch"))?;
        match agg {
            TantivyAggregationResult::BucketResult(BucketResult::Terms {
                buckets,
                sum_other_doc_count,
                ..
            }) => {
                let child_agg_def_opt = attribute_group_by
                    .child
                    .as_ref()
                    .ok_or_else(|| missing_required("attribute_fields.child"))?
                    .aggregation
                    .as_ref()
                    .ok_or_else(|| missing_required("attribute_fields.child.aggregation"))?;

                let state_key_len = state.key.len();
                debug_assert!(state.value.is_empty());

                let mut total_in_buckets = 0u64;
                for mut bucket in buckets {
                    total_in_buckets += bucket.doc_count;
                    state.key.push(bucket.key.to_string());
                    self.consume_agg_aux(
                        &mut bucket.sub_aggregation,
                        state,
                        child_agg_def_opt,
                        bucket.doc_count,
                    )?;
                    if !state.value.is_empty() {
                        self.results.push(state.clone());
                        state.value.clear();
                    }
                    state.key.truncate(state_key_len);
                }
                if let Some(total_field) = attribute_group_by.total.as_ref() {
                    let total_count: u64 = total_in_buckets + sum_other_doc_count;
                    let mut total_agg_results = extract_total_siblings_results(agg_result, &key);
                    state.key.push(total_field.to_string());
                    self.consume_agg_aux(
                        &mut total_agg_results,
                        state,
                        child_agg_def_opt,
                        total_count,
                    )?;
                    if !state.value.is_empty() {
                        self.results.push(state.clone());
                        state.value.clear();
                    }
                    state.key.truncate(state_key_len);
                }
            }
            _ => return Err(internal_error("result content missmatch").into()),
        }
        Ok(())
    }

    fn handle_time_group_by(
        &mut self,
        agg_result: &mut TantivyAggregationResults,
        state: &mut EvpAggregationResult,
        time_grouping: &quickwit_proto::cloudprem::TimeGrouping,
    ) -> Result<(), CloudPremError> {
        use tantivy::aggregation::agg_result::BucketResult;

        let agg = agg_result
            .0
            .remove(&time_grouping.output)
            .ok_or_else(|| internal_error("result content missmatch"))?;

        match agg {
            TantivyAggregationResult::BucketResult(BucketResult::Histogram { buckets }) => {
                let child_agg_def_opt = time_grouping
                    .child
                    .as_ref()
                    .ok_or_else(|| missing_required("attribute_fields.child"))?
                    .aggregation
                    .as_ref()
                    .ok_or_else(|| missing_required("attribute_fields.child.aggregation"))?;

                let state_key_len = state.key.len();
                debug_assert!(state.value.is_empty());

                for mut bucket in bucket_iter(buckets) {
                    state.key.push(
                        bucket
                            .key_as_string
                            .unwrap_or_else(|| bucket.key.to_string()),
                    );
                    self.consume_agg_aux(
                        &mut bucket.sub_aggregation,
                        state,
                        child_agg_def_opt,
                        bucket.doc_count,
                    )?;
                    if !state.value.is_empty() {
                        self.results.push(state.clone());
                        state.value.clear();
                    }
                    state.key.truncate(state_key_len);
                }
            }
            _ => return Err(internal_error("result content missmatch").into()),
        }
        Ok(())
    }

    fn handle_metric_compute(
        &mut self,
        agg_result: &mut TantivyAggregationResults,
        state: &mut EvpAggregationResult,
        metric_compute: &quickwit_proto::cloudprem::MetricCompute,
        parent_count: u64,
    ) -> Result<(), CloudPremError> {
        use tantivy::aggregation::agg_result::MetricResult;

        if metric_compute.r#type.as_str() == "COUNT" {
            state.value.push(u64_to_agg_value(parent_count));
            return Ok(());
        }

        let agg = agg_result
            .0
            .remove(&metric_compute.id)
            .ok_or_else(|| internal_error("result content missmatch"))?;

        let TantivyAggregationResult::MetricResult(metric_result) = agg else {
            return Err(internal_error("result content missmatch").into());
        };

        match (metric_compute.r#type.as_str(), metric_result) {
            ("CARDINALITY_SKETCH" | "CARDINALITY", MetricResult::Cardinality(cardinality)) => {
                // Finalized path: the sketch data is lost after finalization, only the scalar
                // estimate remains. Return as uint64 — this is only used in tests; the production
                // cloudprem service always uses the intermediate path which preserves the sketch.
                state.value.push(u64_to_agg_value(
                    cardinality.value.unwrap_or_default() as u64
                ));
            }
            ("SUM", MetricResult::Sum(metric_res))
            | ("MIN", MetricResult::Min(metric_res))
            | ("MAX", MetricResult::Max(metric_res)) => {
                state
                    .value
                    .push(f64_to_agg_value(metric_res.value.unwrap_or_default()));
            }
            ("AVG", MetricResult::Average(avg)) => {
                state
                    .value
                    .push(generate_avg(avg.value.unwrap_or_default()));
            }
            ("QUANTILE_SKETCH", MetricResult::Percentiles(_)) => {
                // Finalized percentile results only contain evaluated float values —
                // the DDSketch is consumed during finalization. Event-query requires
                // the raw DDSketch for merging, so this path must not be used.
                // Use skip_aggregation_finalization=true to get DDSketch via
                // intermediate_aggregation_result_to_proto instead.
                return Err(InvalidQuery::Other(anyhow::anyhow!(
                    "QUANTILE_SKETCH requires skip_aggregation_finalization=true; finalized \
                     results do not contain the DDSketch needed by event-query"
                ))
                .into());
            }
            (agg_name, _) => {
                return Err(InvalidQuery::Other(anyhow::anyhow!(
                    "aggregation type mismatch for {agg_name}"
                ))
                .into());
            }
        }

        Ok(())
    }
}

fn extract_total_siblings_results(
    aggregations: &mut TantivyAggregationResults,
    agg_name: &str,
) -> TantivyAggregationResults {
    let mut results = TantivyAggregationResults(Default::default());
    let total_sibling_prefix = total_subaggregation_sibling_names(agg_name, "");
    let total_sibling_keys: Vec<String> = aggregations
        .0
        .keys()
        .filter(|key| key.starts_with(&total_sibling_prefix))
        .cloned()
        .collect();
    for total_sibling_key in total_sibling_keys {
        let Some(sub_agg_results) = aggregations.0.remove(&total_sibling_key) else {
            continue;
        };
        let Some(sub_agg_key) = total_sibling_key.strip_prefix(&total_sibling_prefix) else {
            continue;
        };
        results.0.insert(sub_agg_key.to_string(), sub_agg_results);
    }
    results
}

// --- Intermediate aggregation result handling (standalone, for skip_aggregation_finalization=true)
// ---

/// Convert intermediate aggregation results to CloudPrem proto format.
/// Returns raw sum/count for AVG (for proper weighted-average merging across query steps).
pub fn intermediate_aggregation_result_to_proto(
    intermediate_results: IntermediateAggregationResults,
    aggregations_def: &quickwit_proto::cloudprem::Aggregation,
    parent_count: u64,
) -> Result<Vec<EvpAggregationResult>, CloudPremError> {
    let mut mapper = IntermediateResultMapper {
        results: Vec::new(),
    };
    mapper.consume_agg(intermediate_results, aggregations_def, parent_count)?;
    Ok(mapper.results)
}

struct IntermediateResultMapper {
    results: Vec<EvpAggregationResult>,
}

impl IntermediateResultMapper {
    fn consume_agg(
        &mut self,
        mut agg_result: IntermediateAggregationResults,
        aggregations_def: &quickwit_proto::cloudprem::Aggregation,
        parent_count: u64,
    ) -> Result<(), CloudPremError> {
        let mut state = EvpAggregationResult::default();
        self.consume_agg_aux(
            &mut agg_result,
            &mut state,
            aggregations_def
                .aggregation
                .as_ref()
                .ok_or_else(|| missing_required("aggregation"))?,
            parent_count,
        )?;
        if !state.value.is_empty() {
            self.results.push(state);
        }
        Ok(())
    }

    fn consume_agg_aux(
        &mut self,
        agg_result: &mut IntermediateAggregationResults,
        state: &mut EvpAggregationResult,
        aggregations_def: &quickwit_proto::cloudprem::aggregation::Aggregation,
        parent_count: u64,
    ) -> Result<(), CloudPremError> {
        match aggregations_def {
            AggregationNode::AttributeGroupBy(attribute_group_by) => {
                self.handle_attribute_group_by(agg_result, state, attribute_group_by)?;
            }
            AggregationNode::TimeGroupBy(time_grouping) => {
                self.handle_time_group_by(agg_result, state, time_grouping)?;
            }
            AggregationNode::HistogramGroupBy(_) => {
                return Err(unsupported_query_error("histogram group by").into());
            }
            AggregationNode::FlatFieldsGroupBy(_) => {
                return Err(unsupported_query_error("flat fields group by").into());
            }
            AggregationNode::Computes(computes) => {
                for agg in &computes.aggregation {
                    let agg = agg
                        .aggregation
                        .as_ref()
                        .ok_or_else(|| missing_required("attribute_fields.child.aggregation"))?;
                    self.consume_agg_aux(agg_result, state, agg, parent_count)?;
                }
                for time_grouping in &computes.time_grouping {
                    self.handle_time_group_by(agg_result, state, time_grouping)?;
                }
            }
            AggregationNode::ListCompute(_) => {
                return Err(unsupported_query_error("list compute").into());
            }
            AggregationNode::AnyCompute(_) => {
                return Err(unsupported_query_error("any compute").into());
            }
            AggregationNode::MetricCompute(metric_compute) => {
                self.handle_metric_compute(agg_result, state, metric_compute, parent_count)?;
            }
        }
        Ok(())
    }

    fn handle_attribute_group_by(
        &mut self,
        agg_result: &mut IntermediateAggregationResults,
        state: &mut EvpAggregationResult,
        attribute_group_by: &quickwit_proto::cloudprem::AttributeGroupBy,
    ) -> Result<(), CloudPremError> {
        use tantivy::aggregation::intermediate_agg_result::{
            IntermediateAggregationResult as TantivyIntermediateAggResult, IntermediateBucketResult,
        };

        let key = extract_field_name(attribute_group_by.expression.as_ref())?;
        // When no documents match, the key may be absent — return empty results.
        let Some(agg) = agg_result.remove(&key) else {
            return Ok(());
        };
        match agg {
            TantivyIntermediateAggResult::Bucket(IntermediateBucketResult::Terms { buckets }) => {
                let child_agg_def_opt = attribute_group_by
                    .child
                    .as_ref()
                    .ok_or_else(|| missing_required("attribute_fields.child"))?
                    .aggregation
                    .as_ref()
                    .ok_or_else(|| missing_required("attribute_fields.child.aggregation"))?;

                let state_key_len = state.key.len();
                debug_assert!(state.value.is_empty());

                let sum_other_doc_count = buckets.sum_other_doc_count();
                let mut total_in_buckets = 0u64;
                for (bucket_key, entry) in buckets.entries().iter() {
                    let doc_count = entry.doc_count as u64;
                    total_in_buckets += doc_count;
                    state.key.push(bucket_key.to_string());
                    let mut sub_agg = entry.sub_aggregation.clone();
                    self.consume_agg_aux(&mut sub_agg, state, child_agg_def_opt, doc_count)?;
                    if !state.value.is_empty() {
                        self.results.push(state.clone());
                        state.value.clear();
                    }
                    state.key.truncate(state_key_len);
                }
                if let Some(total_field) = attribute_group_by.total.as_ref() {
                    let total_count: u64 = total_in_buckets + sum_other_doc_count;
                    let mut total_agg = extract_intermediate_total_siblings(agg_result, &key);
                    state.key.push(total_field.to_string());
                    self.consume_agg_aux(&mut total_agg, state, child_agg_def_opt, total_count)?;
                    if !state.value.is_empty() {
                        self.results.push(state.clone());
                        state.value.clear();
                    }
                    state.key.truncate(state_key_len);
                }
            }
            _ => return Err(internal_error("result content missmatch").into()),
        }
        Ok(())
    }

    fn handle_time_group_by(
        &mut self,
        agg_result: &mut IntermediateAggregationResults,
        state: &mut EvpAggregationResult,
        time_grouping: &quickwit_proto::cloudprem::TimeGrouping,
    ) -> Result<(), CloudPremError> {
        use tantivy::aggregation::intermediate_agg_result::{
            IntermediateAggregationResult as TantivyIntermediateAggResult, IntermediateBucketResult,
        };

        // When no documents match, the key may be absent — return empty results.
        let Some(agg) = agg_result.remove(&time_grouping.output) else {
            return Ok(());
        };

        match agg {
            TantivyIntermediateAggResult::Bucket(IntermediateBucketResult::Histogram {
                buckets,
                is_date_agg,
            }) => {
                let child_agg_def_opt = time_grouping
                    .child
                    .as_ref()
                    .ok_or_else(|| missing_required("attribute_fields.child"))?
                    .aggregation
                    .as_ref()
                    .ok_or_else(|| missing_required("attribute_fields.child.aggregation"))?;

                let state_key_len = state.key.len();
                debug_assert!(state.value.is_empty());

                for mut bucket in buckets {
                    let key_as_string = if is_date_agg {
                        use time::format_description::well_known::Rfc3339;
                        time::OffsetDateTime::from_unix_timestamp_nanos(bucket.key as i128)
                            .ok()
                            .and_then(|dt| dt.format(&Rfc3339).ok())
                            .unwrap_or_else(|| bucket.key.to_string())
                    } else {
                        bucket.key.to_string()
                    };
                    state.key.push(key_as_string);
                    self.consume_agg_aux(
                        &mut bucket.sub_aggregation,
                        state,
                        child_agg_def_opt,
                        bucket.doc_count,
                    )?;
                    if !state.value.is_empty() {
                        self.results.push(state.clone());
                        state.value.clear();
                    }
                    state.key.truncate(state_key_len);
                }
            }
            _ => return Err(internal_error("result content missmatch").into()),
        }
        Ok(())
    }

    fn handle_metric_compute(
        &mut self,
        agg_result: &mut IntermediateAggregationResults,
        state: &mut EvpAggregationResult,
        metric_compute: &quickwit_proto::cloudprem::MetricCompute,
        parent_count: u64,
    ) -> Result<(), CloudPremError> {
        use tantivy::aggregation::intermediate_agg_result::{
            IntermediateAggregationResult as TantivyIntermediateAggResult, IntermediateMetricResult,
        };

        if metric_compute.r#type.as_str() == "COUNT" {
            state.value.push(u64_to_agg_value(parent_count));
            return Ok(());
        }

        // When no documents match, the intermediate result map may not contain
        // the metric key at all. Return sensible defaults instead of erroring.
        let Some(agg) = agg_result.remove(&metric_compute.id) else {
            state
                .value
                .push(default_agg_value(metric_compute.r#type.as_str()));
            return Ok(());
        };

        let TantivyIntermediateAggResult::Metric(metric_result) = agg else {
            return Err(internal_error("result content missmatch").into());
        };

        match (metric_compute.r#type.as_str(), metric_result) {
            ("AVG", IntermediateMetricResult::Average(avg)) => {
                let stats = avg.stats();
                state.value.push(EvpAggValue {
                    value: Some(quickwit_proto::cloudprem::agg_value::Value::AvgValue(
                        quickwit_proto::cloudprem::Avg {
                            sum: stats.sum(),
                            count: stats.count(),
                        },
                    )),
                });
            }
            (
                "CARDINALITY_SKETCH" | "CARDINALITY",
                IntermediateMetricResult::Cardinality(cardinality),
            ) => {
                let sketch_bytes = cardinality.to_sketch_bytes();
                state.value.push(EvpAggValue {
                    value: Some(
                        quickwit_proto::cloudprem::agg_value::Value::HllDataSketchValue(
                            sketch_bytes,
                        ),
                    ),
                });
            }
            ("SUM", IntermediateMetricResult::Sum(m)) => {
                state
                    .value
                    .push(u64_to_agg_value(m.finalize().unwrap_or_default() as u64));
            }
            ("MIN", IntermediateMetricResult::Min(m)) => {
                state
                    .value
                    .push(u64_to_agg_value(m.finalize().unwrap_or_default() as u64));
            }
            ("MAX", IntermediateMetricResult::Max(m)) => {
                state
                    .value
                    .push(u64_to_agg_value(m.finalize().unwrap_or_default() as u64));
            }
            // QUANTILE_SKETCH: encode DDSketch into Java-compatible binary format
            // for proper merging in event-query via Sketch.fromByteArray().
            ("QUANTILE_SKETCH", IntermediateMetricResult::Percentiles(percentiles_collector)) => {
                let sketch_bytes = percentiles_collector.to_sketch_bytes();
                state.value.push(EvpAggValue {
                    value: Some(quickwit_proto::cloudprem::agg_value::Value::SketchValue(
                        sketch_bytes,
                    )),
                });
            }
            (agg_name, IntermediateMetricResult::Percentiles(percentiles_res))
                if let Some(percentile) = agg_name
                    .strip_prefix("PC")
                    .and_then(|percentage| percentage.parse().ok()) =>
            {
                let percentiles_req = metric::PercentilesAggregationReq {
                    field: String::new(),
                    percents: Some(vec![percentile]),
                    keyed: false,
                    missing: None,
                };
                let metric::PercentileValues::Vec(results) =
                    percentiles_res.into_final_result(&percentiles_req).values
                else {
                    return Err(internal_error("percentile incorrectly keyed").into());
                };
                state.value.push(f64_to_agg_value(
                    results
                        .first()
                        .ok_or_else(|| internal_error("percentile empty"))?
                        .value,
                ));
            }
            (agg_name, _) => {
                return Err(InvalidQuery::Other(anyhow::anyhow!(
                    "aggregation type mismatch for {agg_name}"
                ))
                .into());
            }
        }

        Ok(())
    }
}

fn extract_intermediate_total_siblings(
    aggregations: &mut IntermediateAggregationResults,
    agg_name: &str,
) -> IntermediateAggregationResults {
    let mut results = IntermediateAggregationResults::default();
    let total_sibling_prefix = total_subaggregation_sibling_names(agg_name, "");
    let total_sibling_keys: Vec<String> = aggregations
        .keys()
        .filter(|key| key.starts_with(&total_sibling_prefix))
        .cloned()
        .collect();
    for total_sibling_key in total_sibling_keys {
        let Some(sub_agg_results) = aggregations.remove(&total_sibling_key) else {
            continue;
        };
        let Some(sub_agg_key) = total_sibling_key.strip_prefix(&total_sibling_prefix) else {
            continue;
        };
        let _ = results.push(sub_agg_key.to_string(), sub_agg_results);
    }
    results
}

/// Return a sensible default `AggValue` for the given aggregation type when no
/// documents match (i.e. the intermediate result map has no entry for the key).
fn default_agg_value(agg_type: &str) -> EvpAggValue {
    match agg_type {
        "AVG" => EvpAggValue {
            value: Some(quickwit_proto::cloudprem::agg_value::Value::AvgValue(
                quickwit_proto::cloudprem::Avg { sum: 0.0, count: 0 },
            )),
        },
        "CARDINALITY_SKETCH" | "CARDINALITY" => {
            // Return an empty DataSketches HLL sketch (lg_k=11, Hll4)
            let empty = tantivy::aggregation::metric::CardinalityCollector::default();
            EvpAggValue {
                value: Some(
                    quickwit_proto::cloudprem::agg_value::Value::HllDataSketchValue(
                        empty.to_sketch_bytes(),
                    ),
                ),
            }
        }
        "QUANTILE_SKETCH" => {
            let empty = tantivy::aggregation::metric::PercentilesCollector::default();
            EvpAggValue {
                value: Some(quickwit_proto::cloudprem::agg_value::Value::SketchValue(
                    empty.to_sketch_bytes(),
                )),
            }
        }
        // SUM, MIN, MAX, and anything else → 0
        _ => u64_to_agg_value(0),
    }
}

fn u64_to_agg_value(val: u64) -> EvpAggValue {
    EvpAggValue {
        value: Some(quickwit_proto::cloudprem::agg_value::Value::Uint64Value(
            val,
        )),
    }
}

fn f64_to_agg_value(val: f64) -> EvpAggValue {
    EvpAggValue {
        value: Some(quickwit_proto::cloudprem::agg_value::Value::Float64Value(
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

    use prost_types::Any;
    use quickwit_proto::cloudprem::agg_value::Value;
    use quickwit_proto::cloudprem::calc_node::FieldRef;
    use quickwit_proto::cloudprem::{AggValue, CalcNode, ExpressionNode, MetricCompute};
    use tantivy::Searcher;

    pub fn test_searcher() -> Searcher {
        use tantivy::schema::*;
        let mut schema_builder = Schema::builder();
        let host_field = schema_builder.add_text_field("host", FAST);
        let value_field = schema_builder.add_u64_field("value", FAST);
        let schema = schema_builder.build();
        let index = tantivy::IndexBuilder::new()
            .schema(schema)
            .create_in_ram()
            .unwrap();

        let mut index_writer = index.writer_with_num_threads(1, 20_000_000).unwrap();
        for count in 1..13 {
            let mut doc = tantivy::TantivyDocument::default();
            doc.add_text(host_field, format!("host_{count}"));
            doc.add_u64(value_field, count);
            for _ in 0..count {
                index_writer.add_document(doc.clone()).unwrap();
            }
        }
        index_writer.commit().unwrap();
        index.reader().unwrap().searcher()
    }

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

    pub fn field_expr(field_name: &str) -> ExpressionNode {
        let calc_node = CalcNode {
            calc_node: Some(quickwit_proto::cloudprem::calc_node::CalcNode::FieldRef(
                FieldRef {
                    field_name: field_name.to_string(),
                },
            )),
        };
        let any_calc_node = Any::from_msg(&calc_node).unwrap();
        ExpressionNode {
            calc_node: Some(any_calc_node),
        }
    }

    pub fn count_expr() -> ExpressionNode {
        field_expr("count")
    }

    pub fn host_expr() -> ExpressionNode {
        field_expr("host")
    }

    pub fn status_expr() -> ExpressionNode {
        field_expr("status")
    }

    pub fn count_metric() -> quickwit_proto::cloudprem::aggregation::Aggregation {
        count_metric_with_id("count:count")
    }

    pub fn count_metric_with_id(id: &str) -> quickwit_proto::cloudprem::aggregation::Aggregation {
        quickwit_proto::cloudprem::aggregation::Aggregation::MetricCompute(MetricCompute {
            expression: Some(count_expr()),
            id: id.to_string(),
            r#type: "COUNT".to_string(),
        })
    }

    pub fn distinct_source_metric() -> quickwit_proto::cloudprem::aggregation::Aggregation {
        quickwit_proto::cloudprem::aggregation::Aggregation::MetricCompute(MetricCompute {
            expression: Some(field_expr("tag.source")),
            id: "tag.source:cardinality".to_string(),
            r#type: "CARDINALITY_SKETCH".to_string(),
        })
    }

    pub fn avg_value_metric() -> quickwit_proto::cloudprem::aggregation::Aggregation {
        quickwit_proto::cloudprem::aggregation::Aggregation::MetricCompute(MetricCompute {
            expression: Some(field_expr("value")),
            id: "avg:avg".to_string(),
            r#type: "AVG".to_string(),
        })
    }

    pub fn percentile_value_metric() -> quickwit_proto::cloudprem::aggregation::Aggregation {
        quickwit_proto::cloudprem::aggregation::Aggregation::MetricCompute(MetricCompute {
            expression: Some(field_expr("value")),
            id: "percentile:value".to_string(),
            r#type: "QUANTILE_SKETCH".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use prost_types::Any;
    use quickwit_proto::cloudprem::aggregation::Aggregation as AggregationEnum;
    use quickwit_proto::cloudprem::sort_by_expr_and_agg::SortType;
    use quickwit_proto::cloudprem::*;
    use tantivy::aggregation::AggregationCollector;
    use tantivy::aggregation::agg_req::{Aggregation as TantivyAgg, AggregationVariants};
    use tantivy::aggregation::bucket::*;
    use tantivy::query::AllQuery;

    use super::test_helpers::*;
    use super::{aggregation_result_to_proto, rollup_to_interval, to_tantivy_aggregation};
    use crate::aggregations::{
        AggregationResult as QuickwitAggregationResult,
        AggregationResults as QuickwitAggregationResults, BucketEntries, BucketEntry, BucketResult,
        Key, MetricResult,
    };
    use crate::cloudprem::sanitize_metric_id_aggregations;

    #[test]
    fn test_count_request() {
        let evp_agg = Aggregation {
            aggregation: Some(AggregationEnum::Computes(Computes {
                aggregation: vec![Aggregation {
                    aggregation: Some(count_metric()),
                }],
                time_grouping: vec![],
            })),
        };

        let expected = [].into_iter().collect();

        let res = to_tantivy_aggregation(evp_agg, 0).unwrap();

        assert_eq!(res, expected);
    }

    #[test]
    fn test_count_by_facet_request() {
        let evp_agg = Aggregation {
            aggregation: Some(AggregationEnum::AttributeGroupBy(Box::new(
                AttributeGroupBy {
                    include: None,
                    expression: Some(status_expr()),
                    limit: 50,
                    sort: Some(SortByExprAndAgg {
                        ascending: false,
                        expr_and_agg: Some(ExprAndAgg {
                            expr: Some(count_expr()),
                            agg_function: "count".to_string(),
                        }),
                        r#type: SortType::Metric as i32,
                    }),
                    missing: None,
                    total: None,
                    child: Some(Box::new(Aggregation {
                        aggregation: Some(AggregationEnum::Computes(Computes {
                            aggregation: vec![Aggregation {
                                aggregation: Some(count_metric()),
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
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            },
        )]
        .into_iter()
        .collect();

        let res = to_tantivy_aggregation(evp_agg, 0).unwrap();

        assert_eq!(res, expected);
    }

    #[test]
    fn test_count_by_facet_with_include() {
        let make_agg = |include: Option<&str>| {
            let evp_agg = Aggregation {
                aggregation: Some(AggregationEnum::AttributeGroupBy(Box::new(
                    AttributeGroupBy {
                        include: include.map(str::to_string),
                        expression: Some(status_expr()),
                        limit: 50,
                        sort: None,
                        missing: None,
                        total: None,
                        child: Some(Box::new(Aggregation {
                            aggregation: Some(AggregationEnum::Computes(Computes {
                                aggregation: vec![Aggregation {
                                    aggregation: Some(count_metric()),
                                }],
                                time_grouping: vec![],
                            })),
                        })),
                    },
                ))),
            };
            to_tantivy_aggregation(evp_agg, 0).unwrap()
        };

        // A non-empty include value should produce a regex filter.
        let res = make_agg(Some("error"));
        let terms = match &res["status"].agg {
            AggregationVariants::Terms(terms) => terms,
            other => panic!("expected Terms, got {other:?}"),
        };
        assert_eq!(
            terms.include,
            Some(IncludeExcludeParam::Regex(".*error.*".to_string()))
        );

        // An empty include string must be treated the same as None — no regex built.
        let res_empty = make_agg(Some(""));
        let terms_empty = match &res_empty["status"].agg {
            AggregationVariants::Terms(terms) => terms,
            other => panic!("expected Terms, got {other:?}"),
        };
        assert_eq!(terms_empty.include, None);
    }

    #[test]
    fn test_timeline_aggregation_request() {
        let evp_agg = Aggregation {
            aggregation: Some(AggregationEnum::AttributeGroupBy(Box::new(
                AttributeGroupBy {
                    include: None,
                    expression: Some(status_expr()),
                    limit: 10,
                    sort: Some(SortByExprAndAgg {
                        ascending: false,
                        expr_and_agg: Some(ExprAndAgg {
                            expr: Some(count_expr()),
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
                                            aggregation: Some(count_metric_with_id(
                                                "count:count:timeseries:28800000",
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
                    ..Default::default()
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
                        sub_aggregation: Default::default(),
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

    #[allow(dead_code)]
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
        let aggregation_results = QuickwitAggregationResults(Vec::new());
        let expected = vec![AggregationResult {
            key: vec![],
            value: vec![2u64.to_value()],
        }];

        let agg_def_inner = count_metric();
        let agg_def = quickwit_proto::cloudprem::Aggregation {
            aggregation: Some(agg_def_inner),
        };
        let res = aggregation_result_to_proto(aggregation_results, &agg_def, 2).unwrap();
        assert_eq!(res, expected);
    }

    #[test]
    fn test_map_reply_admin_test() {
        let aggregation_results = terms(
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
                        sub_aggregation: QuickwitAggregationResults(Vec::new()),
                    }],
                ),
            }],
        );
        let count_agg = count_metric();

        let time_grouping = quickwit_proto::cloudprem::TimeGrouping {
            output: "time:28800000".to_string(),
            path: "".to_string(),
            time_zone: "".to_string(),
            interval_ns: None,
            rollup: None,
            child: Some(Box::new(quickwit_proto::cloudprem::Aggregation {
                aggregation: Some(count_agg),
            })),
        };
        let time_group_by_inner = quickwit_proto::cloudprem::aggregation::Aggregation::TimeGroupBy(
            Box::new(time_grouping),
        );
        let attr_group_by = quickwit_proto::cloudprem::AttributeGroupBy {
            include: None,
            expression: Some(ExpressionNode {
                calc_node: Some(Any {
                    type_url: "type.googleapis.com/calcfieldspb.CalcNode".to_string(),
                    value: vec![18, 8, 10, 6, 115, 116, 97, 116, 117, 115],
                }),
            }),
            limit: 100,
            sort: None,
            missing: None,
            total: None,
            child: Some(Box::new(quickwit_proto::cloudprem::Aggregation {
                aggregation: Some(time_group_by_inner),
            })),
        };
        let agg_defs_inner = quickwit_proto::cloudprem::aggregation::Aggregation::AttributeGroupBy(
            Box::new(attr_group_by),
        );
        let agg_defs = quickwit_proto::cloudprem::Aggregation {
            aggregation: Some(agg_defs_inner),
        };
        let expected = vec![AggregationResult {
            key: vec!["info".to_string(), "2025-01-30T08:00:00Z".to_string()],
            value: vec![2u64.to_value()],
        }];

        let res = aggregation_result_to_proto(aggregation_results, &agg_defs, 10).unwrap();
        assert_eq!(res, expected);
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

    #[test]
    fn test_aggregation_with_total() {
        // This aggregation has been extracted from the table widget request.
        let child = quickwit_proto::cloudprem::aggregation::Aggregation::Computes(Computes {
            aggregation: vec![Aggregation {
                aggregation: Some(
                    quickwit_proto::cloudprem::aggregation::Aggregation::MetricCompute(
                        MetricCompute {
                            expression: Some(count_expr()),
                            id: "count:count".to_string(),
                            r#type: "COUNT".to_string(),
                        },
                    ),
                ),
            }],
            time_grouping: vec![],
        });

        let attribute_group_by = AttributeGroupBy {
            include: None,
            expression: Some(host_expr()),
            limit: 2,
            sort: Some(SortByExprAndAgg {
                ascending: false,
                expr_and_agg: Some(ExprAndAgg {
                    expr: Some(count_expr()),
                    agg_function: "count".to_string(),
                }),
                r#type: SortType::Metric as i32,
            }),
            missing: None,
            total: Some("__TOTAL__".to_string()),
            child: Some(Box::new(Aggregation {
                aggregation: Some(child),
            })),
        };

        let agg_inner = quickwit_proto::cloudprem::aggregation::Aggregation::AttributeGroupBy(
            Box::new(attribute_group_by),
        );
        let agg = Aggregation {
            aggregation: Some(agg_inner),
        };

        let tantivy_aggs_ast = to_tantivy_aggregation(agg.clone(), 0i64).unwrap();
        let tantivy_aggs_ast_json = serde_json::to_value(&tantivy_aggs_ast).unwrap();

        assert_eq!(
            tantivy_aggs_ast_json,
            serde_json::json!(
                {
                   "host":{
                      "terms":{
                         "field":"host",
                         "size": 2
                      }
                   }
                }
            )
        );

        let searcher = test_searcher();
        let aggregation_collector =
            AggregationCollector::from_aggs(tantivy_aggs_ast, Default::default());
        let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();

        let evp_agg_results =
            super::aggregation_result_to_proto(aggregation_results.into(), &agg, 10).unwrap();

        assert_eq!(evp_agg_results[0].key, vec!["host_12".to_string()]);
        let val = evp_agg_results[0].value[0].value.as_ref().unwrap();
        assert_eq!(val, &agg_value::Value::Uint64Value(12));

        let total_aggregation_result = evp_agg_results
            .iter()
            .find(|el| &el.key[0] == "__TOTAL__")
            .unwrap();
        let total_val = total_aggregation_result.value[0].value.as_ref().unwrap();
        // We expect the total value to be the sum of all count from 1 to 12 included.
        // That's n(n+1)/2 where n = 12.
        assert_eq!(total_val, &agg_value::Value::Uint64Value(12 * 13 / 2));
    }

    #[test]
    fn test_aggregation_multiple_metrics() {
        // This aggregation has been extracted from the table widget request.
        let child = quickwit_proto::cloudprem::aggregation::Aggregation::Computes(Computes {
            aggregation: vec![
                Aggregation {
                    aggregation: Some(
                        quickwit_proto::cloudprem::aggregation::Aggregation::MetricCompute(
                            MetricCompute {
                                expression: Some(ExpressionNode {
                                    calc_node: Some(Any {
                                        type_url: "type.googleapis.com/calcfieldspb.CalcNode"
                                            .to_string(),
                                        value: vec![18u8, 7, 10, 5, 99, 111, 117, 110, 116],
                                    }),
                                }),
                                id: "count:count".to_string(),
                                r#type: "COUNT".to_string(),
                            },
                        ),
                    ),
                },
                Aggregation {
                    aggregation: Some(avg_value_metric()),
                },
            ],
            time_grouping: vec![],
        });

        let attribute_group_by = AttributeGroupBy {
            include: None,
            expression: Some(host_expr()),
            limit: 2,
            sort: Some(SortByExprAndAgg {
                ascending: false,
                expr_and_agg: Some(ExprAndAgg {
                    expr: Some(count_expr()),
                    agg_function: "count".to_string(),
                }),
                r#type: SortType::Metric as i32,
            }),
            missing: None,
            total: None,
            child: Some(Box::new(Aggregation {
                aggregation: Some(child),
            })),
        };

        let agg_inner = quickwit_proto::cloudprem::aggregation::Aggregation::AttributeGroupBy(
            Box::new(attribute_group_by),
        );
        let agg = Aggregation {
            aggregation: Some(agg_inner),
        };

        let tantivy_aggs_ast = to_tantivy_aggregation(agg.clone(), 0i64).unwrap();
        let tantivy_aggs_ast_json = serde_json::to_value(&tantivy_aggs_ast).unwrap();

        assert_eq!(
            tantivy_aggs_ast_json,
            serde_json::json!(
                {
                   "host":{
                      "terms":{
                         "field":"host",
                         "size": 2,
                      },
                      "aggs": {
                         "avg:avg": {
                            "avg": {
                               "field": "value",
                               "missing": null
                            }
                         }
                      }
                   }
                }
            )
        );

        let searcher = test_searcher();
        let aggregation_collector =
            AggregationCollector::from_aggs(tantivy_aggs_ast, Default::default());
        let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();

        let evp_agg_results =
            super::aggregation_result_to_proto(aggregation_results.into(), &agg, 10).unwrap();

        assert_eq!(evp_agg_results[0].key, vec!["host_12".to_string()]);
        let count = evp_agg_results[0].value[0].value.as_ref().unwrap();
        assert_eq!(count, &agg_value::Value::Uint64Value(12));
        let avg_value = evp_agg_results[0].value[1].value.as_ref().unwrap();
        // TODO fix when we support mergeable averages
        assert_eq!(
            avg_value,
            &agg_value::Value::AvgValue(Avg {
                sum: 12.0,
                count: 1
            })
        );
    }

    #[test]
    fn test_aggregation_group_with_sort_by_missing_metric() {
        // This aggregation has been extracted from the table widget request.
        let child = quickwit_proto::cloudprem::aggregation::Aggregation::Computes(Computes {
            aggregation: vec![Aggregation {
                aggregation: Some(count_metric()),
            }],
            time_grouping: vec![],
        });

        let attribute_group_by = AttributeGroupBy {
            include: None,
            expression: Some(host_expr()),
            limit: 2,
            sort: Some(SortByExprAndAgg {
                ascending: true,
                expr_and_agg: Some(ExprAndAgg {
                    expr: Some(field_expr("tag.source")),
                    agg_function: "cardinality".to_string(),
                }),
                r#type: SortType::Metric as i32,
            }),
            missing: None,
            child: Some(Box::new(Aggregation {
                aggregation: Some(child),
            })),
            total: None,
        };

        let agg_inner = quickwit_proto::cloudprem::aggregation::Aggregation::AttributeGroupBy(
            Box::new(attribute_group_by),
        );
        let mut agg = Aggregation {
            aggregation: Some(agg_inner),
        };
        sanitize_metric_id_aggregations(&mut agg);

        let tantivy_aggs_ast = to_tantivy_aggregation(agg.clone(), 0i64).unwrap();
        let tantivy_aggs_ast_json = serde_json::to_value(&tantivy_aggs_ast).unwrap();

        assert_eq!(
            tantivy_aggs_ast_json,
            serde_json::json!(
                {
                  "host": {
                    "aggs": {
                      "__sort_by_key": {
                        "cardinality": {
                          "field": "tag.source"
                        }
                      }
                    },
                    "terms": {
                      "field": "host",
                      "order": {
                        "__sort_by_key": "asc"
                      },
                      "size": 2
                    }
                  }
                }
            )
        );

        let searcher = test_searcher();
        let aggregation_collector =
            AggregationCollector::from_aggs(tantivy_aggs_ast, Default::default());
        let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();

        let evp_agg_results =
            super::aggregation_result_to_proto(aggregation_results.into(), &agg, 10).unwrap();

        assert_eq!(evp_agg_results[0].value.len(), 1);
    }

    #[test]
    fn test_aggregation_group_with_sort_by_present_metric() {
        // This aggregation has been extracted from the table widget request.
        let child = quickwit_proto::cloudprem::aggregation::Aggregation::Computes(Computes {
            aggregation: vec![
                Aggregation {
                    aggregation: Some(count_metric()),
                },
                Aggregation {
                    aggregation: Some(distinct_source_metric()),
                },
            ],
            time_grouping: vec![],
        });

        let attribute_group_by = AttributeGroupBy {
            include: None,
            expression: Some(host_expr()),
            limit: 2,
            sort: Some(SortByExprAndAgg {
                ascending: true,
                expr_and_agg: Some(ExprAndAgg {
                    expr: Some(field_expr("tag.source")),
                    agg_function: "cardinality".to_string(),
                }),
                r#type: SortType::Metric as i32,
            }),
            missing: None,
            child: Some(Box::new(Aggregation {
                aggregation: Some(child),
            })),
            total: None,
        };

        let agg_inner = quickwit_proto::cloudprem::aggregation::Aggregation::AttributeGroupBy(
            Box::new(attribute_group_by),
        );
        let mut agg = Aggregation {
            aggregation: Some(agg_inner),
        };

        super::sanitize_metric_id_aggregations(&mut agg);
        let tantivy_aggs_ast = to_tantivy_aggregation(agg.clone(), 0i64).unwrap();
        let tantivy_aggs_ast_json = serde_json::to_value(&tantivy_aggs_ast).unwrap();

        assert_eq!(
            tantivy_aggs_ast_json,
            serde_json::json!(
                {
                  "host": {
                    "aggs": {
                      "tag_DOT_source:cardinality": {
                        "cardinality": {
                          "field": "tag.source"
                        }
                      }
                    },
                    "terms": {
                      "field": "host",
                      "order": {
                        "tag_DOT_source:cardinality": "asc"
                      },
                      "size": 2
                    }
                  }
                }
            )
        );

        let searcher = test_searcher();
        let aggregation_collector =
            AggregationCollector::from_aggs(tantivy_aggs_ast, Default::default());
        let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();

        let evp_agg_results =
            super::aggregation_result_to_proto(aggregation_results.into(), &agg, 10).unwrap();

        assert_eq!(evp_agg_results[0].value.len(), 2);
    }

    #[test]
    fn test_aggregation_with_total_and_multiple_metrics() {
        // This aggregation has been extracted from the table widget request.
        let child = quickwit_proto::cloudprem::aggregation::Aggregation::Computes(Computes {
            aggregation: vec![
                Aggregation {
                    aggregation: Some(count_metric()),
                },
                Aggregation {
                    aggregation: Some(avg_value_metric()),
                },
            ],
            time_grouping: vec![],
        });

        let attribute_group_by = AttributeGroupBy {
            include: None,
            expression: Some(host_expr()),
            limit: 2,
            sort: Some(SortByExprAndAgg {
                ascending: false,
                expr_and_agg: Some(ExprAndAgg {
                    expr: Some(count_expr()),
                    agg_function: "count".to_string(),
                }),
                r#type: SortType::Metric as i32,
            }),
            missing: None,
            total: Some("__TOTAL__".to_string()),
            child: Some(Box::new(Aggregation {
                aggregation: Some(child),
            })),
        };

        let agg_inner = quickwit_proto::cloudprem::aggregation::Aggregation::AttributeGroupBy(
            Box::new(attribute_group_by),
        );
        let agg = Aggregation {
            aggregation: Some(agg_inner),
        };

        let tantivy_aggs_ast = to_tantivy_aggregation(agg.clone(), 0i64).unwrap();
        let tantivy_aggs_ast_json = serde_json::to_value(&tantivy_aggs_ast).unwrap();

        assert_eq!(
            tantivy_aggs_ast_json,
            serde_json::json!({
                   "host":{
                      "terms":{
                         "field":"host",
                         "size": 2,
                      },
                      "aggs": {
                         "avg:avg": {
                            "avg": {
                               "field": "value",
                               "missing": null
                            }
                         }
                      }
                   },
                   "host_TOTAL::avg:avg": {
                       "avg": {
                           "field": "value",
                           "missing": null
                       }
                   }
            })
        );

        let searcher = test_searcher();
        let aggregation_collector =
            AggregationCollector::from_aggs(tantivy_aggs_ast, Default::default());
        let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();

        let evp_agg_results =
            super::aggregation_result_to_proto(aggregation_results.into(), &agg, 10).unwrap();

        assert_eq!(evp_agg_results.len(), 3);
        assert_eq!(evp_agg_results[0].key, vec!["host_12".to_string()]);

        let count = evp_agg_results[0].value[0].value.as_ref().unwrap();
        assert_eq!(count, &agg_value::Value::Uint64Value(12));
        let avg_value = evp_agg_results[0].value[1].value.as_ref().unwrap();
        // TODO fix when we support mergeable averages
        assert_eq!(
            avg_value,
            &agg_value::Value::AvgValue(Avg {
                sum: 12.0,
                count: 1
            })
        );
        assert_eq!(evp_agg_results[2].key, vec!["__TOTAL__".to_string()]);
        let count_value = evp_agg_results[2].value[0].value.as_ref().unwrap();
        assert_eq!(count_value, &agg_value::Value::Uint64Value(78));
        let avg_value = evp_agg_results[2].value[1].value.as_ref().unwrap();
        assert_eq!(
            avg_value,
            &agg_value::Value::AvgValue(Avg {
                sum: 8.333333333333334,
                count: 1
            })
        );
    }

    // --- Intermediate aggregation result tests ---
    // These mirror the finalized result tests above but use DistributedAggregationCollector
    // to produce IntermediateAggregationResults and call intermediate_aggregation_result_to_proto.
    // Key differences vs finalized:
    //   - AVG returns raw {sum, count} instead of {computed_avg, 1}
    //   - Terms buckets are NOT sorted or limited (all buckets returned, order is unspecified)

    fn find_result_by_key<'a>(
        results: &'a [AggregationResult],
        key: &str,
    ) -> &'a AggregationResult {
        results
            .iter()
            .find(|r| r.key.first().map(|k| k.as_str()) == Some(key))
            .unwrap_or_else(|| panic!("expected result with key '{key}'"))
    }

    #[test]
    fn test_intermediate_count_response() {
        let agg_def_inner = count_metric();
        let agg_def = quickwit_proto::cloudprem::Aggregation {
            aggregation: Some(agg_def_inner),
        };
        let expected = vec![AggregationResult {
            key: vec![],
            value: vec![2u64.to_value()],
        }];

        let intermediate =
            tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults::default(
            );
        let res =
            super::intermediate_aggregation_result_to_proto(intermediate, &agg_def, 2).unwrap();
        assert_eq!(res, expected);
    }

    #[test]
    fn test_intermediate_aggregation_with_total() {
        let child = quickwit_proto::cloudprem::aggregation::Aggregation::Computes(Computes {
            aggregation: vec![Aggregation {
                aggregation: Some(count_metric()),
            }],
            time_grouping: vec![],
        });

        let attribute_group_by = AttributeGroupBy {
            include: None,
            expression: Some(host_expr()),
            limit: 2,
            sort: Some(SortByExprAndAgg {
                ascending: false,
                expr_and_agg: Some(ExprAndAgg {
                    expr: Some(count_expr()),
                    agg_function: "count".to_string(),
                }),
                r#type: SortType::Metric as i32,
            }),
            missing: None,
            total: Some("__TOTAL__".to_string()),
            child: Some(Box::new(Aggregation {
                aggregation: Some(child),
            })),
        };

        let agg_inner = quickwit_proto::cloudprem::aggregation::Aggregation::AttributeGroupBy(
            Box::new(attribute_group_by),
        );
        let agg = Aggregation {
            aggregation: Some(agg_inner),
        };

        let tantivy_aggs_ast = to_tantivy_aggregation(agg.clone(), 0i64).unwrap();
        let searcher = test_searcher();
        let distributed_collector =
            tantivy::aggregation::DistributedAggregationCollector::from_aggs(
                tantivy_aggs_ast,
                Default::default(),
            );
        let intermediate_results = searcher.search(&AllQuery, &distributed_collector).unwrap();

        let evp_agg_results =
            super::intermediate_aggregation_result_to_proto(intermediate_results, &agg, 10)
                .unwrap();

        // Intermediate results include all 12 host buckets + 1 __TOTAL__ row
        assert_eq!(evp_agg_results.len(), 13);

        // Verify host_12 bucket (12 docs)
        let host_12 = find_result_by_key(&evp_agg_results, "host_12");
        let val = host_12.value[0].value.as_ref().unwrap();
        assert_eq!(val, &agg_value::Value::Uint64Value(12));

        // Verify host_1 bucket (1 doc)
        let host_1 = find_result_by_key(&evp_agg_results, "host_1");
        let val = host_1.value[0].value.as_ref().unwrap();
        assert_eq!(val, &agg_value::Value::Uint64Value(1));

        let total = find_result_by_key(&evp_agg_results, "__TOTAL__");
        let total_val = total.value[0].value.as_ref().unwrap();
        assert_eq!(total_val, &agg_value::Value::Uint64Value(12 * 13 / 2));
    }

    #[test]
    fn test_intermediate_aggregation_multiple_metrics() {
        let child = quickwit_proto::cloudprem::aggregation::Aggregation::Computes(Computes {
            aggregation: vec![
                Aggregation {
                    aggregation: Some(
                        quickwit_proto::cloudprem::aggregation::Aggregation::MetricCompute(
                            MetricCompute {
                                expression: Some(ExpressionNode {
                                    calc_node: Some(Any {
                                        type_url: "type.googleapis.com/calcfieldspb.CalcNode"
                                            .to_string(),
                                        value: vec![18u8, 7, 10, 5, 99, 111, 117, 110, 116],
                                    }),
                                }),
                                id: "count:count".to_string(),
                                r#type: "COUNT".to_string(),
                            },
                        ),
                    ),
                },
                Aggregation {
                    aggregation: Some(avg_value_metric()),
                },
            ],
            time_grouping: vec![],
        });

        let attribute_group_by = AttributeGroupBy {
            include: None,
            expression: Some(host_expr()),
            limit: 2,
            sort: Some(SortByExprAndAgg {
                ascending: false,
                expr_and_agg: Some(ExprAndAgg {
                    expr: Some(count_expr()),
                    agg_function: "count".to_string(),
                }),
                r#type: SortType::Metric as i32,
            }),
            missing: None,
            total: None,
            child: Some(Box::new(Aggregation {
                aggregation: Some(child),
            })),
        };

        let agg_inner = quickwit_proto::cloudprem::aggregation::Aggregation::AttributeGroupBy(
            Box::new(attribute_group_by),
        );
        let agg = Aggregation {
            aggregation: Some(agg_inner),
        };

        let tantivy_aggs_ast = to_tantivy_aggregation(agg.clone(), 0i64).unwrap();
        let searcher = test_searcher();
        let distributed_collector =
            tantivy::aggregation::DistributedAggregationCollector::from_aggs(
                tantivy_aggs_ast,
                Default::default(),
            );
        let intermediate_results = searcher.search(&AllQuery, &distributed_collector).unwrap();

        let evp_agg_results =
            super::intermediate_aggregation_result_to_proto(intermediate_results, &agg, 10)
                .unwrap();

        // All 12 host buckets returned (limit not applied to intermediate results)
        assert_eq!(evp_agg_results.len(), 12);

        let host_12 = find_result_by_key(&evp_agg_results, "host_12");
        let count = host_12.value[0].value.as_ref().unwrap();
        assert_eq!(count, &agg_value::Value::Uint64Value(12));
        let avg_value = host_12.value[1].value.as_ref().unwrap();
        // Intermediate AVG returns raw sum and count for proper merging.
        // host_12 has 12 docs, all with value=12, so sum=144.0, count=12.
        assert_eq!(
            avg_value,
            &agg_value::Value::AvgValue(Avg {
                sum: 144.0,
                count: 12
            })
        );

        // Also verify a smaller bucket: host_3 has 3 docs with value=3
        let host_3 = find_result_by_key(&evp_agg_results, "host_3");
        let count = host_3.value[0].value.as_ref().unwrap();
        assert_eq!(count, &agg_value::Value::Uint64Value(3));
        let avg_value = host_3.value[1].value.as_ref().unwrap();
        assert_eq!(
            avg_value,
            &agg_value::Value::AvgValue(Avg { sum: 9.0, count: 3 })
        );
    }

    #[test]
    fn test_intermediate_aggregation_with_total_and_multiple_metrics() {
        let child = quickwit_proto::cloudprem::aggregation::Aggregation::Computes(Computes {
            aggregation: vec![
                Aggregation {
                    aggregation: Some(count_metric()),
                },
                Aggregation {
                    aggregation: Some(avg_value_metric()),
                },
            ],
            time_grouping: vec![],
        });

        let attribute_group_by = AttributeGroupBy {
            include: None,
            expression: Some(host_expr()),
            limit: 2,
            sort: Some(SortByExprAndAgg {
                ascending: false,
                expr_and_agg: Some(ExprAndAgg {
                    expr: Some(count_expr()),
                    agg_function: "count".to_string(),
                }),
                r#type: SortType::Metric as i32,
            }),
            missing: None,
            total: Some("__TOTAL__".to_string()),
            child: Some(Box::new(Aggregation {
                aggregation: Some(child),
            })),
        };

        let agg_inner = quickwit_proto::cloudprem::aggregation::Aggregation::AttributeGroupBy(
            Box::new(attribute_group_by),
        );
        let agg = Aggregation {
            aggregation: Some(agg_inner),
        };

        let tantivy_aggs_ast = to_tantivy_aggregation(agg.clone(), 0i64).unwrap();
        let searcher = test_searcher();
        let distributed_collector =
            tantivy::aggregation::DistributedAggregationCollector::from_aggs(
                tantivy_aggs_ast,
                Default::default(),
            );
        let intermediate_results = searcher.search(&AllQuery, &distributed_collector).unwrap();

        let evp_agg_results =
            super::intermediate_aggregation_result_to_proto(intermediate_results, &agg, 10)
                .unwrap();

        // 12 host buckets + 1 __TOTAL__ row
        assert_eq!(evp_agg_results.len(), 13);

        let host_12 = find_result_by_key(&evp_agg_results, "host_12");
        let count = host_12.value[0].value.as_ref().unwrap();
        assert_eq!(count, &agg_value::Value::Uint64Value(12));
        let avg_value = host_12.value[1].value.as_ref().unwrap();
        // Intermediate: host_12 has 12 docs with value=12, so sum=144.0, count=12
        assert_eq!(
            avg_value,
            &agg_value::Value::AvgValue(Avg {
                sum: 144.0,
                count: 12
            })
        );

        let total = find_result_by_key(&evp_agg_results, "__TOTAL__");
        let count_value = total.value[0].value.as_ref().unwrap();
        assert_eq!(count_value, &agg_value::Value::Uint64Value(78));
        let avg_value = total.value[1].value.as_ref().unwrap();
        // Intermediate: total is sum of k*k for k=1..12 = 650, count = 78
        assert_eq!(
            avg_value,
            &agg_value::Value::AvgValue(Avg {
                sum: 650.0,
                count: 78
            })
        );
    }

    // --- Percentile (QUANTILE_SKETCH) tests ---

    #[test]
    fn test_finalized_percentile_compute_errors() {
        // Finalized percentile must return an error because the DDSketch
        // is consumed during finalization and event-query requires raw DDSketch.
        let child = AggregationEnum::Computes(Computes {
            aggregation: vec![Aggregation {
                aggregation: Some(percentile_value_metric()),
            }],
            time_grouping: vec![],
        });
        let agg = Aggregation {
            aggregation: Some(child),
        };

        let tantivy_aggs_ast = to_tantivy_aggregation(agg.clone(), 0i64).unwrap();
        let searcher = test_searcher();
        let aggregation_collector =
            AggregationCollector::from_aggs(tantivy_aggs_ast, Default::default());
        let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();

        let result = super::aggregation_result_to_proto(aggregation_results.into(), &agg, 78);

        assert!(result.is_err(), "finalized QUANTILE_SKETCH should error");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("skip_aggregation_finalization"),
            "error should mention skip_aggregation_finalization, got: {err_msg}"
        );
    }

    #[test]
    fn test_intermediate_percentile_compute() {
        let child = AggregationEnum::Computes(Computes {
            aggregation: vec![Aggregation {
                aggregation: Some(percentile_value_metric()),
            }],
            time_grouping: vec![],
        });
        let agg = Aggregation {
            aggregation: Some(child),
        };

        let tantivy_aggs_ast = to_tantivy_aggregation(agg.clone(), 0i64).unwrap();
        let searcher = test_searcher();
        let distributed_collector =
            tantivy::aggregation::DistributedAggregationCollector::from_aggs(
                tantivy_aggs_ast,
                Default::default(),
            );
        let intermediate_results = searcher.search(&AllQuery, &distributed_collector).unwrap();

        let evp_agg_results =
            super::intermediate_aggregation_result_to_proto(intermediate_results, &agg, 78)
                .unwrap();

        assert_eq!(evp_agg_results.len(), 1);
        // Intermediate percentile returns DDSketch binary bytes
        let val = evp_agg_results[0].value[0].value.as_ref().unwrap();
        match val {
            agg_value::Value::SketchValue(bytes) => {
                assert!(!bytes.is_empty(), "sketch bytes should not be empty");
                // Non-empty DDSketch starts with FLAG_COUNT (0xA0)
                assert_eq!(
                    bytes[0], 0xA0,
                    "DDSketch binary should start with FLAG_COUNT (0xA0)"
                );
                assert!(
                    bytes.len() > 20,
                    "sketch bytes too small: {} bytes",
                    bytes.len()
                );
            }
            other => panic!("expected SketchValue for intermediate percentile, got {other:?}"),
        }
    }

    #[test]
    fn test_intermediate_percentile_with_group_by() {
        let child = AggregationEnum::Computes(Computes {
            aggregation: vec![
                Aggregation {
                    aggregation: Some(count_metric()),
                },
                Aggregation {
                    aggregation: Some(percentile_value_metric()),
                },
            ],
            time_grouping: vec![],
        });

        let attribute_group_by = AttributeGroupBy {
            include: None,
            expression: Some(host_expr()),
            limit: 100,
            sort: Some(SortByExprAndAgg {
                ascending: false,
                expr_and_agg: Some(ExprAndAgg {
                    expr: Some(count_expr()),
                    agg_function: "count".to_string(),
                }),
                r#type: SortType::Metric as i32,
            }),
            missing: None,
            total: None,
            child: Some(Box::new(Aggregation {
                aggregation: Some(child),
            })),
        };

        let agg_inner = AggregationEnum::AttributeGroupBy(Box::new(attribute_group_by));
        let agg = Aggregation {
            aggregation: Some(agg_inner),
        };

        let tantivy_aggs_ast = to_tantivy_aggregation(agg.clone(), 0i64).unwrap();
        let searcher = test_searcher();
        let distributed_collector =
            tantivy::aggregation::DistributedAggregationCollector::from_aggs(
                tantivy_aggs_ast,
                Default::default(),
            );
        let intermediate_results = searcher.search(&AllQuery, &distributed_collector).unwrap();

        let evp_agg_results =
            super::intermediate_aggregation_result_to_proto(intermediate_results, &agg, 78)
                .unwrap();

        // All 12 host buckets
        assert_eq!(evp_agg_results.len(), 12);

        // Each bucket should have count (u64) + percentile (sketch_value)
        let host_5 = find_result_by_key(&evp_agg_results, "host_5");
        assert_eq!(host_5.value.len(), 2);

        let count = host_5.value[0].value.as_ref().unwrap();
        assert_eq!(count, &agg_value::Value::Uint64Value(5));

        let sketch = host_5.value[1].value.as_ref().unwrap();
        match sketch {
            agg_value::Value::SketchValue(bytes) => {
                assert_eq!(
                    bytes[0], 0xA0,
                    "DDSketch binary should start with FLAG_COUNT"
                );
                assert!(bytes.len() > 10);
            }
            other => panic!("expected SketchValue, got {other:?}"),
        }
    }
}
