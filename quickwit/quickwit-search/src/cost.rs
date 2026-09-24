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

use quickwit_proto::search::SearchRequest;
use quickwit_query::query_ast::{
    BoolQuery, FieldPresenceQuery, FullTextQuery, PhrasePrefixQuery, QueryAst, QueryAstVisitor,
    RangeQuery, TermQuery, TermSetQuery, UserInputQuery, WildcardQuery,
};
use tantivy::aggregation::agg_req::{AggregationVariants, Aggregations};

use crate::SearchError;
use crate::collector::QuickwitAggregations;

// The following weights are relative. They were found by testing different query shapes in a
// benchmark environment and comparing relative costs to a control.
const FIXED_SPLIT_COST: usize = 5;
const DOCS_PER_COST_UNIT: u64 = 100_000;
const DEPTH_MULTIPLIER: f32 = 1.5;
const Q_TERM: f32 = 1.0;
const Q_RANGE: f32 = 1.5;
const Q_WC_TRAILING: f32 = 2.0;
const Q_WC_LEADING: f32 = 125.0;
const Q_PHRASE_PREFIX: f32 = 1.5;
const AGG_TERMS: f32 = 3.0;
const AGG_COMPOSITE: f32 = 6.0;
const AGG_DATE_HISTOGRAM: f32 = 8.0;
const AGG_DEFAULT: f32 = 3.0;

// Factor used to compute the cost of searching a split for the specified search request
//
// query_complexity_factor = shape_cost + agg_cost
// shape_cost is based on what is in the query ast. agg_cost is based on the aggregation, if present
pub(crate) fn compute_query_complexity_factor(
    search_request: &SearchRequest,
) -> crate::Result<f32> {
    let query_ast: QueryAst = serde_json::from_str(&search_request.query_ast)
        .map_err(|err| SearchError::InvalidQuery(err.to_string()))?;
    let mut visitor = ShapeCostVisitor {
        total: 0.0,
        bool_depth: 0,
    };
    let _ = visitor.visit(&query_ast);
    let shape_cost = visitor.total.max(1.0);
    let aggregation_cost = agg_cost(search_request.aggregation_request.as_deref())?;
    Ok(shape_cost + aggregation_cost)
}

// The cost of searching a split with `num_docs` docs and the specified `query_complexity_factor`
// split_cost = hit_cost * query_complexity_factor
pub(crate) fn compute_split_query_cost(num_docs: u64, query_complexity_factor: f32) -> usize {
    let hit_cost = FIXED_SPLIT_COST as f32 + (num_docs / DOCS_PER_COST_UNIT) as f32;
    (hit_cost * query_complexity_factor) as usize
}

fn agg_cost(aggregation_request: Option<&str>) -> crate::Result<f32> {
    let Some(aggregation_request) = aggregation_request else {
        return Ok(0.0);
    };
    let aggregations: QuickwitAggregations = serde_json::from_str(aggregation_request)
        .map_err(|err| SearchError::InvalidAggregationRequest(err.to_string()))?;
    match aggregations {
        QuickwitAggregations::FindTraceIdsAggregation(_) => Ok(AGG_DEFAULT),
        QuickwitAggregations::TantivyAggregations(aggregations) => {
            Ok(agg_tree_cost(&aggregations, 1))
        }
    }
}

fn agg_tree_cost(aggregations: &Aggregations, depth: u32) -> f32 {
    let mut total = 0.0;
    for aggregation in aggregations.values() {
        total += agg_type_multiplier(&aggregation.agg) * depth as f32;
        total += agg_tree_cost(&aggregation.sub_aggregation, depth + 1);
    }
    total
}

fn agg_type_multiplier(aggregation: &AggregationVariants) -> f32 {
    match aggregation {
        AggregationVariants::Terms(_) => AGG_TERMS,
        AggregationVariants::Composite(_) => AGG_COMPOSITE,
        AggregationVariants::DateHistogram(_) => AGG_DATE_HISTOGRAM,
        _ => AGG_DEFAULT,
    }
}

/// Walks the query AST and sums per-clause shape costs, scaling each leaf by its
/// nesting depth. `bool_depth` counts how many `bool` nodes visitor is currently
/// inside; the outermost `bool`'s direct clauses are treated as depth 0.
struct ShapeCostVisitor {
    total: f32,
    bool_depth: i32,
}

impl ShapeCostVisitor {
    fn add_leaf(&mut self, multiplier: f32) {
        let depth = (self.bool_depth - 1).max(0);
        let depth_factor = (depth as f32 * DEPTH_MULTIPLIER).max(1.0);
        self.total += multiplier * depth_factor;
    }
}

impl<'a> QueryAstVisitor<'a> for ShapeCostVisitor {
    type Err = std::convert::Infallible;

    fn visit_bool(&mut self, bool_query: &'a BoolQuery) -> Result<(), Self::Err> {
        self.bool_depth += 1;
        for clause in bool_query
            .must
            .iter()
            .chain(bool_query.filter.iter())
            .chain(bool_query.should.iter())
            .chain(bool_query.must_not.iter())
        {
            self.visit(clause)?;
        }
        self.bool_depth -= 1;
        Ok(())
    }

    fn visit_term(&mut self, _: &'a TermQuery) -> Result<(), Self::Err> {
        self.add_leaf(Q_TERM);
        Ok(())
    }

    fn visit_term_set(&mut self, _: &'a TermSetQuery) -> Result<(), Self::Err> {
        self.add_leaf(Q_TERM);
        Ok(())
    }

    fn visit_full_text(&mut self, _: &'a FullTextQuery) -> Result<(), Self::Err> {
        self.add_leaf(Q_TERM);
        Ok(())
    }

    fn visit_match_all(&mut self) -> Result<(), Self::Err> {
        self.add_leaf(Q_TERM);
        Ok(())
    }

    fn visit_exists(&mut self, _: &'a FieldPresenceQuery) -> Result<(), Self::Err> {
        self.add_leaf(Q_TERM);
        Ok(())
    }

    fn visit_user_text(&mut self, _: &'a UserInputQuery) -> Result<(), Self::Err> {
        self.add_leaf(Q_TERM);
        Ok(())
    }

    fn visit_range(&mut self, _: &'a RangeQuery) -> Result<(), Self::Err> {
        // TODO: Consider discounting timestamp filters.
        self.add_leaf(Q_RANGE);
        Ok(())
    }

    fn visit_wildcard(&mut self, wildcard_query: &'a WildcardQuery) -> Result<(), Self::Err> {
        if wildcard_query.value.starts_with('*') {
            self.add_leaf(Q_WC_LEADING);
        } else {
            self.add_leaf(Q_WC_TRAILING);
        }
        Ok(())
    }

    fn visit_phrase_prefix(&mut self, _: &'a PhrasePrefixQuery) -> Result<(), Self::Err> {
        self.add_leaf(Q_PHRASE_PREFIX);
        Ok(())
    }

    fn visit_match_none(&mut self) -> Result<(), Self::Err> {
        Ok(())
    }

    // visit_boost / visit_cache_node use the trait defaults, which recurse into
    // the inner AST transparently (cache_node skips on a cache hit).
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::*;

    fn term() -> QueryAst {
        TermQuery {
            field: "status".to_string(),
            value: "error".to_string(),
        }
        .into()
    }

    #[test]
    fn test_leaf_query_factors() {
        let range: QueryAst = RangeQuery {
            field: "duration".to_string(),
            lower_bound: Bound::Included(10u64.into()),
            upper_bound: Bound::Excluded(100u64.into()),
        }
        .into();
        for (query, expected) in [(QueryAst::MatchAll, 1.0), (term(), 1.0), (range, 1.5)] {
            let request = SearchRequest {
                query_ast: serde_json::to_string(&query).unwrap(),
                ..Default::default()
            };
            assert_eq!(compute_query_complexity_factor(&request).unwrap(), expected);
        }
    }

    #[test]
    fn test_bool_cost() {
        let wrapped = BoolQuery {
            must: vec![term()],
            ..Default::default()
        };
        let mut request = SearchRequest {
            query_ast: serde_json::to_string(&QueryAst::Bool(wrapped.clone())).unwrap(),
            ..Default::default()
        };
        assert_eq!(compute_query_complexity_factor(&request).unwrap(), 1.0);

        let mut query = BoolQuery {
            must: vec![term()],
            filter: vec![term()],
            should: vec![term()],
            must_not: vec![term()],
            ..Default::default()
        };
        request.query_ast = serde_json::to_string(&QueryAst::Bool(query.clone())).unwrap();
        assert_eq!(compute_query_complexity_factor(&request).unwrap(), 4.0);

        query.must = vec![wrapped.into()];
        request.query_ast = serde_json::to_string(&QueryAst::Bool(query)).unwrap();
        // The nested term costs 1.5; the three outer clauses still cost 1.0 each.
        assert_eq!(compute_query_complexity_factor(&request).unwrap(), 4.5);
    }

    #[test]
    fn test_wildcard_factors() {
        for (value, expected) in [("error*", 2.0), ("*error", 125.0)] {
            let query = WildcardQuery {
                field: "message".to_string(),
                value: value.to_string(),
                lenient: false,
                case_insensitive: false,
            };
            let request = SearchRequest {
                query_ast: serde_json::to_string(&QueryAst::Wildcard(query)).unwrap(),
                ..Default::default()
            };
            assert_eq!(compute_query_complexity_factor(&request).unwrap(), expected);
        }
    }

    #[test]
    fn test_aggregation_factors() {
        let mut request = SearchRequest {
            query_ast: serde_json::to_string(&term()).unwrap(),
            ..Default::default()
        };
        assert_eq!(compute_query_complexity_factor(&request).unwrap(), 1.0);
        for (aggregation, expected) in [
            (r#"{"a":{"terms":{"field":"host"}}}"#, 4.0),
            (
                r#"{"a":{"composite":{"size":10,"sources":[{"s":{"terms":{"field":"host"}}}]}}}"#,
                7.0,
            ),
            (
                r#"{"a":{"date_histogram":{"field":"timestamp","fixed_interval":"1h"}}}"#,
                9.0,
            ),
            (r#"{"a":{"avg":{"field":"duration"}}}"#, 4.0),
            (
                r#"{"num_traces":20,"trace_id_field_name":"trace_id","span_timestamp_field_name":"span_start_timestamp_nanos"}"#,
                4.0,
            ),
        ] {
            request.aggregation_request = Some(aggregation.to_string());
            assert_eq!(
                compute_query_complexity_factor(&request).unwrap(),
                expected,
                "{aggregation}"
            );
        }
    }

    #[test]
    fn test_invalid_requests_return_errors() {
        let mut request = SearchRequest {
            query_ast: "not json".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            compute_query_complexity_factor(&request),
            Err(SearchError::InvalidQuery(_))
        ));

        request.query_ast = serde_json::to_string(&term()).unwrap();
        request.aggregation_request = Some("not json".to_string());
        assert!(matches!(
            compute_query_complexity_factor(&request),
            Err(SearchError::InvalidAggregationRequest(_))
        ));
    }

    #[test]
    fn test_aggregation_tree_cost() {
        let siblings = r#"{
            "hosts":{"terms":{"field":"host"}},
            "services":{"terms":{"field":"service"}}
        }"#;
        let nested = r#"{
            "hosts":{"terms":{"field":"host"},"aggs":{
                "services":{"terms":{"field":"service"}}
            }}
        }"#;
        let mut request = SearchRequest {
            query_ast: serde_json::to_string(&term()).unwrap(),
            aggregation_request: Some(siblings.to_string()),
            ..Default::default()
        };
        assert_eq!(compute_query_complexity_factor(&request).unwrap(), 7.0);
        request.aggregation_request = Some(nested.to_string());
        assert_eq!(compute_query_complexity_factor(&request).unwrap(), 10.0);
    }
}
