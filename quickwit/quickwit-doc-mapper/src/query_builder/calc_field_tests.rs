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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use quickwit_query::query_ast::{
    BoolQuery, BuildTantivyAstContext, CacheNode, CalcFieldQuery, HitSet, PredicateCache, QueryAst,
    TermQuery,
};
use tantivy::Term;
use tantivy::index::SegmentId;
use tantivy::jitexpr::ast::deserialize;
use tantivy::schema::{Schema, TEXT};

use super::build_query;
use crate::tag_pruning::extract_tags_from_query;
use crate::{FastFieldWarmupInfo, WarmupInfo};

fn calc_field(expression: &str) -> QueryAst {
    CalcFieldQuery {
        expression: deserialize(expression).unwrap(),
    }
    .into()
}

fn expected_fast_fields(names: &[&str]) -> HashSet<FastFieldWarmupInfo> {
    let mut fields = HashSet::with_capacity(names.len());
    for name in names {
        fields.insert(FastFieldWarmupInfo {
            name: (*name).to_string(),
            with_subfields: false,
        });
    }
    fields
}

fn warmup_info(query: QueryAst) -> WarmupInfo {
    // Input names are resolved per segment by Tantivy, not filtered by the builder's schema.
    let schema = Schema::builder().build();
    let context = BuildTantivyAstContext::for_test(&schema);
    build_query(query, &context, None).unwrap().1
}

#[test]
fn test_calc_field_warmup_collects_nested_inputs_and_deduplicates() {
    let query = calc_field(
        "(AND (GT (ADD duration duration) #computed) (EQ (LOWER custom.label) \"ignored.field\"))",
    );
    assert_eq!(
        warmup_info(query),
        WarmupInfo {
            fast_fields: expected_fast_fields(&["duration", "#computed", "custom.label"]),
            ..Default::default()
        }
    );
}

#[test]
fn test_calc_field_warmup_constants_need_no_fields() {
    for expression in ["true", "false", "(EQ 1i64 1i64)"] {
        assert_eq!(warmup_info(calc_field(expression)), WarmupInfo::default());
    }
}

#[test]
fn test_calc_field_warmup_visits_boolean_boost_and_uninitialized_cache_nodes() {
    let query: QueryAst = BoolQuery {
        must: vec![calc_field("must_field")],
        must_not: vec![calc_field("must_not_field")],
        should: vec![calc_field("should_field").boost(Some(2.0f32.try_into().unwrap()))],
        filter: vec![CacheNode::new(calc_field("filter_field")).into()],
        ..Default::default()
    }
    .into();
    assert_eq!(
        warmup_info(query).fast_fields,
        expected_fast_fields(&[
            "must_field",
            "must_not_field",
            "should_field",
            "filter_field"
        ])
    );
}

fn term_query() -> QueryAst {
    TermQuery {
        field: "label".to_string(),
        value: "keep".to_string(),
    }
    .into()
}
