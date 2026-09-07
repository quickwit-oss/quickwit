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

use serde::{Deserialize, Serialize};
use tantivy::jitexpr::ast::UntypedExpr;
use tantivy::query::doc_predicate_query::{DocPredicateQuery, JitExprPredicate};

use super::{BuildTantivyAst, BuildTantivyAstContext, QueryAst, TantivyQueryAst};
use crate::InvalidQuery;

/// A boolean predicate expressed as a calculated-field expression.
///
/// The expression is serialized as a jitexpr string, for example `(GT duration 1i64)`.
/// Variable names refer to fast fields, resolved by Tantivy separately for each segment.
/// Query construction validates boolean result types; column binding and JIT compilation
/// happen when Tantivy creates the segment scorer. Callers must warm the referenced fast
/// fields before running a synchronous search against remote storage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CalcFieldQuery {
    #[serde(with = "jitexpr_serde")]
    pub expression: UntypedExpr,
}

impl From<CalcFieldQuery> for QueryAst {
    fn from(calc_field_query: CalcFieldQuery) -> Self {
        QueryAst::CalcField(calc_field_query)
    }
}

impl BuildTantivyAst for CalcFieldQuery {
    fn build_tantivy_ast_impl(
        &self,
        _context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let predicate = JitExprPredicate::new(self.expression.clone()).map_err(|error| {
            InvalidQuery::Other(anyhow::anyhow!(
                "invalid calculated predicate expression: {error}"
            ))
        })?;
        let query: DocPredicateQuery = predicate.into();
        Ok(query.into())
    }
}

mod jitexpr_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use tantivy::jitexpr::ast::UntypedExpr;

    pub fn serialize<S>(expression: &UntypedExpr, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&tantivy::jitexpr::ast::serialize(expression))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<UntypedExpr, D::Error>
    where D: Deserializer<'de> {
        let expression = String::deserialize(deserializer)?;
        tantivy::jitexpr::ast::deserialize(&expression).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use serde_json::json;
    use tantivy::collector::{Count, TopDocs};
    use tantivy::jitexpr::ast::deserialize;
    use tantivy::query::doc_predicate_query::DocPredicateQuery;
    use tantivy::schema::{FAST, STRING, Schema};
    use tantivy::{Index, TantivyDocument, doc};

    use super::CalcFieldQuery;
    use crate::InvalidQuery;
    use crate::query_ast::{
        BoolQuery, BuildTantivyAstContext, CacheNode, QueryAst, QueryAstTransformer,
        QueryAstVisitor, TermQuery,
    };

    fn calc_field(expression: &str) -> QueryAst {
        CalcFieldQuery {
            expression: deserialize(expression).unwrap(),
        }
        .into()
    }

    #[test]
    fn test_calc_field_serde_roundtrip() {
        let query = calc_field("(GT custom.duration 1i64)");
        let serialized = json!({
            "type": "calc_field",
            "expression": "(GT custom.duration 1i64)"
        });
        assert_eq!(serde_json::to_value(&query).unwrap(), serialized);
        assert_eq!(
            serde_json::from_value::<QueryAst>(serialized).unwrap(),
            query
        );
        assert!(format!("{query:?}").contains("(GT custom.duration 1i64)"));
    }

    #[test]
    fn test_calc_field_hash_prefixed_field_serde_roundtrip() {
        let query = calc_field("(GT #custom.duration 1i64)");
        let serialized = json!({
            "type": "calc_field",
            "expression": "(GT #custom.duration 1i64)"
        });
        assert_eq!(serde_json::to_value(&query).unwrap(), serialized);
        assert_eq!(
            serde_json::from_value::<QueryAst>(serialized).unwrap(),
            query
        );
    }

    #[test]
    fn test_calc_field_rejects_malformed_serialization() {
        for expression in [")", "(GT duration", "(UNKNOWN duration)"] {
            let serialized = json!({"type": "calc_field", "expression": expression});
            assert!(
                serde_json::from_value::<QueryAst>(serialized).is_err(),
                "{expression}"
            );
        }
        for serialized in [
            json!({"type": "calc_field"}),
            json!({"type": "calc_field", "expression": 42}),
            json!({"type": "calc_field", "expression": {}}),
        ] {
            assert!(serde_json::from_value::<QueryAst>(serialized).is_err());
        }
    }

    fn nested_query() -> QueryAst {
        let predicate = calc_field("(GT duration 1i64)");
        BoolQuery {
            must: vec![predicate.clone()],
            must_not: vec![predicate.clone()],
            should: vec![predicate.clone().boost(Some(2.0f32.try_into().unwrap()))],
            filter: vec![CacheNode::new(predicate).into()],
            ..Default::default()
        }
        .into()
    }

    #[derive(Default)]
    struct CountCalcFields {
        count: usize,
    }

    impl<'a> QueryAstVisitor<'a> for CountCalcFields {
        type Err = Infallible;

        fn visit_calc_field(&mut self, _query: &'a CalcFieldQuery) -> Result<(), Self::Err> {
            self.count += 1;
            Ok(())
        }
    }

    #[test]
    fn test_calc_field_parse_user_query_preserves_nested_predicates() {
        let query = nested_query();
        assert_eq!(query.clone().parse_user_query(&[]).unwrap(), query);
    }

    #[test]
    fn test_calc_field_visitor_reaches_nested_predicates() {
        let query = nested_query();
        let mut visitor = CountCalcFields::default();
        visitor.visit(&query).unwrap();
        assert_eq!(visitor.count, 4);
    }

    #[test]
    fn test_calc_field_default_transform_preserves_nested_predicates() {
        struct Identity;
        impl QueryAstTransformer for Identity {
            type Err = Infallible;
        }

        let query = nested_query();
        assert_eq!(Identity.transform(query.clone()).unwrap(), Some(query));
    }

    #[test]
    fn test_calc_field_transform_replaces_nested_predicates() {
        struct ReplaceCalcFields;
        impl QueryAstTransformer for ReplaceCalcFields {
            type Err = Infallible;

            fn transform_calc_field(
                &mut self,
                _query: CalcFieldQuery,
            ) -> Result<Option<QueryAst>, Self::Err> {
                Ok(Some(QueryAst::MatchAll))
            }
        }

        let query = ReplaceCalcFields
            .transform(nested_query())
            .unwrap()
            .unwrap();
        let expected: QueryAst = BoolQuery {
            must: vec![QueryAst::MatchAll],
            must_not: vec![QueryAst::MatchAll],
            should: vec![QueryAst::MatchAll.boost(Some(2.0f32.try_into().unwrap()))],
            filter: vec![CacheNode::new(QueryAst::MatchAll).into()],
            ..Default::default()
        }
        .into();
        assert_eq!(query, expected);
    }

    #[test]
    fn test_calc_field_builds_tantivy_doc_predicate() {
        let schema = Schema::builder().build();
        let context = BuildTantivyAstContext::for_test(&schema);
        let query = calc_field("(GT duration 1i64)")
            .build_tantivy_query(&context)
            .unwrap();
        assert!(query.downcast_ref::<DocPredicateQuery>().is_some());
    }

    #[test]
    fn test_calc_field_rejects_invalid_expressions_at_query_construction() {
        let schema = Schema::builder().build();
        let context = BuildTantivyAstContext::for_test(&schema);
        // jitexpr deserialization checks syntax; arity and types are checked during lowering.
        for expression in ["1i64", "(ADD duration 1i64)", "(GT duration)"] {
            let error = calc_field(expression)
                .build_tantivy_query(&context)
                .unwrap_err();
            assert!(matches!(error, InvalidQuery::Other(_)));
            assert!(
                error
                    .to_string()
                    .contains("invalid calculated predicate expression")
            );
        }
    }

    fn test_index() -> Index {
        let mut schema_builder = Schema::builder();
        let duration = schema_builder.add_i64_field("duration", FAST);
        let label = schema_builder.add_text_field("label", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index
            .writer_with_num_threads::<TantivyDocument>(1, 50_000_000)
            .unwrap();
        writer
            .add_document(doc!(duration => 1i64, label => "keep"))
            .unwrap();
        writer
            .add_document(doc!(duration => 3i64, label => "drop"))
            .unwrap();
        writer
            .add_document(doc!(duration => 7i64, label => "keep"))
            .unwrap();
        writer.add_document(doc!(label => "keep")).unwrap();
        writer.commit().unwrap();
        index
    }

    #[test]
    fn test_calc_field_search() {
        let index = test_index();
        let schema = index.schema();
        let context = BuildTantivyAstContext::for_test(&schema);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        for (expression, expected_count) in [
            ("true", 4),
            ("false", 0),
            ("(GT duration 2i64)", 2),
            ("(EQ label \"keep\")", 3),
            ("(GT absent 2i64)", 0),
        ] {
            let query = calc_field(expression)
                .build_tantivy_query(&context)
                .unwrap();
            assert_eq!(
                searcher.search(&*query, &Count).unwrap(),
                expected_count,
                "{expression}"
            );
        }
    }

    #[test]
    fn test_calc_field_composes_with_term_filter() {
        let index = test_index();
        let schema = index.schema();
        let context = BuildTantivyAstContext::for_test(&schema);
        let ast: QueryAst = BoolQuery {
            must: vec![
                TermQuery {
                    field: "label".to_string(),
                    value: "keep".to_string(),
                }
                .into(),
            ],
            filter: vec![calc_field("(GT duration 2i64)")],
            ..Default::default()
        }
        .into();
        let query = ast.build_tantivy_query(&context).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().search(&*query, &Count).unwrap(), 1);
    }

    #[test]
    fn test_calc_field_boosts_constant_score() {
        let index = test_index();
        let schema = index.schema();
        let context = BuildTantivyAstContext::for_test(&schema);
        let query = calc_field("(GT duration 2i64)")
            .boost(Some(4.0f32.try_into().unwrap()))
            .build_tantivy_query(&context)
            .unwrap();
        let reader = index.reader().unwrap();
        let hits = reader
            .searcher()
            .search(&*query, &TopDocs::with_limit(10).order_by_score())
            .unwrap();
        assert_eq!(hits.len(), 2);
        for (score, _address) in hits {
            assert_eq!(score, 4.0);
        }
    }
}
