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

use serde::Deserialize;
use serde_with::formats::PreferMany;
use serde_with::{OneOrMany, serde_as};

use super::LeniencyBool;
use crate::elastic_query_dsl::bool_query::BoolQuery;
use crate::elastic_query_dsl::match_bool_prefix::MatchBoolPrefixQuery;
use crate::elastic_query_dsl::match_phrase_query::{MatchPhraseQuery, MatchPhraseQueryParams};
use crate::elastic_query_dsl::match_query::{MatchQuery, MatchQueryParams};
use crate::elastic_query_dsl::phrase_prefix_query::{
    MatchPhrasePrefixQuery, MatchPhrasePrefixQueryParams,
};
use crate::elastic_query_dsl::{ConvertibleToQueryAst, ElasticQueryDslInner};
use crate::query_ast::{DisjunctionMaxQuery, QueryAst};

/// Multi match queries are a bit odd. They end up being expanded into another type of query.
/// In Quickwit, we operate this expansion in generic way at the time of deserialization.
#[derive(Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(try_from = "MultiMatchQueryForDeserialization")]
pub struct MultiMatchQuery {
    match_type: MatchType,
    children: Vec<ElasticQueryDslInner>,
}

#[serde_as]
#[derive(Deserialize, Debug, Eq, PartialEq, Clone)]
struct MultiMatchQueryForDeserialization {
    #[serde(rename = "type", default)]
    match_type: MatchType,
    // Other parameters is used to dynamically collect more parameters.
    // We will then expand the query at the json level, and then deserialize the right object.
    #[serde(flatten)]
    other_parameters: serde_json::Map<String, serde_json::Value>,
    #[serde_as(deserialize_as = "OneOrMany<_, PreferMany>")]
    #[serde(default)]
    fields: Vec<String>,
    #[serde(default)]
    lenient: LeniencyBool,
}

fn deserialize_match_query_for_one_field(
    match_type: MatchType,
    field: &str,
    json_object: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Result<ElasticQueryDslInner> {
    let json_val = serde_json::Value::Object(json_object);
    match match_type {
        MatchType::Phrase => {
            let params: MatchPhraseQueryParams = serde_json::from_value(json_val)?;
            let phrase_query = MatchPhraseQuery {
                field: field.to_string(),
                params,
            };
            Ok(ElasticQueryDslInner::MatchPhrase(phrase_query))
        }
        MatchType::PhrasePrefix => {
            let phrase_prefix_params: MatchPhrasePrefixQueryParams =
                serde_json::from_value(json_val)?;
            let phrase_prefix = MatchPhrasePrefixQuery {
                field: field.to_string(),
                value: phrase_prefix_params,
            };
            Ok(ElasticQueryDslInner::MatchPhrasePrefix(phrase_prefix))
        }
        MatchType::BoolPrefix => {
            let bool_prefix_params: MatchQueryParams = serde_json::from_value(json_val)?;
            let bool_prefix = MatchBoolPrefixQuery {
                params: bool_prefix_params,
                field: field.to_string(),
            };
            Ok(ElasticQueryDslInner::MatchBoolPrefix(bool_prefix))
        }
        MatchType::MostFields | MatchType::BestFields | MatchType::CrossFields => {
            let match_query_params: MatchQueryParams = serde_json::from_value(json_val)?;
            let match_query = MatchQuery {
                field: field.to_string(),
                params: match_query_params,
            };
            Ok(ElasticQueryDslInner::Match(match_query))
        }
    }
}

fn validate_field_name(field_name: &str) -> Result<(), String> {
    if field_name.contains('^') {
        return Err(format!(
            "Quickwit does not support field boosting in the multi match query fields (got \
             `{field_name}`)"
        ));
    }
    if field_name.contains('*') {
        return Err(format!(
            "Quickwit does not support wildcards in the multi match query fields (got \
             `{field_name}`)"
        ));
    }
    Ok(())
}

impl TryFrom<MultiMatchQueryForDeserialization> for MultiMatchQuery {
    type Error = serde_json::Error;

    fn try_from(multi_match_query: MultiMatchQueryForDeserialization) -> Result<Self, Self::Error> {
        if multi_match_query.fields.is_empty() {
            // TODO: We can use default field from index configuration instead
            return Err(serde::de::Error::custom(
                "Quickwit does not support multi match query with 0 fields. MultiMatchQueries \
                 must have at least one field.",
            ));
        }
        for field in &multi_match_query.fields {
            validate_field_name(field).map_err(serde::de::Error::custom)?;
        }
        let mut children = Vec::new();
        for field in multi_match_query.fields {
            let child = deserialize_match_query_for_one_field(
                multi_match_query.match_type,
                &field,
                multi_match_query.other_parameters.clone(),
            )?;
            children.push(child);
        }
        Ok(MultiMatchQuery {
            match_type: multi_match_query.match_type,
            children,
        })
    }
}

#[derive(Deserialize, Debug, Default, Eq, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum MatchType {
    #[default]
    MostFields,
    BestFields,
    CrossFields, // Not implemented will be converted to MostFields
    Phrase,
    PhrasePrefix,
    BoolPrefix,
}

impl ConvertibleToQueryAst for MultiMatchQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        match self.match_type {
            MatchType::BestFields | MatchType::Phrase | MatchType::PhrasePrefix => {
                let children = self
                    .children
                    .into_iter()
                    .map(ElasticQueryDslInner::convert_to_query_ast)
                    .collect::<anyhow::Result<Vec<_>>>()?;
                Ok(DisjunctionMaxQuery {
                    disjuncts: children,
                }
                .into())
            }
            MatchType::MostFields | MatchType::CrossFields | MatchType::BoolPrefix => {
                BoolQuery::union(self.children).convert_to_query_ast()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::collector::TopDocs;
    use tantivy::schema::{Schema, TEXT};
    use tantivy::{Index, TantivyDocument, doc};

    use super::*;
    use crate::elastic_query_dsl::{ElasticQueryDsl, default_max_expansions};
    use crate::query_ast::BuildTantivyAstContext;

    #[track_caller]
    fn test_multimatch_query_ok_aux<T: Into<ElasticQueryDslInner>>(json: &str, expected: T) {
        let multi_match_query: MultiMatchQuery = serde_json::from_str(json).unwrap();
        let expected = expected.into().convert_to_query_ast().unwrap();
        let expected = match multi_match_query.match_type {
            MatchType::BestFields | MatchType::Phrase | MatchType::PhrasePrefix => {
                let QueryAst::Bool(bool_query) = expected else {
                    panic!("the expected query should be a boolean union")
                };
                DisjunctionMaxQuery {
                    disjuncts: bool_query.should,
                }
                .into()
            }
            MatchType::MostFields | MatchType::CrossFields | MatchType::BoolPrefix => expected,
        };
        let query_ast = multi_match_query.convert_to_query_ast().unwrap();
        assert_eq!(query_ast, expected);
    }

    #[track_caller]
    fn test_multimatch_query_err_aux(json: &str, expected_error_msg: &'static str) {
        let err_msg: String = serde_json::from_str::<MultiMatchQuery>(json)
            .unwrap_err()
            .to_string();
        assert!(err_msg.contains(expected_error_msg), "Got `{err_msg}`");
    }

    #[test]
    fn test_multimatch_query_deserialization() {
        test_multimatch_query_ok_aux(
            r#"{
                "query": "quick brown fox",
                "type": "most_fields",
                "fields": ["title", "body"]
            }"#,
            BoolQuery::union(vec![
                MatchQuery {
                    field: "title".to_string(),
                    params: MatchQueryParams {
                        query: "quick brown fox".to_string(),
                        operator: crate::BooleanOperand::Or,
                        zero_terms_query: Default::default(),
                        lenient: false,
                    },
                }
                .into(),
                MatchQuery {
                    field: "body".to_string(),
                    params: MatchQueryParams {
                        query: "quick brown fox".to_string(),
                        operator: crate::BooleanOperand::Or,
                        zero_terms_query: Default::default(),
                        lenient: false,
                    },
                }
                .into(),
            ]),
        );

        test_multimatch_query_ok_aux(
            r#"{
            "query": "quick brown fox",
            "type": "best_fields",
            "fields": ["title", "body"]
        }"#,
            BoolQuery::union(vec![
                MatchQuery {
                    field: "title".to_string(),
                    params: MatchQueryParams {
                        query: "quick brown fox".to_string(),
                        operator: crate::BooleanOperand::Or,
                        zero_terms_query: Default::default(),
                        lenient: false,
                    },
                }
                .into(),
                MatchQuery {
                    field: "body".to_string(),
                    params: MatchQueryParams {
                        query: "quick brown fox".to_string(),
                        operator: crate::BooleanOperand::Or,
                        zero_terms_query: Default::default(),
                        lenient: false,
                    },
                }
                .into(),
            ]),
        );

        test_multimatch_query_ok_aux(
            r#"{
            "query": "quick brown fox",
            "type": "cross_fields",
            "fields": ["title", "body"]
        }"#,
            BoolQuery::union(vec![
                MatchQuery {
                    field: "title".to_string(),
                    params: MatchQueryParams {
                        query: "quick brown fox".to_string(),
                        operator: crate::BooleanOperand::Or,
                        zero_terms_query: Default::default(),
                        lenient: false,
                    },
                }
                .into(),
                MatchQuery {
                    field: "body".to_string(),
                    params: MatchQueryParams {
                        query: "quick brown fox".to_string(),
                        operator: crate::BooleanOperand::Or,
                        zero_terms_query: Default::default(),
                        lenient: false,
                    },
                }
                .into(),
            ]),
        );

        test_multimatch_query_ok_aux(
            r#"{
            "query": "quick brown fox",
            "type": "phrase",
            "fields": ["title", "body"]
        }"#,
            BoolQuery::union(vec![
                MatchPhraseQuery {
                    field: "title".to_string(),
                    params: MatchPhraseQueryParams {
                        query: "quick brown fox".to_string(),
                        zero_terms_query: Default::default(),
                        analyzer: None,
                        slop: Default::default(),
                    },
                }
                .into(),
                MatchPhraseQuery {
                    field: "body".to_string(),
                    params: MatchPhraseQueryParams {
                        query: "quick brown fox".to_string(),
                        zero_terms_query: Default::default(),
                        analyzer: None,
                        slop: Default::default(),
                    },
                }
                .into(),
            ]),
        );

        test_multimatch_query_ok_aux(
            r#"{
            "query": "quick brown fox",
            "type": "phrase_prefix",
            "fields": ["title", "body"]
        }"#,
            BoolQuery::union(vec![
                MatchPhrasePrefixQuery {
                    field: "title".to_string(),
                    value: MatchPhrasePrefixQueryParams {
                        query: "quick brown fox".to_string(),
                        analyzer: Default::default(),
                        max_expansions: default_max_expansions(),
                        slop: Default::default(),
                        zero_terms_query: Default::default(),
                    },
                }
                .into(),
                MatchPhrasePrefixQuery {
                    field: "body".to_string(),
                    value: MatchPhrasePrefixQueryParams {
                        query: "quick brown fox".to_string(),
                        analyzer: Default::default(),
                        max_expansions: default_max_expansions(),
                        slop: Default::default(),
                        zero_terms_query: Default::default(),
                    },
                }
                .into(),
            ]),
        );

        test_multimatch_query_ok_aux(
            r#"{
            "query": "quick brown",
            "type": "bool_prefix",
            "fields": ["title", "body"]
        }"#,
            BoolQuery::union(vec![
                MatchBoolPrefixQuery {
                    field: "title".to_string(),
                    params: MatchQueryParams {
                        query: "quick brown".to_string(),
                        operator: crate::BooleanOperand::Or,
                        zero_terms_query: Default::default(),
                        lenient: false,
                    },
                }
                .into(),
                MatchBoolPrefixQuery {
                    field: "body".to_string(),
                    params: MatchQueryParams {
                        query: "quick brown".to_string(),
                        operator: crate::BooleanOperand::Or,
                        zero_terms_query: Default::default(),
                        lenient: false,
                    },
                }
                .into(),
            ]),
        );
    }

    #[test]
    fn test_multimatch_unsupported() {
        test_multimatch_query_err_aux(
            r#"{
                "query": "quick brown fox",
                "type": "most_fields",
                "fields": ["body", "body.*"]
            }"#,
            "Quickwit does not support wildcards",
        );
        test_multimatch_query_err_aux(
            r#"{
                "query": "quick brown fox",
                "type": "most_fields",
                "fields": ["body", "title^3"]
            }"#,
            "Quickwit does not support field boosting",
        );
    }

    #[test]
    fn test_multimatch_field_score_combination() {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT);
        let body = schema_builder.add_text_field("body", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index
            .writer_with_num_threads::<TantivyDocument>(1, 50_000_000)
            .unwrap();
        writer
            .add_document(doc!(title => "alpha", body => "beta"))
            .unwrap();
        writer
            .add_document(doc!(title => "alpha", body => "alpha"))
            .unwrap();
        writer
            .add_document(doc!(title => "beta", body => "alpha"))
            .unwrap();
        writer.commit().unwrap();

        let schema = index.schema();
        let context = BuildTantivyAstContext::for_test(&schema);
        let searcher = index.reader().unwrap().searcher();
        let scores_for_type = |match_type: &str| {
            let json = format!(
                r#"{{"multi_match": {{"query": "alpha", "type": "{match_type}", "fields": ["title", "body"]}}}}"#
            );
            let query_dsl: ElasticQueryDsl = serde_json::from_str(&json).unwrap();
            let query_ast = QueryAst::try_from(query_dsl).unwrap();
            let query = query_ast.build_tantivy_query(&context).unwrap();
            searcher
                .search(&*query, &TopDocs::with_limit(3).order_by_score())
                .unwrap()
        };

        let best_fields_scores = scores_for_type("best_fields");
        assert_eq!(best_fields_scores.len(), 3);
        assert_eq!(best_fields_scores[0].0, best_fields_scores[2].0);

        let most_fields_scores = scores_for_type("most_fields");
        assert_eq!(most_fields_scores.len(), 3);
        assert!(most_fields_scores[0].0 > most_fields_scores[1].0);

        let bool_prefix_scores = scores_for_type("bool_prefix");
        assert_eq!(bool_prefix_scores.len(), 3);
        assert!(bool_prefix_scores[0].0 > bool_prefix_scores[1].0);
    }
}
