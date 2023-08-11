// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use serde::Deserialize;
use serde_with::formats::PreferMany;
use serde_with::{serde_as, OneOrMany};

use crate::elastic_query_dsl::bool_query::BoolQuery;
use crate::elastic_query_dsl::match_phrase_query::{MatchPhraseQuery, MatchPhraseQueryParams};
use crate::elastic_query_dsl::match_query::{MatchQuery, MatchQueryParams};
use crate::elastic_query_dsl::phrase_prefix_query::{
    MatchPhrasePrefixQuery, MatchPhrasePrefixQueryParams,
};
use crate::elastic_query_dsl::{ConvertableToQueryAst, ElasticQueryDslInner};

/// Multi match queries are a bit odd. They end up being expanded into another type query of query.
/// In Quickwit, we operate this expansion in generic way at the time of deserialization.
#[derive(Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(try_from = "MultiMatchQueryForDeserialization")]
pub struct MultiMatchQuery(Box<ElasticQueryDslInner>);

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
    // Regardless of this option Quickwit behaves in elasticsearch definition of
    // lenient. We include this property here just to accept user queries containing
    // this option.
    #[serde(default, rename = "lenient")]
    _lenient: bool,
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
        MatchType::MostFields => {
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
            "Quickwit does not support field boosting in the multi match query fields (Got `{}`)",
            field_name
        ));
    }
    if field_name.contains('*') {
        return Err(format!(
            "Quickwit does not support wildcards in the multi match query fields (Got `{}`)",
            field_name
        ));
    }
    Ok(())
}

impl TryFrom<MultiMatchQueryForDeserialization> for MultiMatchQuery {
    type Error = serde_json::Error;

    fn try_from(multi_match_query: MultiMatchQueryForDeserialization) -> Result<Self, Self::Error> {
        if multi_match_query.fields.is_empty() {
            return Err(serde::de::Error::custom(
                "Quickwit does not support multi match query with 0 fields. MultiMatchQuery must \
                 have at least one field.",
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
        let bool_query = BoolQuery::union(children);
        Ok(MultiMatchQuery(Box::new(ElasticQueryDslInner::Bool(
            bool_query,
        ))))
    }
}

#[derive(Deserialize, Debug, Default, Eq, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum MatchType {
    #[default]
    MostFields,
    Phrase,
    PhrasePrefix,
}

impl ConvertableToQueryAst for MultiMatchQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<crate::query_ast::QueryAst> {
        self.0.convert_to_query_ast()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[track_caller]
    fn test_multimatch_query_ok_aux<T: Into<ElasticQueryDslInner>>(json: &str, expected: T) {
        let expected: ElasticQueryDslInner = expected.into();
        let multi_match_query: MultiMatchQuery = serde_json::from_str(json).unwrap();
        let es_query = &*multi_match_query.0;
        assert_eq!(es_query, &expected);
    }

    #[track_caller]
    fn test_multimatch_query_err_aux(json: &str, expected_error_msg: &'static str) {
        let err_msg: String = serde_json::from_str::<MultiMatchQuery>(json)
            .unwrap_err()
            .to_string();
        assert!(err_msg.contains(expected_error_msg), "Got `{}`", err_msg);
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
                        _lenient: false,
                    },
                }
                .into(),
                MatchQuery {
                    field: "body".to_string(),
                    params: MatchQueryParams {
                        query: "quick brown fox".to_string(),
                        operator: crate::BooleanOperand::Or,
                        zero_terms_query: Default::default(),
                        _lenient: false,
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
}
