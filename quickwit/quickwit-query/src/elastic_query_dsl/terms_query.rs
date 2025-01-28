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

use crate::elastic_query_dsl::bool_query::BoolQuery;
use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::elastic_query_dsl::term_query::term_query_from_field_value;
use crate::elastic_query_dsl::{ConvertibleToQueryAst, ElasticQueryDslInner};
use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::QueryAst;

#[derive(PartialEq, Eq, Debug, Deserialize, Clone)]
#[serde(try_from = "TermsQueryForSerialization")]
pub struct TermsQuery {
    pub boost: Option<NotNaNf32>,
    pub field: String,
    pub values: Vec<String>,
}

#[derive(Deserialize)]
struct TermsQueryForSerialization {
    #[serde(default)]
    boost: Option<NotNaNf32>,
    #[serde(flatten)]
    capture_other: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}
impl From<OneOrMany> for Vec<String> {
    fn from(one_or_many: OneOrMany) -> Vec<String> {
        match one_or_many {
            OneOrMany::One(one_value) => vec![one_value],
            OneOrMany::Many(values) => values,
        }
    }
}

impl TryFrom<TermsQueryForSerialization> for TermsQuery {
    type Error = serde_json::Error;

    fn try_from(value: TermsQueryForSerialization) -> serde_json::Result<TermsQuery> {
        let one_field: OneFieldMap<OneOrMany> = serde_json::from_value(value.capture_other)?;
        let one_field_values: Vec<String> = one_field.value.into();
        Ok(TermsQuery {
            boost: value.boost,
            field: one_field.field,
            values: one_field_values,
        })
    }
}

impl ConvertibleToQueryAst for TermsQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let term_queries: Vec<ElasticQueryDslInner> = self
            .values
            .into_iter()
            .map(|value| term_query_from_field_value(self.field.clone(), value))
            .map(ElasticQueryDslInner::from)
            .collect();
        let mut union = BoolQuery::union(term_queries);
        union.boost = self.boost;
        union.convert_to_query_ast()
    }
}

impl From<TermsQuery> for ElasticQueryDslInner {
    fn from(term_query: TermsQuery) -> Self {
        Self::Terms(term_query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terms_query_simple() {
        let terms_query_json = r#"{ "user.id": ["hello", "happy"] }"#;
        let terms_query: TermsQuery = serde_json::from_str(terms_query_json).unwrap();
        assert_eq!(&terms_query.field, "user.id");
        assert_eq!(
            &terms_query.values[..],
            &["hello".to_string(), "happy".to_string()]
        );
    }

    #[test]
    fn test_terms_query_single_term_not_array() {
        let terms_query_json = r#"{ "user.id": "hello"}"#;
        let terms_query: TermsQuery = serde_json::from_str(terms_query_json).unwrap();
        assert_eq!(&terms_query.field, "user.id");
        assert_eq!(&terms_query.values[..], &["hello".to_string()]);
    }

    #[test]
    fn test_terms_query_single_term_boost() {
        let terms_query_json = r#"{ "user.id": ["hello", "happy"], "boost": 2 }"#;
        let terms_query: TermsQuery = serde_json::from_str(terms_query_json).unwrap();
        assert_eq!(&terms_query.field, "user.id");
        assert_eq!(
            &terms_query.values[..],
            &["hello".to_string(), "happy".to_string()]
        );
        let boost: f32 = terms_query.boost.unwrap().into();
        assert!((boost - 2.0f32).abs() < 0.0001f32);
    }
}
