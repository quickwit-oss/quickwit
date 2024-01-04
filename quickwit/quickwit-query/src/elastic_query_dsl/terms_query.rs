// Copyright (C) 2024 Quickwit, Inc.
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

use crate::elastic_query_dsl::bool_query::BoolQuery;
use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::elastic_query_dsl::term_query::term_query_from_field_value;
use crate::elastic_query_dsl::{ConvertableToQueryAst, ElasticQueryDslInner};
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

impl ConvertableToQueryAst for TermsQuery {
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
