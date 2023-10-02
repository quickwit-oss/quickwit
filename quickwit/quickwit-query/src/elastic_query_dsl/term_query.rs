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

use serde::{Deserialize, Deserializer};

use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::elastic_query_dsl::{ConvertableToQueryAst, ElasticQueryDslInner};
use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::{self, QueryAst};

#[derive(Deserialize, Clone, Eq, PartialEq, Debug)]
#[serde(
    from = "OneFieldMap<TermQueryParamsForDeserialization>",
    into = "OneFieldMap<TermQueryValue>"
)]
pub struct TermQuery {
    pub(crate) field: String,
    pub(crate) params: TermQueryParams,
}

#[derive(PartialEq, Eq, Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TermQueryParams {
    #[serde(deserialize_with = "from_string_or_number")]
    pub value: String,
    #[serde(default)]
    pub boost: Option<NotNaNf32>,
}

pub fn term_query_from_field_value(field: impl ToString, value: impl ToString) -> TermQuery {
    TermQuery {
        field: field.to_string(),
        params: TermQueryParams {
            value: value.to_string(),
            boost: None,
        },
    }
}

impl From<TermQuery> for ElasticQueryDslInner {
    fn from(term_query: TermQuery) -> Self {
        Self::Term(term_query)
    }
}

impl ConvertableToQueryAst for TermQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let TermQueryParams { value, boost } = self.params;
        let term_ast: QueryAst = query_ast::TermQuery {
            field: self.field,
            value,
        }
        .into();
        Ok(term_ast.boost(boost))
    }
}

// --------------
//
// Below is the Deserialization code
// We want to support the following JSON formats:
//
// `{"field": {"value": "term", "boost": null}}`
// `{"field": "value"}`
// `{"field": 123}`
//
// We don't use untagged enum to support this, in order to keep good errors.
//
// The code below is adapted from solution described here: https://serde.rs/string-or-struct.html

#[derive(Deserialize)]
#[serde(transparent)]
pub(crate) struct TermQueryParamsForDeserialization {
    #[serde(deserialize_with = "string_or_struct")]
    pub(crate) inner: TermQueryParams,
}

fn from_string_or_number<'de, D>(deserializer: D) -> Result<String, D::Error>
where D: Deserializer<'de> {
    let json_value: serde_json::Value = Deserialize::deserialize(deserializer)?;
    match json_value {
        serde_json::Value::String(string) => Ok(string),
        serde_json::Value::Number(number) => Ok(number.to_string()),
        value => Err(serde::de::Error::custom(format!(
            "Expected a string or a number, got {:?}",
            value
        ))),
    }
}

impl From<OneFieldMap<TermQueryParamsForDeserialization>> for TermQuery {
    fn from(term_query_params: OneFieldMap<TermQueryParamsForDeserialization>) -> Self {
        let OneFieldMap { field, value } = term_query_params;
        TermQuery {
            field,
            params: value.inner,
        }
    }
}

struct TermQueryValueStringOrStructVisitor;

impl<'de> serde::de::Visitor<'de> for TermQueryValueStringOrStructVisitor {
    type Value = TermQueryParams;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("string or map containing the parameters of a match query.")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where E: serde::de::Error {
        Ok(TermQueryParams {
            value: value.to_string(),
            boost: None,
        })
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where E: serde::de::Error {
        Ok(TermQueryParams {
            value: value.to_string(),
            boost: None,
        })
    }

    fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
    where M: serde::de::MapAccess<'de> {
        Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(map))
    }
}

fn string_or_struct<'de, D>(deserializer: D) -> Result<TermQueryParams, D::Error>
where D: Deserializer<'de> {
    deserializer.deserialize_any(TermQueryValueStringOrStructVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_term_query_simple() {
        let term_query_json_1 = r#"{ "product_id": { "value": 61809, "boost": null } }"#;
        let term_query_1: TermQuery = serde_json::from_str(term_query_json_1).unwrap();
        assert_eq!(
            &term_query_1,
            &term_query_from_field_value("product_id", "61809")
        );

        let term_query_json_2 = r#"{ "product_id": { "value": "61809" } }"#;
        let term_query_2: TermQuery = serde_json::from_str(term_query_json_2).unwrap();
        assert_eq!(
            &term_query_2,
            &term_query_from_field_value("product_id", "61809")
        );

        let term_query_json_3 = r#"{ "product_id": "61809" }"#;
        let term_query_3: TermQuery = serde_json::from_str(term_query_json_3).unwrap();
        assert_eq!(
            &term_query_3,
            &term_query_from_field_value("product_id", "61809")
        );
    }
}
