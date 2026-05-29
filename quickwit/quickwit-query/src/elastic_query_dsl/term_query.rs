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

use serde::{Deserialize, Deserializer, Serialize};

use super::LiteralOrStructForSerialization;
use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::elastic_query_dsl::{ConvertibleToQueryAst, ElasticQueryDslInner};
use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::{self, QueryAst};

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(from = "OneFieldMap<LiteralOrStructForSerialization<TermQueryParams>>")]
pub struct TermQuery {
    pub field: String,
    pub value: TermQueryParams,
}

impl From<OneFieldMap<LiteralOrStructForSerialization<TermQueryParams>>> for TermQuery {
    fn from(one_field_map: OneFieldMap<LiteralOrStructForSerialization<TermQueryParams>>) -> Self {
        TermQuery {
            field: one_field_map.field,
            value: one_field_map.value.inner,
        }
    }
}

impl From<String> for TermQueryParams {
    fn from(query: String) -> TermQueryParams {
        TermQueryParams {
            value: query,
            boost: None,
            case_insensitive: false,
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum TermValue {
    I64(i64),
    U64(u64),
    Str(String),
    Bool(bool),
    F64(f64),
}

fn deserialize_term_value<'de, D>(deserializer: D) -> Result<String, D::Error>
where D: Deserializer<'de> {
    let term_value = TermValue::deserialize(deserializer)?;
    match term_value {
        TermValue::I64(i64) => Ok(i64.to_string()),
        TermValue::U64(u64) => Ok(u64.to_string()),
        TermValue::Str(str) => Ok(str),
        TermValue::Bool(b) => Ok(b.to_string()),
        TermValue::F64(f) => Ok(f.to_string()),
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TermQueryParams {
    #[serde(deserialize_with = "deserialize_term_value")]
    pub value: String,
    #[serde(default)]
    pub boost: Option<NotNaNf32>,
    #[serde(default)]
    case_insensitive: bool,
}

#[cfg(test)]
pub fn term_query_from_field_value(field: impl ToString, value: impl ToString) -> TermQuery {
    TermQuery {
        field: field.to_string(),
        value: TermQueryParams {
            value: value.to_string(),
            boost: None,
            case_insensitive: false,
        },
    }
}

impl From<TermQuery> for ElasticQueryDslInner {
    fn from(term_query: TermQuery) -> Self {
        Self::Term(term_query)
    }
}

impl ConvertibleToQueryAst for TermQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let TermQueryParams {
            value,
            boost,
            case_insensitive,
        } = self.value;
        if case_insensitive {
            let ci_value = format!("(?i){}", regex::escape(&value));
            let term_ast: QueryAst = query_ast::RegexQuery {
                field: self.field,
                regex: ci_value,
            }
            .into();
            return Ok(term_ast.boost(boost));
        }
        let term_ast: QueryAst = query_ast::TermQuery {
            field: self.field,
            value,
        }
        .into();
        Ok(term_ast.boost(boost))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_term_query_string() {
        let term_query_json = r#"{ "product_id": { "value": "61809" } }"#;
        let term_query: TermQuery = serde_json::from_str(term_query_json).unwrap();
        assert_eq!(
            &term_query,
            &term_query_from_field_value("product_id", "61809")
        );
    }

    #[test]
    fn test_term_query_string_short_form() {
        let term_query: TermQuery = serde_json::from_str(
            r#"{
            "product_id": "61809"
        }"#,
        )
        .unwrap();
        assert_eq!(
            &term_query,
            &term_query_from_field_value("product_id", "61809")
        );
    }

    #[test]
    fn test_term_query_bool() {
        let term_query_json = r#"{ "is_product_pretty": { "value": true } }"#;
        let term_query: TermQuery = serde_json::from_str(term_query_json).unwrap();
        assert_eq!(
            &term_query,
            &term_query_from_field_value("is_product_pretty", "true")
        );
    }

    #[test]
    fn test_term_query_bool_short_form() {
        let term_query_json = r#"{ "is_product_pretty": true }"#;
        let term_query: TermQuery = serde_json::from_str(term_query_json).unwrap();
        assert_eq!(
            &term_query,
            &term_query_from_field_value("is_product_pretty", "true")
        );
    }

    #[test]
    fn test_term_query_float() {
        let term_query_json = r#"{ "price": { "value": 1.1 } }"#;
        let term_query: TermQuery = serde_json::from_str(term_query_json).unwrap();
        assert_eq!(&term_query, &term_query_from_field_value("price", "1.1"));
    }

    #[test]
    fn test_term_query_float_short_form() {
        let term_query_json = r#"{ "price": 1.1 }"#;
        let term_query: TermQuery = serde_json::from_str(term_query_json).unwrap();
        assert_eq!(&term_query, &term_query_from_field_value("price", "1.1"));
    }
}
