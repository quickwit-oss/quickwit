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

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::elastic_query_dsl::ConvertibleToQueryAst;
use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::query_ast::{QueryAst, RegexQuery as AstRegexQuery};

/// Elasticsearch supports two formats for regexp queries:
/// - Shorthand: `{"regexp": {"field": "pattern"}}`
/// - Full:      `{"regexp": {"field": {"value": "pattern", "case_insensitive": true}}}`
#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct RegexQueryParams {
    value: String,
    case_insensitive: bool,
}

impl<'de> Deserialize<'de> for RegexQueryParams {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RegexQueryParamsVisitor;

        impl<'de> Visitor<'de> for RegexQueryParamsVisitor {
            type Value = RegexQueryParams;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string pattern or an object with `value` and optional `case_insensitive`")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<RegexQueryParams, E> {
                Ok(RegexQueryParams {
                    value: value.to_string(),
                    case_insensitive: false,
                })
            }

            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<RegexQueryParams, M::Error> {
                #[derive(Deserialize)]
                struct FullParams {
                    value: String,
                    #[serde(default)]
                    case_insensitive: bool,
                }
                let full = FullParams::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(RegexQueryParams {
                    value: full.value,
                    case_insensitive: full.case_insensitive,
                })
            }
        }

        deserializer.deserialize_any(RegexQueryParamsVisitor)
    }
}

pub type RegexQuery = OneFieldMap<RegexQueryParams>;

impl ConvertibleToQueryAst for RegexQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let regex = if self.value.case_insensitive {
            format!("(?i){}", self.value.value)
        } else {
            self.value.value.clone()
        };
        Ok(AstRegexQuery {
            field: self.field,
            regex,
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regex_query_shorthand_format() {
        let json = serde_json::json!({"service": ".*logs.*"});
        let query: RegexQuery = serde_json::from_value(json).unwrap();
        assert_eq!(query.field, "service");
        assert_eq!(query.value.value, ".*logs.*");
        assert!(!query.value.case_insensitive);
    }

    #[test]
    fn test_regex_query_full_format() {
        let json = serde_json::json!({"service": {"value": ".*logs.*", "case_insensitive": true}});
        let query: RegexQuery = serde_json::from_value(json).unwrap();
        assert_eq!(query.field, "service");
        assert_eq!(query.value.value, ".*logs.*");
        assert!(query.value.case_insensitive);
    }

    #[test]
    fn test_regex_query_full_format_default_case() {
        let json = serde_json::json!({"service": {"value": ".*logs.*"}});
        let query: RegexQuery = serde_json::from_value(json).unwrap();
        assert_eq!(query.field, "service");
        assert_eq!(query.value.value, ".*logs.*");
        assert!(!query.value.case_insensitive);
    }
}
