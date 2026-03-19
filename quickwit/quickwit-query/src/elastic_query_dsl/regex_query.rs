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

use crate::elastic_query_dsl::ConvertibleToQueryAst;
use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::query_ast::{QueryAst, RegexQuery as AstRegexQuery};

/// Elasticsearch supports two formats for regexp queries:
/// - Shorthand: `{"regexp": {"field": "pattern"}}`
/// - Full:      `{"regexp": {"field": {"value": "pattern", "case_insensitive": true}}}`
#[derive(Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(untagged)]
pub enum RegexQueryParams {
    Full {
        #[serde(rename = "value")]
        pattern: String,
        #[serde(default)]
        case_insensitive: bool,
    },
    Shorthand(String),
}

impl RegexQueryParams {
    fn into_tuple(self) -> (String, bool) {
        match self {
            RegexQueryParams::Full {
                pattern,
                case_insensitive,
            } => (pattern, case_insensitive),
            RegexQueryParams::Shorthand(pattern) => (pattern, false),
        }
    }
}

pub type RegexQuery = OneFieldMap<RegexQueryParams>;

impl ConvertibleToQueryAst for RegexQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let (pattern, case_insensitive) = self.value.into_tuple();

        let regex = if case_insensitive {
            format!("(?i){pattern}")
        } else {
            pattern
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
        let (pattern, case_insensitive) = query.value.into_tuple();
        assert_eq!(pattern, ".*logs.*");
        assert!(!case_insensitive);
    }

    #[test]
    fn test_regex_query_full_format() {
        let json = serde_json::json!({"service": {"value": ".*logs.*", "case_insensitive": true}});
        let query: RegexQuery = serde_json::from_value(json).unwrap();
        assert_eq!(query.field, "service");
        let (pattern, case_insensitive) = query.value.into_tuple();
        assert_eq!(pattern, ".*logs.*");
        assert!(case_insensitive);
    }

    #[test]
    fn test_regex_query_full_format_default_case() {
        let json = serde_json::json!({"service": {"value": ".*logs.*"}});
        let query: RegexQuery = serde_json::from_value(json).unwrap();
        assert_eq!(query.field, "service");
        let (pattern, case_insensitive) = query.value.into_tuple();
        assert_eq!(pattern, ".*logs.*");
        assert!(!case_insensitive);
    }
}
