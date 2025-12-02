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

use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::elastic_query_dsl::{ConvertibleToQueryAst, StringOrStructForSerialization};
use crate::query_ast::{QueryAst, WildcardQuery as AstWildcardQuery};

#[derive(Deserialize, Clone, Eq, PartialEq, Debug)]
#[serde(from = "OneFieldMap<StringOrStructForSerialization<PrefixQueryParams>>")]
pub(crate) struct PrefixQuery {
    pub(crate) field: String,
    pub(crate) params: PrefixQueryParams,
}

#[derive(Deserialize, Debug, Default, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct PrefixQueryParams {
    value: String,
    #[serde(default)]
    case_insensitive: bool,
}

impl ConvertibleToQueryAst for PrefixQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let wildcard = format!(
            "{}*",
            self.params
                .value
                .replace(r"\", r"\\")
                .replace("*", r"\*")
                .replace("?", r"\?")
        );
        Ok(AstWildcardQuery {
            field: self.field,
            value: wildcard,
            lenient: true,
            case_insensitive: self.params.case_insensitive,
        }
        .into())
    }
}

impl From<OneFieldMap<StringOrStructForSerialization<PrefixQueryParams>>> for PrefixQuery {
    fn from(
        match_query_params: OneFieldMap<StringOrStructForSerialization<PrefixQueryParams>>,
    ) -> Self {
        let OneFieldMap { field, value } = match_query_params;
        PrefixQuery {
            field,
            params: value.inner,
        }
    }
}

impl From<String> for PrefixQueryParams {
    fn from(value: String) -> PrefixQueryParams {
        PrefixQueryParams {
            value,
            case_insensitive: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_query_convert_to_query_ast() {
        let prefix_query_json = r#"{
            "user_name": {
                "value": "john"
            }
        }"#;
        let prefix_query: PrefixQuery = serde_json::from_str(prefix_query_json).unwrap();
        let query_ast = prefix_query.convert_to_query_ast().unwrap();

        if let QueryAst::Wildcard(prefix) = query_ast {
            assert_eq!(prefix.field, "user_name");
            assert_eq!(prefix.value, "john*");
            assert!(prefix.lenient);
        } else {
            panic!("Expected QueryAst::Prefix, got {:?}", query_ast);
        }
    }

    #[test]
    fn test_prefix_query_convert_to_query_ast_special_chars() {
        let prefix_query_json = r#"{
            "user_name": {
                "value": "a\\dm?n*"
            }
        }"#;
        let prefix_query: PrefixQuery = serde_json::from_str(prefix_query_json).unwrap();
        let query_ast = prefix_query.convert_to_query_ast().unwrap();

        if let QueryAst::Wildcard(prefix) = query_ast {
            assert_eq!(prefix.field, "user_name");
            assert_eq!(prefix.value, r"a\\dm\?n\**");
            assert!(prefix.lenient);
        } else {
            panic!("Expected QueryAst::Prefix, got {:?}", query_ast);
        }
    }
}
