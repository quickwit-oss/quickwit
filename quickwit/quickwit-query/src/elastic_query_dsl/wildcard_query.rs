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

use crate::NotNaNf32;
use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::elastic_query_dsl::{ConvertibleToQueryAst, StringOrStructForSerialization};
use crate::query_ast::{QueryAst, WildcardQuery as AstWildcardQuery};

#[derive(Deserialize, Clone, Eq, PartialEq, Debug)]
#[serde(from = "OneFieldMap<StringOrStructForSerialization<WildcardQueryParams>>")]
pub(crate) struct WildcardQuery {
    pub(crate) field: String,
    pub(crate) params: WildcardQueryParams,
}

#[derive(Deserialize, Debug, Default, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct WildcardQueryParams {
    value: String,
    #[serde(default)]
    pub boost: Option<NotNaNf32>,
    #[serde(default)]
    case_insensitive: bool,
}

impl ConvertibleToQueryAst for WildcardQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let wildcard_ast: QueryAst = AstWildcardQuery {
            field: self.field,
            value: self.params.value,
            lenient: true,
            case_insensitive: self.params.case_insensitive,
        }
        .into();
        Ok(wildcard_ast.boost(self.params.boost))
    }
}

impl From<OneFieldMap<StringOrStructForSerialization<WildcardQueryParams>>> for WildcardQuery {
    fn from(
        match_query_params: OneFieldMap<StringOrStructForSerialization<WildcardQueryParams>>,
    ) -> Self {
        let OneFieldMap { field, value } = match_query_params;
        WildcardQuery {
            field,
            params: value.inner,
        }
    }
}

impl From<String> for WildcardQueryParams {
    fn from(value: String) -> WildcardQueryParams {
        WildcardQueryParams {
            value,
            boost: None,
            case_insensitive: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wildcard_query_convert_to_query_ast() {
        let wildcard_query_json = r#"{
            "user_name": {
                "value": "john*"
            }
        }"#;
        let wildcard_query: WildcardQuery = serde_json::from_str(wildcard_query_json).unwrap();
        let query_ast = wildcard_query.convert_to_query_ast().unwrap();

        if let QueryAst::Wildcard(wildcard) = query_ast {
            assert_eq!(wildcard.field, "user_name");
            assert_eq!(wildcard.value, "john*");
            assert!(wildcard.lenient);
        } else {
            panic!("Expected QueryAst::Wildcard");
        }
    }

    #[test]
    fn test_boosted_wildcard_query_convert_to_query_ast() {
        let wildcard_query_json = r#"{
            "user_name": {
                "value": "john*",
                "boost": 2.0
            }
        }"#;
        let wildcard_query: WildcardQuery = serde_json::from_str(wildcard_query_json).unwrap();
        let query_ast = wildcard_query.convert_to_query_ast().unwrap();

        if let QueryAst::Boost { underlying, boost } = query_ast {
            if let QueryAst::Wildcard(wildcard) = *underlying {
                assert_eq!(wildcard.field, "user_name");
                assert_eq!(wildcard.value, "john*");
                assert!(wildcard.lenient);
            } else {
                panic!("Expected underlying QueryAst::Wildcard");
            }
            assert_eq!(boost, NotNaNf32::try_from(2.0).unwrap());
        } else {
            panic!("Expected QueryAst::Wildcard");
        }
    }
}
