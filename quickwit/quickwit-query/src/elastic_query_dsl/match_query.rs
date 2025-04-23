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

use super::LeniencyBool;
use crate::elastic_query_dsl::{
    ConvertibleToQueryAst, ElasticQueryDslInner, StringOrStructForSerialization,
};
use crate::query_ast::{FullTextParams, FullTextQuery, QueryAst};
use crate::{BooleanOperand, MatchAllOrNone, OneFieldMap};

/// `MatchQuery` as defined in
/// <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html>
#[derive(Deserialize, Clone, Eq, PartialEq, Debug)]
#[serde(from = "OneFieldMap<StringOrStructForSerialization<MatchQueryParams>>")]
pub struct MatchQuery {
    pub(crate) field: String,
    pub(crate) params: MatchQueryParams,
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct MatchQueryParams {
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) operator: BooleanOperand,
    #[serde(default)]
    pub(crate) zero_terms_query: MatchAllOrNone,
    #[serde(default)]
    pub(crate) lenient: LeniencyBool,
}

impl ConvertibleToQueryAst for MatchQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let full_text_params = FullTextParams {
            tokenizer: None,
            mode: self.params.operator.into(),
            zero_terms_query: self.params.zero_terms_query,
        };
        Ok(QueryAst::FullText(FullTextQuery {
            field: self.field,
            text: self.params.query,
            params: full_text_params,
            lenient: self.params.lenient,
        }))
    }
}

impl From<MatchQuery> for ElasticQueryDslInner {
    fn from(match_query: MatchQuery) -> Self {
        ElasticQueryDslInner::Match(match_query)
    }
}

impl From<OneFieldMap<StringOrStructForSerialization<MatchQueryParams>>> for MatchQuery {
    fn from(
        match_query_params: OneFieldMap<StringOrStructForSerialization<MatchQueryParams>>,
    ) -> Self {
        let OneFieldMap { field, value } = match_query_params;
        MatchQuery {
            field,
            params: value.inner,
        }
    }
}

impl From<String> for MatchQueryParams {
    fn from(query: String) -> MatchQueryParams {
        MatchQueryParams {
            query,
            zero_terms_query: Default::default(),
            operator: Default::default(),
            lenient: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_ast::FullTextMode;

    #[test]
    fn test_deserialize_match_query_string() {
        // We accept a single string
        let match_query: MatchQuery = serde_json::from_str(r#"{"my_field": "my_query"}"#).unwrap();
        assert_eq!(match_query.field, "my_field");
        assert_eq!(&match_query.params.query, "my_query");
        assert_eq!(match_query.params.operator, BooleanOperand::Or);
    }

    #[test]
    fn test_deserialize_match_query_struct() {
        // We accept a struct too.
        let match_query: MatchQuery =
            serde_json::from_str(r#"{"my_field": {"query": "my_query", "operator": "AND"}}"#)
                .unwrap();
        assert_eq!(match_query.field, "my_field");
        assert_eq!(&match_query.params.query, "my_query");
        assert_eq!(match_query.params.operator, BooleanOperand::And);
    }

    #[test]
    fn test_deserialize_match_query_nice_errors() {
        let deser_error = serde_json::from_str::<MatchQuery>(
            r#"{"my_field": {"query": "my_query", "wrong_param": 2}}"#,
        )
        .unwrap_err();
        assert!(
            deser_error
                .to_string()
                .contains("unknown field `wrong_param`")
        );
    }

    #[test]
    fn test_match_query() {
        let match_query = MatchQuery {
            field: "body".to_string(),
            params: MatchQueryParams {
                query: "hello".to_string(),
                operator: BooleanOperand::And,
                zero_terms_query: crate::MatchAllOrNone::MatchAll,
                lenient: false,
            },
        };
        let ast = match_query.convert_to_query_ast().unwrap();
        let QueryAst::FullText(FullTextQuery {
            field,
            text,
            params,
            lenient: _,
        }) = ast
        else {
            panic!()
        };
        assert_eq!(field, "body");
        assert_eq!(text, "hello");
        assert_eq!(
            params.mode,
            FullTextMode::Bool {
                operator: BooleanOperand::And
            }
        );
        assert_eq!(params.zero_terms_query, MatchAllOrNone::MatchAll);
    }
}
