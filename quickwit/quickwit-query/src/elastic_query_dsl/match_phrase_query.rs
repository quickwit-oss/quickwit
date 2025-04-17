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

use crate::elastic_query_dsl::{
    ConvertibleToQueryAst, ElasticQueryDslInner, StringOrStructForSerialization,
};
use crate::query_ast::{FullTextMode, FullTextParams, FullTextQuery, QueryAst};
use crate::{MatchAllOrNone, OneFieldMap};

/// `MatchPhraseQuery` as defined in
/// <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html>
#[derive(Deserialize, Clone, Eq, PartialEq, Debug)]
#[serde(from = "OneFieldMap<StringOrStructForSerialization<MatchPhraseQueryParams>>")]
pub(crate) struct MatchPhraseQuery {
    pub(crate) field: String,
    pub(crate) params: MatchPhraseQueryParams,
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
#[serde(deny_unknown_fields)]
pub struct MatchPhraseQueryParams {
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) zero_terms_query: MatchAllOrNone,
    #[serde(default)]
    pub(crate) analyzer: Option<String>,
    #[serde(default)]
    pub(crate) slop: u32,
}

impl ConvertibleToQueryAst for MatchPhraseQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let full_text_params = FullTextParams {
            tokenizer: self.params.analyzer,
            mode: FullTextMode::Phrase {
                slop: self.params.slop,
            },
            zero_terms_query: self.params.zero_terms_query,
        };
        Ok(QueryAst::FullText(FullTextQuery {
            field: self.field,
            text: self.params.query,
            params: full_text_params,
            lenient: false,
        }))
    }
}

impl From<MatchPhraseQuery> for ElasticQueryDslInner {
    fn from(match_phrase_query: MatchPhraseQuery) -> Self {
        ElasticQueryDslInner::MatchPhrase(match_phrase_query)
    }
}

impl From<OneFieldMap<StringOrStructForSerialization<MatchPhraseQueryParams>>>
    for MatchPhraseQuery
{
    fn from(
        match_query_params: OneFieldMap<StringOrStructForSerialization<MatchPhraseQueryParams>>,
    ) -> Self {
        let OneFieldMap { field, value } = match_query_params;
        MatchPhraseQuery {
            field,
            params: value.inner,
        }
    }
}

impl From<String> for MatchPhraseQueryParams {
    fn from(query: String) -> MatchPhraseQueryParams {
        MatchPhraseQueryParams {
            query,
            zero_terms_query: Default::default(),
            analyzer: None,
            slop: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_match_query_string() {
        // We accept a single string
        let match_query: MatchPhraseQuery =
            serde_json::from_str(r#"{"my_field": "my_query"}"#).unwrap();
        assert_eq!(match_query.field, "my_field");
        assert_eq!(&match_query.params.query, "my_query");
        assert_eq!(match_query.params.slop, 0u32);
        assert!(match_query.params.analyzer.is_none());
        assert_eq!(
            match_query.params.zero_terms_query,
            MatchAllOrNone::MatchNone
        );
    }

    #[test]
    fn test_deserialize_match_query_struct() {
        // We accept a struct too.
        let match_query: MatchPhraseQuery = serde_json::from_str(
            r#"
            {"my_field":
                {
                    "query": "my_query",
                    "slop": 1
                }
            }
        "#,
        )
        .unwrap();
        assert_eq!(match_query.field, "my_field");
        assert_eq!(&match_query.params.query, "my_query");
        assert_eq!(match_query.params.slop, 1u32);
    }

    #[test]
    fn test_deserialize_match_query_nice_errors() {
        let deser_error = serde_json::from_str::<MatchPhraseQuery>(
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
        let match_query = MatchPhraseQuery {
            field: "body".to_string(),
            params: MatchPhraseQueryParams {
                analyzer: Some("whitespace".to_string()),
                query: "hello".to_string(),
                slop: 2u32,
                zero_terms_query: crate::MatchAllOrNone::MatchAll,
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
        assert_eq!(params.mode, FullTextMode::Phrase { slop: 2u32 });
        assert_eq!(params.zero_terms_query, MatchAllOrNone::MatchAll);
    }
}
