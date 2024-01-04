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

use crate::elastic_query_dsl::{ConvertableToQueryAst, StringOrStructForSerialization};
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
    query: String,
    #[serde(default)]
    zero_terms_query: MatchAllOrNone,
    #[serde(default)]
    analyzer: Option<String>,
    #[serde(default)]
    slop: u32,
}

impl ConvertableToQueryAst for MatchPhraseQuery {
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
        }))
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
        assert!(deser_error
            .to_string()
            .contains("unknown field `wrong_param`"));
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
