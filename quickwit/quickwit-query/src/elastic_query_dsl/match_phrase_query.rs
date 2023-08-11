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

use std::fmt;

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::elastic_query_dsl::ConvertableToQueryAst;
use crate::query_ast::{FullTextMode, FullTextParams, FullTextQuery, QueryAst};
use crate::{MatchAllOrNone, OneFieldMap};

/// `MatchQuery` as defined in
/// <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html>
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
#[serde(
    from = "OneFieldMap<MatchPhraseQueryParamsForDeserialization>",
    into = "OneFieldMap<MatchPhraseQueryParams>"
)]
pub(crate) struct MatchPhraseQuery {
    pub(crate) field: String,
    pub(crate) params: MatchPhraseQueryParams,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
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

// --------------
//
// Below is the Serialization/Deserialization code
// The difficulty here is to support the two following formats:
//
// `{"field": {"query": "my query", "default_operator": "OR"}}`
// `{"field": "my query"}`
//
// We don't use untagged enum to support this, in order to keep good errors.
//
// The code below is adapted from solution described here: https://serde.rs/string-or-struct.html

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct MatchPhraseQueryParamsForDeserialization {
    #[serde(deserialize_with = "string_or_struct")]
    inner: MatchPhraseQueryParams,
}

impl From<MatchPhraseQuery> for OneFieldMap<MatchPhraseQueryParams> {
    fn from(match_phrase_query: MatchPhraseQuery) -> OneFieldMap<MatchPhraseQueryParams> {
        OneFieldMap {
            field: match_phrase_query.field,
            value: match_phrase_query.params,
        }
    }
}

impl From<OneFieldMap<MatchPhraseQueryParamsForDeserialization>> for MatchPhraseQuery {
    fn from(match_query_params: OneFieldMap<MatchPhraseQueryParamsForDeserialization>) -> Self {
        let OneFieldMap { field, value } = match_query_params;
        MatchPhraseQuery {
            field,
            params: value.inner,
        }
    }
}

struct MatchQueryParamsStringOrStructVisitor;

impl<'de> Visitor<'de> for MatchQueryParamsStringOrStructVisitor {
    type Value = MatchPhraseQueryParams;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("string or map containing the parameters of a match query.")
    }

    fn visit_str<E>(self, query: &str) -> Result<Self::Value, E>
    where E: serde::de::Error {
        Ok(MatchPhraseQueryParams {
            query: query.to_string(),
            zero_terms_query: Default::default(),
            analyzer: None,
            slop: 0,
        })
    }

    fn visit_map<M>(self, map: M) -> Result<MatchPhraseQueryParams, M::Error>
    where M: MapAccess<'de> {
        Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
    }
}

fn string_or_struct<'de, D>(deserializer: D) -> Result<MatchPhraseQueryParams, D::Error>
where D: Deserializer<'de> {
    deserializer.deserialize_any(MatchQueryParamsStringOrStructVisitor)
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
