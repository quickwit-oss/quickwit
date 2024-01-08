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

use crate::elastic_query_dsl::{
    ConvertableToQueryAst, ElasticQueryDslInner, StringOrStructForSerialization,
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
    // Regardless of this option Quickwit behaves in elasticsearch definition of
    // lenient. We include this property here just to accept user queries containing
    // this option.
    #[serde(default, rename = "lenient")]
    pub(crate) _lenient: bool,
}

impl ConvertableToQueryAst for MatchQuery {
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
            _lenient: false,
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
        assert!(deser_error
            .to_string()
            .contains("unknown field `wrong_param`"));
    }

    #[test]
    fn test_match_query() {
        let match_query = MatchQuery {
            field: "body".to_string(),
            params: MatchQueryParams {
                query: "hello".to_string(),
                operator: BooleanOperand::And,
                zero_terms_query: crate::MatchAllOrNone::MatchAll,
                _lenient: false,
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
        assert_eq!(
            params.mode,
            FullTextMode::Bool {
                operator: BooleanOperand::And
            }
        );
        assert_eq!(params.zero_terms_query, MatchAllOrNone::MatchAll);
    }
}
