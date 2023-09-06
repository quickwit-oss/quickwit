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

use serde::Deserialize;

use crate::elastic_query_dsl::match_query::{MatchQueryParams, MatchQueryParamsForDeserialization};
use crate::elastic_query_dsl::{default_max_expansions, ConvertableToQueryAst};
use crate::query_ast::{FullTextParams, FullTextQuery, QueryAst};
use crate::OneFieldMap;

/// `MatchBoolPrefixQuery` as defined in
/// <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-bool-prefix-query.html>
#[derive(Deserialize, Clone, Eq, PartialEq, Debug)]
#[serde(
    from = "OneFieldMap<MatchQueryParamsForDeserialization>",
    into = "OneFieldMap<MatchQueryParams>"
)]
pub(crate) struct MatchBoolPrefixQuery {
    pub(crate) field: String,
    pub(crate) params: MatchQueryParams,
}

impl ConvertableToQueryAst for MatchBoolPrefixQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let full_text_params = FullTextParams {
            tokenizer: None,
            mode: crate::query_ast::FullTextMode::BoolPrefix {
                operator: self.params.operator,
                max_expansions: default_max_expansions(),
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

impl From<OneFieldMap<MatchQueryParamsForDeserialization>> for MatchBoolPrefixQuery {
    fn from(match_query_params: OneFieldMap<MatchQueryParamsForDeserialization>) -> Self {
        let OneFieldMap { field, value } = match_query_params;
        MatchBoolPrefixQuery {
            field,
            params: value.inner,
        }
    }
}
