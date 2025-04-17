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

use super::{ElasticQueryDslInner, StringOrStructForSerialization};
use crate::OneFieldMap;
use crate::elastic_query_dsl::match_query::MatchQueryParams;
use crate::elastic_query_dsl::{ConvertibleToQueryAst, default_max_expansions};
use crate::query_ast::{FullTextParams, FullTextQuery, QueryAst};

/// `MatchBoolPrefixQuery` as defined in
/// <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-bool-prefix-query.html>
#[derive(Deserialize, Clone, Eq, PartialEq, Debug)]
#[serde(from = "OneFieldMap<StringOrStructForSerialization<MatchQueryParams>>")]
pub(crate) struct MatchBoolPrefixQuery {
    pub(crate) field: String,
    pub(crate) params: MatchQueryParams,
}

impl ConvertibleToQueryAst for MatchBoolPrefixQuery {
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
            lenient: self.params.lenient,
        }))
    }
}

impl From<MatchBoolPrefixQuery> for ElasticQueryDslInner {
    fn from(match_bool_prefix_query: MatchBoolPrefixQuery) -> Self {
        ElasticQueryDslInner::MatchBoolPrefix(match_bool_prefix_query)
    }
}

impl From<OneFieldMap<StringOrStructForSerialization<MatchQueryParams>>> for MatchBoolPrefixQuery {
    fn from(
        match_query_params: OneFieldMap<StringOrStructForSerialization<MatchQueryParams>>,
    ) -> Self {
        let OneFieldMap { field, value } = match_query_params;
        MatchBoolPrefixQuery {
            field,
            params: value.inner,
        }
    }
}
