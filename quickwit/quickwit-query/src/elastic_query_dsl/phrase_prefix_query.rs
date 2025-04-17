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

use crate::MatchAllOrNone;
use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::elastic_query_dsl::{
    ConvertibleToQueryAst, ElasticQueryDslInner, default_max_expansions,
};
use crate::query_ast::{self, FullTextMode, FullTextParams, QueryAst};

pub(crate) type MatchPhrasePrefixQuery = OneFieldMap<MatchPhrasePrefixQueryParams>;

#[derive(PartialEq, Eq, Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct MatchPhrasePrefixQueryParams {
    pub query: String,
    #[serde(default)]
    pub analyzer: Option<String>,
    #[serde(default = "default_max_expansions")]
    pub max_expansions: u32,
    #[serde(default)]
    pub slop: u32,
    #[serde(default, skip_serializing_if = "MatchAllOrNone::is_none")]
    pub zero_terms_query: MatchAllOrNone,
}

impl From<MatchPhrasePrefixQuery> for ElasticQueryDslInner {
    fn from(term_query: MatchPhrasePrefixQuery) -> Self {
        Self::MatchPhrasePrefix(term_query)
    }
}

impl ConvertibleToQueryAst for MatchPhrasePrefixQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let MatchPhrasePrefixQueryParams {
            query,
            analyzer,
            max_expansions,
            slop,
            zero_terms_query,
        } = self.value;
        let analyzer = FullTextParams {
            tokenizer: analyzer,
            mode: FullTextMode::Phrase { slop },
            zero_terms_query,
        };
        let phrase_prefix_query_ast = query_ast::PhrasePrefixQuery {
            field: self.field,
            phrase: query,
            params: analyzer,
            max_expansions,
            lenient: false,
        };
        Ok(phrase_prefix_query_ast.into())
    }
}

#[cfg(test)]
mod tests {
    use super::{MatchAllOrNone, MatchPhrasePrefixQuery, MatchPhrasePrefixQueryParams};

    #[test]
    fn test_term_query_simple() {
        let phrase_prefix_json = r#"{ "message": { "query": "quick brown f" } }"#;
        let phrase_prefix: MatchPhrasePrefixQuery =
            serde_json::from_str(phrase_prefix_json).unwrap();
        let expected = MatchPhrasePrefixQuery {
            field: "message".to_string(),
            value: MatchPhrasePrefixQueryParams {
                query: "quick brown f".to_string(),
                analyzer: None,
                max_expansions: 50,
                slop: 0,
                zero_terms_query: MatchAllOrNone::MatchNone,
            },
        };

        assert_eq!(&phrase_prefix, &expected);
    }
}
