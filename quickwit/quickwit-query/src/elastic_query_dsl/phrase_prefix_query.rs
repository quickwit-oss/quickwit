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

use serde::{Deserialize, Serialize};

use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::elastic_query_dsl::{ConvertableToQueryAst, ElasticQueryDslInner};
use crate::query_ast::{self, FullTextMode, FullTextParams, QueryAst};
use crate::MatchAllOrNone;

pub type MatchPhrasePrefix = OneFieldMap<PhrasePrefixValue>;

fn default_max_expansions() -> u32 {
    50
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct PhrasePrefixValue {
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

impl From<MatchPhrasePrefix> for ElasticQueryDslInner {
    fn from(term_query: MatchPhrasePrefix) -> Self {
        Self::MatchPhrasePrefix(term_query)
    }
}

impl ConvertableToQueryAst for MatchPhrasePrefix {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let PhrasePrefixValue {
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
            analyzer,
            max_expansions,
        };
        Ok(phrase_prefix_query_ast.into())
    }
}

#[cfg(test)]
mod tests {
    use super::{MatchAllOrNone, MatchPhrasePrefix, PhrasePrefixValue};

    #[test]
    fn test_term_query_simple() {
        let phrase_prefix_json = r#"{ "message": { "query": "quick brown f" } }"#;
        let phrase_prefix: MatchPhrasePrefix = serde_json::from_str(phrase_prefix_json).unwrap();
        let expected = MatchPhrasePrefix {
            field: "message".to_string(),
            value: PhrasePrefixValue {
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
