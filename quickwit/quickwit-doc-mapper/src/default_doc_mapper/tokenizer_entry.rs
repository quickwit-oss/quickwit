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

use anyhow::Context;
use itertools::Itertools;
use quickwit_query::DEFAULT_REMOVE_TOKEN_LENGTH;
use serde::{Deserialize, Serialize};
use tantivy::tokenizer::{
    AsciiFoldingFilter, BoxTokenFilter, LowerCaser, NgramTokenizer, RegexTokenizer,
    RemoveLongFilter, TextAnalyzer,
};

/// A `TokenizerEntry` defines a custom tokenizer with its name and configuration.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
pub struct TokenizerEntry {
    /// Tokenizer name.
    pub name: String,
    /// Tokenizer configuration.
    #[serde(flatten)]
    pub(crate) config: TokenizerConfig,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
pub struct TokenizerConfig {
    #[serde(flatten)]
    tokenizer_type: TokenizerType,
    #[serde(default)]
    filters: Vec<TokenFilterType>,
}

impl TokenizerConfig {
    pub fn text_analyzer(&self) -> anyhow::Result<TextAnalyzer> {
        let boxed_token_filters = self.token_filters();
        let text_analyzer = match &self.tokenizer_type {
            TokenizerType::Ngram(options) => {
                let tokenizer =
                    NgramTokenizer::new(options.min_gram, options.max_gram, options.prefix_only);
                TextAnalyzer::new(tokenizer, boxed_token_filters)
            }
            TokenizerType::Regex(options) => {
                let tokenizer = RegexTokenizer::new(&options.pattern)
                    .with_context(|| "Invalid regex tokenizer".to_string())?;
                TextAnalyzer::new(tokenizer, boxed_token_filters)
            }
        };
        Ok(text_analyzer)
    }

    fn token_filters(&self) -> Vec<BoxTokenFilter> {
        self.filters
            .iter()
            .map(|token_filter| token_filter.box_token_filter())
            .collect_vec()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TokenFilterType {
    RemoveLong,
    LowerCaser,
    AsciiFolding,
}

impl TokenFilterType {
    fn box_token_filter(&self) -> BoxTokenFilter {
        match &self {
            Self::RemoveLong => {
                BoxTokenFilter::from(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
            }
            Self::LowerCaser => BoxTokenFilter::from(LowerCaser),
            Self::AsciiFolding => BoxTokenFilter::from(AsciiFoldingFilter),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TokenizerType {
    Ngram(NgramTokenizerOption),
    Regex(RegexTokenizerOption),
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NgramTokenizerOption {
    pub min_gram: usize,
    pub max_gram: usize,
    #[serde(default)]
    pub prefix_only: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RegexTokenizerOption {
    pub pattern: String,
}

#[cfg(test)]
mod tests {
    use super::{NgramTokenizerOption, TokenizerType};
    use crate::TokenizerEntry;

    #[test]
    fn test_deserialize_tokenizer_entry() {
        let result: Result<TokenizerEntry, serde_json::Error> =
            serde_json::from_str::<TokenizerEntry>(
                r#"
            {
                "name": "my_tokenizer",
                "type": "ngram",
                "filters": [
                    "remove_long",
                    "lower_caser",
                    "ascii_folding"
                ],
                "min_gram": 1,
                "max_gram": 3
            }
            "#,
            );
        assert!(result.is_ok());
        let tokenizer_config_entry = result.unwrap();
        assert_eq!(tokenizer_config_entry.config.filters.len(), 3);
        match tokenizer_config_entry.config.tokenizer_type {
            TokenizerType::Ngram(options) => {
                assert_eq!(
                    options,
                    NgramTokenizerOption {
                        min_gram: 1,
                        max_gram: 3,
                        prefix_only: false,
                    }
                )
            }
            _ => panic!("Unexpected tokenizer type"),
        }
    }

    #[test]
    fn test_deserialize_tokenizer_entry_failed_with_wrong_key() {
        let result: Result<TokenizerEntry, serde_json::Error> =
            serde_json::from_str::<TokenizerEntry>(
                r#"
            {
                "name": "my_tokenizer",
                "type": "ngram",
                "filters": [
                    "remove_long",
                    "lower_caser",
                    "ascii_folding"
                ],
                "min_gram": 1,
                "max_gram": 3,
                "abc": 123
            }
            "#,
            );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unknown field `abc`"));
    }

    #[test]
    fn test_deserialize_tokenizer_entry_regex() {
        let result: Result<TokenizerEntry, serde_json::Error> =
            serde_json::from_str::<TokenizerEntry>(
                r#"
            {
                "name": "my_tokenizer",
                "type": "regex",
                "pattern": "(my_pattern)"
            }
            "#,
            );
        assert!(result.is_ok());
    }
}
