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

use anyhow::Context;
use quickwit_query::{CodeTokenizer, DEFAULT_REMOVE_TOKEN_LENGTH};
use serde::{Deserialize, Serialize};
use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, NgramTokenizer, RegexTokenizer, RemoveLongFilter,
    SimpleTokenizer, TextAnalyzer, Token,
};

/// A `TokenizerEntry` defines a custom tokenizer with its name and configuration.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, utoipa::ToSchema)]
pub struct TokenizerEntry {
    /// Tokenizer name.
    pub name: String,
    /// Tokenizer configuration.
    #[serde(flatten)]
    pub(crate) config: TokenizerConfig,
}

/// Tokenizer configuration.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, utoipa::ToSchema)]
pub struct TokenizerConfig {
    #[serde(flatten)]
    pub(crate) tokenizer_type: TokenizerType,
    #[serde(default)]
    pub(crate) filters: Vec<TokenFilterType>,
}

impl TokenizerConfig {
    /// Build a `TextAnalyzer` from a `TokenizerConfig`.
    pub fn text_analyzer(&self) -> anyhow::Result<TextAnalyzer> {
        let mut text_analyzer_builder = match &self.tokenizer_type {
            TokenizerType::Simple => TextAnalyzer::builder(SimpleTokenizer::default()).dynamic(),
            #[cfg(any(test, feature = "multilang"))]
            TokenizerType::Multilang => {
                TextAnalyzer::builder(quickwit_query::MultiLangTokenizer::default()).dynamic()
            }
            TokenizerType::SourceCode => TextAnalyzer::builder(CodeTokenizer::default()).dynamic(),
            TokenizerType::Ngram(options) => {
                let tokenizer =
                    NgramTokenizer::new(options.min_gram, options.max_gram, options.prefix_only)
                        .with_context(|| "invalid ngram tokenizer".to_string())?;
                TextAnalyzer::builder(tokenizer).dynamic()
            }
            TokenizerType::Regex(options) => {
                let tokenizer = RegexTokenizer::new(&options.pattern)
                    .with_context(|| "invalid regex tokenizer".to_string())?;
                TextAnalyzer::builder(tokenizer).dynamic()
            }
        };
        for filter in &self.filters {
            match filter.tantivy_token_filter_enum() {
                TantivyTokenFilterEnum::RemoveLong(token_filter) => {
                    text_analyzer_builder = text_analyzer_builder.filter_dynamic(token_filter);
                }
                TantivyTokenFilterEnum::LowerCaser(token_filter) => {
                    text_analyzer_builder = text_analyzer_builder.filter_dynamic(token_filter);
                }
                TantivyTokenFilterEnum::AsciiFolding(token_filter) => {
                    text_analyzer_builder = text_analyzer_builder.filter_dynamic(token_filter);
                }
            }
        }
        Ok(text_analyzer_builder.build())
    }
}

/// Helper function to analyze a text with a given `TokenizerConfig`.
pub fn analyze_text(text: &str, tokenizer: &TokenizerConfig) -> anyhow::Result<Vec<Token>> {
    let mut text_analyzer = tokenizer.text_analyzer()?;
    let mut token_stream = text_analyzer.token_stream(text);
    let mut tokens = Vec::new();
    token_stream.process(&mut |token| {
        tokens.push(token.clone());
    });
    Ok(tokens)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TokenFilterType {
    RemoveLong,
    LowerCaser,
    AsciiFolding,
}

/// Tantivy token filter enum to build
/// a `TextAnalyzer` with dynamic token filters.
enum TantivyTokenFilterEnum {
    RemoveLong(RemoveLongFilter),
    LowerCaser(LowerCaser),
    AsciiFolding(AsciiFoldingFilter),
}

impl TokenFilterType {
    fn tantivy_token_filter_enum(&self) -> TantivyTokenFilterEnum {
        match &self {
            Self::RemoveLong => TantivyTokenFilterEnum::RemoveLong(RemoveLongFilter::limit(
                DEFAULT_REMOVE_TOKEN_LENGTH,
            )),
            Self::LowerCaser => TantivyTokenFilterEnum::LowerCaser(LowerCaser),
            Self::AsciiFolding => TantivyTokenFilterEnum::AsciiFolding(AsciiFoldingFilter),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TokenizerType {
    #[cfg(any(test, feature = "multilang"))]
    Multilang,
    Ngram(NgramTokenizerOption),
    Regex(RegexTokenizerOption),
    Simple,
    SourceCode,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NgramTokenizerOption {
    pub min_gram: usize,
    pub max_gram: usize,
    #[serde(default)]
    pub prefix_only: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RegexTokenizerOption {
    pub pattern: String,
}

#[cfg(test)]
mod tests {
    use super::{NgramTokenizerOption, TokenizerType};
    use crate::TokenizerEntry;
    use crate::doc_mapper::RegexTokenizerOption;

    #[test]
    fn test_deserialize_tokenizer_entry() {
        let result: Result<TokenizerEntry, serde_json::Error> =
            serde_json::from_str::<TokenizerEntry>(
                r#"
            {
                "name": "my_tokenizer",
                "type": "ngram",
                "min_gram": 1,
                "max_gram": 3,
                "filters": [
                    "remove_long",
                    "lower_caser",
                    "ascii_folding"
                ]
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
                "min_gram": 1,
                "max_gram": 3,
                "filters": [
                    "remove_long",
                    "lower_caser",
                    "ascii_folding"
                ],
                "abc": 123
            }
            "#,
            );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown field `abc`")
        );
    }

    #[test]
    fn test_tokenizer_entry_regex() {
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
        let tokenizer_config_entry = result.unwrap();
        assert_eq!(tokenizer_config_entry.config.filters.len(), 0);
        match tokenizer_config_entry.config.tokenizer_type {
            TokenizerType::Regex(options) => {
                assert_eq!(
                    options,
                    RegexTokenizerOption {
                        pattern: "(my_pattern)".to_string(),
                    }
                )
            }
            _ => panic!("Unexpected tokenizer type"),
        }
    }
}
