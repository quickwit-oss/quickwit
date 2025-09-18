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

mod chinese_compatible;
mod code_tokenizer;
#[cfg(feature = "multilang")]
mod multilang;
mod tokenizer_manager;

use once_cell::sync::Lazy;
use tantivy::tokenizer::{
    AsciiFoldingFilter, Language, LowerCaser, RawTokenizer, RemoveLongFilter, SimpleTokenizer,
    Stemmer, TextAnalyzer, WhitespaceTokenizer,
};

use self::chinese_compatible::ChineseTokenizer;
pub use self::code_tokenizer::CodeTokenizer;
#[cfg(feature = "multilang")]
pub use self::multilang::MultiLangTokenizer;
pub use self::tokenizer_manager::{RAW_TOKENIZER_NAME, TokenizerManager};

pub const DEFAULT_REMOVE_TOKEN_LENGTH: usize = 255;

/// Quickwit's tokenizer/analyzer manager.
pub fn create_default_quickwit_tokenizer_manager() -> TokenizerManager {
    let tokenizer_manager = TokenizerManager::new();

    let raw_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
        .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
        .build();
    tokenizer_manager.register("raw", raw_tokenizer, false);

    let raw_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
        .filter(LowerCaser)
        .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
        .build();
    tokenizer_manager.register("raw_lowercase", raw_tokenizer, true);

    let lower_case_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
        .filter(LowerCaser)
        .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
        .build();
    tokenizer_manager.register("lowercase", lower_case_tokenizer, true);

    let default_tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
        .filter(LowerCaser)
        .build();
    tokenizer_manager.register("default", default_tokenizer, true);

    let en_stem_tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
        .filter(LowerCaser)
        .filter(Stemmer::new(Language::English))
        .build();
    tokenizer_manager.register("en_stem", en_stem_tokenizer, true);

    tokenizer_manager.register("whitespace", WhitespaceTokenizer::default(), false);

    let chinese_tokenizer = TextAnalyzer::builder(ChineseTokenizer)
        .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
        .filter(LowerCaser)
        .build();
    tokenizer_manager.register("chinese_compatible", chinese_tokenizer, true);
    tokenizer_manager.register(
        "source_code_default",
        TextAnalyzer::builder(CodeTokenizer::default())
            .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
            .filter(LowerCaser)
            .filter(AsciiFoldingFilter)
            .build(),
        true,
    );
    tokenizer_manager.register(
        "source_code_with_hex",
        TextAnalyzer::builder(CodeTokenizer::with_hex_support())
            .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
            .filter(LowerCaser)
            .filter(AsciiFoldingFilter)
            .build(),
        true,
    );
    #[cfg(feature = "multilang")]
    tokenizer_manager.register(
        "multilang_default",
        TextAnalyzer::builder(MultiLangTokenizer::default())
            .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
            .filter(LowerCaser)
            .build(),
        true,
    );
    tokenizer_manager
}

fn create_quickwit_fastfield_normalizer_manager() -> TokenizerManager {
    let raw_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
        .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
        .build();
    let lower_case_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
        .filter(LowerCaser)
        .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
        .build();
    let tokenizer_manager = TokenizerManager::new();
    tokenizer_manager.register("raw", raw_tokenizer, false);
    tokenizer_manager.register("lowercase", lower_case_tokenizer, true);
    tokenizer_manager
}

pub fn get_quickwit_fastfield_normalizer_manager() -> &'static TokenizerManager {
    static QUICKWIT_FAST_FIELD_NORMALIZER_MANAGER: Lazy<TokenizerManager> =
        Lazy::new(create_quickwit_fastfield_normalizer_manager);
    &QUICKWIT_FAST_FIELD_NORMALIZER_MANAGER
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_tokenizers_in_manager() {
        let tokenizer_manager = super::create_default_quickwit_tokenizer_manager();
        tokenizer_manager
            .get_tokenizer("chinese_compatible")
            .unwrap();
        tokenizer_manager.get_tokenizer("default").unwrap();
        tokenizer_manager.get_tokenizer("raw").unwrap();
    }

    #[test]
    fn test_raw_tokenizer() {
        let tokenizer_manager = super::create_default_quickwit_tokenizer_manager();
        let my_haiku = r#"
        white sandy beach
        a strong wind is coming
        sand in my face
        "#;
        let my_long_text = "a text, that is just too long, no one will type it, no one will like \
                            it, no one shall find it. I just need some more chars, now you may \
                            not pass.";

        let mut tokenizer = tokenizer_manager.get_tokenizer("raw").unwrap();
        let mut haiku_stream = tokenizer.token_stream(my_haiku);
        assert!(haiku_stream.advance());
        assert!(!haiku_stream.advance());
        let mut other_tokenizer = tokenizer_manager.get_tokenizer("raw").unwrap();
        let mut other_stream = other_tokenizer.token_stream(my_long_text);
        assert!(other_stream.advance());
        assert!(!other_stream.advance());
    }

    #[test]
    fn test_code_tokenizer_in_tokenizer_manager() {
        let mut code_tokenizer = super::create_default_quickwit_tokenizer_manager()
            .get_tokenizer("source_code_default")
            .unwrap();
        let mut token_stream = code_tokenizer.token_stream("PigCaféFactory2");
        let mut tokens = Vec::new();
        while let Some(token) = token_stream.next() {
            tokens.push(token.text.to_string());
        }
        assert_eq!(tokens, vec!["pig", "cafe", "factory", "2"])
    }

    #[test]
    fn test_raw_lowercase_tokenizer() {
        let tokenizer_manager = super::create_default_quickwit_tokenizer_manager();
        let my_long_text = "a text, that is just too long, no one will type it, no one will like \
                            it, no one shall find it. I just need some more chars, now you may \
                            not pass.";

        let mut tokenizer = tokenizer_manager.get_tokenizer("raw_lowercase").unwrap();
        let mut stream = tokenizer.token_stream(my_long_text);
        assert!(stream.advance());
        assert_eq!(stream.token().text.len(), my_long_text.len());
        // there are non letter, so we can't check for all lowercase directly
        assert!(stream.token().text.chars().all(|c| !c.is_uppercase()));
        assert!(!stream.advance());
    }
}
