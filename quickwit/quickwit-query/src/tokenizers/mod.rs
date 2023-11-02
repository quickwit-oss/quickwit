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
pub use self::tokenizer_manager::TokenizerManager;

pub const DEFAULT_REMOVE_TOKEN_LENGTH: usize = 255;

/// Quickwit's tokenizer/analyzer manager.
pub fn create_default_quickwit_tokenizer_manager() -> TokenizerManager {
    let tokenizer_manager = TokenizerManager::new();

    let raw_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
        .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
        .build();
    tokenizer_manager.register("raw", raw_tokenizer, false);

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
    tokenizer_manager.register("chinese_compatible", chinese_tokenizer, false);
    tokenizer_manager.register(
        "source_code_default",
        TextAnalyzer::builder(CodeTokenizer::default())
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
}
