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
mod multilanguage;

use once_cell::sync::Lazy;
use tantivy::tokenizer::{
    LowerCaser, RawTokenizer, RemoveLongFilter, TextAnalyzer, TokenizerManager,
};

use self::chinese_compatible::ChineseTokenizer;
use self::multilanguage::MultiLanguageTokenizer;

/// Quickwit's tokenizer/analyzer manager.
pub static QUICKWIT_TOKENIZER_MANAGER: Lazy<TokenizerManager> =
    Lazy::new(get_quickwit_tokenizer_manager);

fn get_quickwit_tokenizer_manager() -> TokenizerManager {
    let raw_tokenizer = TextAnalyzer::builder(RawTokenizer)
        .filter(RemoveLongFilter::limit(100))
        .build();

    let chinese_tokenizer = TextAnalyzer::builder(ChineseTokenizer)
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .build();

    let multilanguage_tokenizer = TextAnalyzer::builder(MultiLanguageTokenizer::new())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .build();

    let tokenizer_manager = TokenizerManager::default();

    tokenizer_manager.register("raw", raw_tokenizer);
    tokenizer_manager.register("chinese_compatible", chinese_tokenizer);
    tokenizer_manager.register("multilanguage", multilanguage_tokenizer);

    tokenizer_manager
}

#[cfg(test)]
mod tests {
    use super::get_quickwit_tokenizer_manager;

    #[test]
    fn test_tokenizers_in_manager() {
        get_quickwit_tokenizer_manager()
            .get("chinese_compatible")
            .unwrap();
        get_quickwit_tokenizer_manager().get("default").unwrap();
        get_quickwit_tokenizer_manager()
            .get("multilanguage")
            .unwrap();
        get_quickwit_tokenizer_manager().get("raw").unwrap();
    }

    #[test]
    fn test_raw_tokenizer() {
        let my_haiku = r#"
        white sandy beach
        a strong wind is coming
        sand in my face
        "#;
        let my_long_text = "a text, that is just too long, no one will type it, no one will like \
                            it, no one shall find it. I just need some more chars, now you may \
                            not pass.";

        let tokenizer = get_quickwit_tokenizer_manager().get("raw").unwrap();
        let mut haiku_stream = tokenizer.token_stream(my_haiku);
        assert!(haiku_stream.advance());
        assert!(!haiku_stream.advance());
        assert!(!tokenizer.token_stream(my_long_text).advance());
    }
}
