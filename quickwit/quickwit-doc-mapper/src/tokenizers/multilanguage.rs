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

use lindera_tantivy::dictionary::load_dictionary;
use lindera_tantivy::stream::LinderaTokenStream;
use lindera_tantivy::tokenizer::LinderaTokenizer;
use lindera_tantivy::{DictionaryConfig, DictionaryKind, Mode};
use nom::InputTake;
use tantivy::tokenizer::{SimpleTokenStream, SimpleTokenizer, Token, TokenStream, Tokenizer};
use whichlang::{detect_language, Lang};

/// Multilanguage tokenizer that uses the `whichlang` to detect the language of the text
/// and uses the appropriate tokenizer for the detected language:
/// - lindera for Chinese, Japanese, and Korean.
/// - Quickwit's default tokenizer for other languages.
/// It is possible to bypass the language detection by prefixing the text with the language code
/// followed by a colon. For example, `KOR:일본입니다` will be tokenized by the english tokenizer.
/// Current supported prefix are:
/// - `KOR:` for Korean tokenizer
/// - `JPN:` for Japanese tokenizer
/// - `CMN:` for Chinese tokenizer
/// - `ENG:` for Quickwit's default tokenizer
#[derive(Clone)]
pub(crate) struct MultiLanguageTokenizer {
    cmn_tokenizer: LinderaTokenizer,
    jpn_tokenizer: LinderaTokenizer,
    kor_tokenizer: LinderaTokenizer,
    default_tokenizer: SimpleTokenizer,
}

impl MultiLanguageTokenizer {
    pub fn new() -> Self {
        // Chinese tokenizer
        let cmn_dictionary_config = DictionaryConfig {
            kind: Some(DictionaryKind::CcCedict),
            path: None,
        };
        let cmn_dictionary = load_dictionary(cmn_dictionary_config)
            .expect("Lindera `CcCedict` dictionary must be present");
        let cmn_tokenizer = LinderaTokenizer::new(cmn_dictionary, None, Mode::Normal);

        // Japanese tokenizer
        let jpn_dictionary_config = DictionaryConfig {
            kind: Some(DictionaryKind::IPADIC),
            path: None,
        };
        let jpn_dictionary = load_dictionary(jpn_dictionary_config)
            .expect("Lindera `IPAD` dictionary must be present");
        let jpn_tokenizer = LinderaTokenizer::new(jpn_dictionary, None, Mode::Normal);

        // Korean tokenizer
        let kor_dictionary_config = DictionaryConfig {
            kind: Some(DictionaryKind::KoDic),
            path: None,
        };
        let kor_dictionary = load_dictionary(kor_dictionary_config)
            .expect("Lindera `KoDic` dictionary must be present");
        let kor_tokenizer = LinderaTokenizer::new(kor_dictionary, None, Mode::Normal);

        Self {
            cmn_tokenizer,
            jpn_tokenizer,
            kor_tokenizer,
            default_tokenizer: SimpleTokenizer,
        }
    }
}

pub(crate) enum MultiLanguageTokenStream<'a> {
    Empty,
    Lindera(LinderaTokenStream),
    Simple(SimpleTokenStream<'a>),
}

impl<'a> TokenStream for MultiLanguageTokenStream<'a> {
    fn advance(&mut self) -> bool {
        match self {
            MultiLanguageTokenStream::Empty => false,
            MultiLanguageTokenStream::Lindera(tokenizer) => tokenizer.advance(),
            MultiLanguageTokenStream::Simple(tokenizer) => tokenizer.advance(),
        }
    }

    fn token(&self) -> &Token {
        match self {
            MultiLanguageTokenStream::Empty => {
                panic!("Cannot call token() on an empty token stream.")
            }
            MultiLanguageTokenStream::Lindera(tokenizer) => tokenizer.token(),
            MultiLanguageTokenStream::Simple(tokenizer) => tokenizer.token(),
        }
    }

    fn token_mut(&mut self) -> &mut Token {
        match self {
            MultiLanguageTokenStream::Empty => {
                panic!("Cannot call token_mut() on an empty token stream.")
            }
            MultiLanguageTokenStream::Lindera(tokenizer) => tokenizer.token_mut(),
            MultiLanguageTokenStream::Simple(tokenizer) => tokenizer.token_mut(),
        }
    }
}

/// If language prefix is present, returns the corresponding language and the text without the
/// prefix. If no prefix is present, returns (None, text).
/// The language prefix is defined as `{ID}:text` with ID being the 3-letter language code similar
/// to whichlang language code.
fn process_language_prefix(text: &str) -> (Option<Lang>, &str) {
    let prefix_bytes = text.as_bytes().take(std::cmp::min(4, text.len()));
    let prefix_language = match prefix_bytes {
        b"CMN:" => Some(Lang::Cmn),
        b"ENG:" => Some(Lang::Eng),
        b"JPN:" => Some(Lang::Jpn),
        b"KOR:" => Some(Lang::Kor),
        _ => None,
    };
    let text_without_prefix = if prefix_language.is_some() {
        // This is safe as we know that the prefix is made of 4 ascii characters.
        &text[4..]
    } else {
        text
    };
    (prefix_language, text_without_prefix)
}

impl Tokenizer for MultiLanguageTokenizer {
    type TokenStream<'a> = MultiLanguageTokenStream<'a>;
    fn token_stream<'a>(&self, text: &'a str) -> MultiLanguageTokenStream<'a> {
        let (language_prefix, text_to_tokenize) = process_language_prefix(text);
        // If the text is empty, we return an empty token stream.
        // `detect_language` panicks if the text is empty.
        if text.trim().is_empty() {
            return MultiLanguageTokenStream::Empty;
        }
        let language = language_prefix.unwrap_or_else(|| detect_language(text_to_tokenize));
        match language {
            Lang::Cmn => {
                MultiLanguageTokenStream::Lindera(self.cmn_tokenizer.token_stream(text_to_tokenize))
            }
            Lang::Jpn => {
                MultiLanguageTokenStream::Lindera(self.jpn_tokenizer.token_stream(text_to_tokenize))
            }
            Lang::Kor => {
                MultiLanguageTokenStream::Lindera(self.kor_tokenizer.token_stream(text_to_tokenize))
            }
            _ => MultiLanguageTokenStream::Simple(
                self.default_tokenizer.token_stream(text_to_tokenize),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

    use super::{process_language_prefix, MultiLanguageTokenStream, MultiLanguageTokenizer};

    fn test_helper(mut tokenizer: MultiLanguageTokenStream) -> Vec<Token> {
        let mut tokens: Vec<Token> = vec![];
        tokenizer.process(&mut |token: &Token| tokens.push(token.clone()));
        tokens
    }

    #[test]
    fn test_multilanguage_tokenizer_cmn() {
        let tokenizer = MultiLanguageTokenizer::new();
        let tokens = test_helper(
            tokenizer.token_stream("地址1，包含無效的字元 (包括符號與不標準的asci阿爾發字元"),
        );
        assert_eq!(tokens.len(), 19);
        {
            let token = &tokens[0];
            assert_eq!(token.text, "地址");
            assert_eq!(token.offset_from, 0);
            assert_eq!(token.offset_to, 6);
            assert_eq!(token.position, 0);
            assert_eq!(token.position_length, 1);
        }
    }

    #[test]
    fn test_multilanguage_tokenizer_jpn() {
        let tokenizer = MultiLanguageTokenizer::new();
        {
            let tokens = test_helper(tokenizer.token_stream("すもももももももものうち"));
            assert_eq!(tokens.len(), 7);
            {
                let token = &tokens[0];
                assert_eq!(token.text, "すもも");
                assert_eq!(token.offset_from, 0);
                assert_eq!(token.offset_to, 9);
                assert_eq!(token.position, 0);
                assert_eq!(token.position_length, 1);
            }
        }
        {
            // Force usage of JPN tokenizer.
            let tokens = test_helper(tokenizer.token_stream("JPN:すもももももももものうち"));
            assert_eq!(tokens.len(), 7);
        }
        {
            // Force usage of ENG tokenizer.
            // This tokenizer will return only one token.
            let tokens = test_helper(tokenizer.token_stream("ENG:すもももももももものうち"));
            assert_eq!(tokens.len(), 1);
        }
    }

    #[test]
    fn test_multilanguage_tokenizer_kor() {
        let tokenizer = MultiLanguageTokenizer::new();
        {
            let tokens = test_helper(tokenizer.token_stream("일본입니다. 매우 멋진 단어입니다."));
            assert_eq!(tokens.len(), 11);
            {
                let token = &tokens[0];
                assert_eq!(token.text, "일본");
                assert_eq!(token.offset_from, 0);
                assert_eq!(token.offset_to, 6);
                assert_eq!(token.position, 0);
                assert_eq!(token.position_length, 1);
            }
        }
        {
            let tokens =
                test_helper(tokenizer.token_stream("KOR:일본입니다. 매우 멋진 단어입니다."));
            assert_eq!(tokens.len(), 11);
        }
        {
            let tokens = test_helper(tokenizer.token_stream("ENG:일본입니다"));
            assert_eq!(tokens.len(), 1);
        }
    }

    #[test]
    fn test_multilanguage_tokenizer_with_empty_string() {
        let tokenizer = MultiLanguageTokenizer::new();
        {
            let tokens = test_helper(tokenizer.token_stream(""));
            assert_eq!(tokens.len(), 0);
        }
        {
            let tokens = test_helper(tokenizer.token_stream("   "));
            assert_eq!(tokens.len(), 0);
        }
    }

    #[test]
    fn test_multilanguage_process_language_prefix() {
        {
            let (lang, text) = process_language_prefix("JPN:すもももももももものうち");
            assert_eq!(lang, Some(whichlang::Lang::Jpn));
            assert_eq!(text, "すもももももももものうち");
        }
        {
            let (lang, text) = process_language_prefix("CMN:地址1，包含無效的字元");
            assert_eq!(lang, Some(whichlang::Lang::Cmn));
            assert_eq!(text, "地址1，包含無效的字元");
        }
        {
            let (lang, text) = process_language_prefix("ENG:my address");
            assert_eq!(lang, Some(whichlang::Lang::Eng));
            assert_eq!(text, "my address");
        }
        {
            let (lang, text) = process_language_prefix("UNK:my address");
            assert!(lang.is_none());
            assert_eq!(text, "UNK:my address");
        }
    }
}
