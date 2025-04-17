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

use lindera_core::mode::Mode;
use lindera_dictionary::{DictionaryConfig, DictionaryKind, load_dictionary_from_config};
use lindera_tokenizer::token::Token as LinderaToken;
use lindera_tokenizer::tokenizer::Tokenizer as LinderaTokenizer;
use once_cell::sync::Lazy;
use tantivy::tokenizer::{SimpleTokenStream, SimpleTokenizer, Token, TokenStream, Tokenizer};
use whichlang::{Lang, detect_language};

// Note(fmassot): we use `lindera_tokenizer::tokenizer::Tokenizer` and not
// `use lindera_tantivy::tokenizer::LinderaTokenizer` to avoid
// costly copy of lindera dictionaries each time we clone the `MultiLangTokenizer`.

/// Mandarin chinese tokenizer.
static CMN_TOKENIZER: Lazy<LinderaTokenizer> = Lazy::new(|| {
    let cmn_dictionary_config = DictionaryConfig {
        kind: Some(DictionaryKind::CcCedict),
        path: None,
    };
    let cmn_dictionary = load_dictionary_from_config(cmn_dictionary_config)
        .expect("Lindera `CcCedict` dictionary must be present");
    LinderaTokenizer::new(cmn_dictionary, None, Mode::Normal)
});

/// Japanese tokenizer.
static JPN_TOKENIZER: Lazy<LinderaTokenizer> = Lazy::new(|| {
    let jpn_dictionary_config = DictionaryConfig {
        kind: Some(DictionaryKind::IPADIC),
        path: None,
    };
    let jpn_dictionary = load_dictionary_from_config(jpn_dictionary_config)
        .expect("Lindera `IPAD` dictionary must be present");
    LinderaTokenizer::new(jpn_dictionary, None, Mode::Normal)
});

/// Korean tokenizer.
static KOR_TOKENIZER: Lazy<LinderaTokenizer> = Lazy::new(|| {
    let kor_dictionary_config = DictionaryConfig {
        kind: Some(DictionaryKind::KoDic),
        path: None,
    };
    let kor_dictionary = load_dictionary_from_config(kor_dictionary_config)
        .expect("Lindera `KoDic` dictionary must be present");
    LinderaTokenizer::new(kor_dictionary, None, Mode::Normal)
});

/// Multilanguage tokenizer that uses the `whichlang` to detect the language of the text
/// and uses the appropriate tokenizer for the detected language:
/// - lindera for Chinese, Japanese, and Korean.
/// - Quickwit's default tokenizer for other languages.
///
/// It is possible to bypass the language detection by prefixing the text with the language code
/// followed by a colon. For example, `KOR:일본입니다` will be tokenized by the korean tokenizer.
/// Current supported prefix are:
/// - `KOR:` for Korean tokenizer
/// - `JPN:` for Japanese tokenizer
/// - `CMN:` for Chinese tokenizer
/// - `ENG:` for Quickwit's default tokenizer
#[derive(Clone, Default)]
pub struct MultiLangTokenizer {
    default_tokenizer: SimpleTokenizer,
    token: Token,
}

impl Tokenizer for MultiLangTokenizer {
    type TokenStream<'a> = MultiLanguageTokenStream<'a>;
    fn token_stream<'a>(&'a mut self, text: &'a str) -> MultiLanguageTokenStream<'a> {
        self.token.reset();
        let (language_prefix, text_to_tokenize) = get_language_from_prefix(text);
        // If the text is empty, we return an empty token stream.
        // `whichlang::detect_language` panicks if the text is empty.
        if text.trim().is_empty() {
            return MultiLanguageTokenStream::Empty;
        }
        let language = language_prefix.unwrap_or_else(|| detect_language(text_to_tokenize));
        match language {
            Lang::Cmn => {
                let lindera_token_stream = LinderaTokenStream {
                    tokens: CMN_TOKENIZER
                        .tokenize(text_to_tokenize)
                        .expect("tokenize method should never fail"),
                    token: &mut self.token,
                };
                MultiLanguageTokenStream::Lindera(lindera_token_stream)
            }
            Lang::Jpn => {
                let lindera_token_stream = LinderaTokenStream {
                    tokens: JPN_TOKENIZER
                        .tokenize(text_to_tokenize)
                        .expect("tokenize method should never fail"),
                    token: &mut self.token,
                };
                MultiLanguageTokenStream::Lindera(lindera_token_stream)
            }
            Lang::Kor => {
                let lindera_token_stream = LinderaTokenStream {
                    tokens: KOR_TOKENIZER
                        .tokenize(text_to_tokenize)
                        .expect("tokenize method should never fail"),
                    token: &mut self.token,
                };
                MultiLanguageTokenStream::Lindera(lindera_token_stream)
            }
            _ => MultiLanguageTokenStream::Simple(
                self.default_tokenizer.token_stream(text_to_tokenize),
            ),
        }
    }
}

/// Gets the language defined by a prefix `{ID}:text` where ID being the 3-letter language used by
/// whichlang) and returns the language and the text without the prefix. If the prefix is not
/// recognized, the language is `None` and the text is the original.
fn get_language_from_prefix(text: &str) -> (Option<Lang>, &str) {
    let prefix_bytes = &text.as_bytes()[0..std::cmp::min(4, text.len())];
    // TODO: refactor.
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
pub enum MultiLanguageTokenStream<'a> {
    Empty,
    Lindera(LinderaTokenStream<'a>),
    Simple(SimpleTokenStream<'a>),
}

impl TokenStream for MultiLanguageTokenStream<'_> {
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

pub struct LinderaTokenStream<'a> {
    pub tokens: Vec<LinderaToken<'a>>,
    pub token: &'a mut Token,
}

impl TokenStream for LinderaTokenStream<'_> {
    fn advance(&mut self) -> bool {
        if self.tokens.is_empty() {
            return false;
        }
        let token = self.tokens.remove(0);
        self.token.text = token.text.to_string();
        self.token.offset_from = token.byte_start;
        self.token.offset_to = token.byte_end;
        self.token.position = token.position;
        self.token.position_length = token.position_length;

        true
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

    use super::{MultiLangTokenizer, MultiLanguageTokenStream, get_language_from_prefix};

    fn test_helper(mut tokenizer: MultiLanguageTokenStream) -> Vec<Token> {
        let mut tokens: Vec<Token> = Vec::new();
        tokenizer.process(&mut |token: &Token| tokens.push(token.clone()));
        tokens
    }

    #[test]
    fn test_multilanguage_tokenizer_cmn() {
        let mut tokenizer = MultiLangTokenizer::default();
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
        let mut tokenizer = MultiLangTokenizer::default();
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
        let mut tokenizer = MultiLangTokenizer::default();
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
        let mut tokenizer = MultiLangTokenizer::default();
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
            let (lang, text) = get_language_from_prefix("JPN:すもももももももものうち");
            assert_eq!(lang, Some(whichlang::Lang::Jpn));
            assert_eq!(text, "すもももももももものうち");
        }
        {
            let (lang, text) = get_language_from_prefix("CMN:地址1，包含無效的字元");
            assert_eq!(lang, Some(whichlang::Lang::Cmn));
            assert_eq!(text, "地址1，包含無效的字元");
        }
        {
            let (lang, text) = get_language_from_prefix("ENG:my address");
            assert_eq!(lang, Some(whichlang::Lang::Eng));
            assert_eq!(text, "my address");
        }
        {
            let (lang, text) = get_language_from_prefix("UNK:my address");
            assert!(lang.is_none());
            assert_eq!(text, "UNK:my address");
        }
        {
            let (lang, text) = get_language_from_prefix("");
            assert!(lang.is_none());
            assert_eq!(text, "");
        }
    }
}
