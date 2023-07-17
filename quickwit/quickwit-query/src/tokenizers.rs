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

use std::ops::Range;
use std::str::CharIndices;

use once_cell::sync::Lazy;
use tantivy::tokenizer::{
    LowerCaser, RawTokenizer, RemoveLongFilter, TextAnalyzer, Token, TokenStream, Tokenizer,
    TokenizerManager,
};

pub const DEFAULT_REMOVE_TOKEN_LENGTH: usize = 255;

pub fn create_default_quickwit_tokenizer_manager() -> TokenizerManager {
    let raw_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
        .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
        .build();

    let chinese_tokenizer = TextAnalyzer::builder(ChineseTokenizer)
        .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
        .filter(LowerCaser)
        .build();

    let tokenizer_manager = TokenizerManager::new();
    tokenizer_manager.register("raw", raw_tokenizer);
    tokenizer_manager.register("chinese_compatible", chinese_tokenizer);

    tokenizer_manager.register(
        "default",
        TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
            .filter(LowerCaser)
            .build(),
    );
    tokenizer_manager.register(
        "en_stem",
        TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
            .filter(LowerCaser)
            .filter(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::English,
            ))
            .build(),
    );
    tokenizer_manager.register(
        "source_code",
        TextAnalyzer::builder(CodeTokenizer::default())
            .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
            .filter(LowerCaser)
            .build(),
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
    tokenizer_manager.register("raw", raw_tokenizer);
    tokenizer_manager.register("lowercase", lower_case_tokenizer);
    tokenizer_manager
}

/// TODO: add docs.
#[derive(Clone, Default)]
pub struct CodeTokenizer(Token);

impl Tokenizer for CodeTokenizer {
    type TokenStream<'a> = CodeTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.0.reset();
        CodeTokenStream {
            chars: text.char_indices(),
            state: CodeTokenStreamState::Empty,
            text,
            token: &mut self.0,
        }
    }
}

pub struct CodeTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: &'a mut Token,
    state: CodeTokenStreamState,
}

impl<'a> TokenStream for CodeTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);

        while let Some((next_char_offset, next_char)) = self.chars.next() {
            match self.state.advance(next_char_offset, next_char) {
                None => {}
                Some(token_offsets) => {
                    self.update_token(token_offsets);
                    return true;
                }
            }
        }

        // No more chars.
        match self.state.finalize() {
            None => {}
            Some(token_offsets) => {
                self.update_token(token_offsets);
                return true;
            }
        }

        false
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

impl<'a> CodeTokenStream<'a> {
    fn update_token(&mut self, token_offsets: Range<usize>) {
        self.token.offset_from = token_offsets.start;
        self.token.offset_to = token_offsets.end;
        self.token
            .text
            .push_str(&self.text[token_offsets.start..token_offsets.end]);
    }
}

enum CodeTokenStreamState {
    Empty,
    ProcessingChars(ProcessingCharsState),
}

struct ProcessingCharsState {
    is_first_char: bool,
    start_offset: usize,
    current_char: char,
    current_char_offset: usize,
    current_char_type: CharType,
}

type TokenOffsets = Range<usize>;

impl CodeTokenStreamState {
    fn reset(&mut self) {
        *self = CodeTokenStreamState::Empty;
    }

    fn advance(&mut self, next_char_offset: usize, next_char: char) -> Option<TokenOffsets> {
        let next_char_type = get_char_type(next_char);
        match self {
            Self::Empty => match next_char_type {
                CharType::Delimiter => {
                    self.reset();
                    None
                }
                _ => {
                    *self = CodeTokenStreamState::ProcessingChars(ProcessingCharsState {
                        is_first_char: true,
                        start_offset: next_char_offset,
                        current_char_offset: next_char_offset,
                        current_char: next_char,
                        current_char_type: next_char_type,
                    });
                    None
                }
            },
            Self::ProcessingChars(state) => {
                match (state.current_char_type, next_char_type) {
                    (_, CharType::Delimiter) => {
                        let offsets = TokenOffsets {
                            start: state.start_offset,
                            end: state.current_char_offset + state.current_char.len_utf8(),
                        };
                        self.reset();
                        Some(offsets)
                    }
                    // We do not emit a token if we have only `Ac` (is_first_char = true).
                    // But we emit the token `AB` if we have `ABCa`,
                    (CharType::UpperCase, CharType::LowerCase) => {
                        if state.is_first_char {
                            state.is_first_char = false;
                            state.current_char_offset = next_char_offset;
                            state.current_char = next_char;
                            state.current_char_type = next_char_type;
                            None
                        } else {
                            let offsets = TokenOffsets {
                                start: state.start_offset,
                                end: state.current_char_offset,
                            };
                            state.is_first_char = false;
                            state.start_offset = state.current_char_offset;
                            state.current_char_offset = next_char_offset;
                            state.current_char = next_char;
                            state.current_char_type = next_char_type;
                            Some(offsets)
                        }
                    }
                    // Don't emit tokens on identical char types.
                    (CharType::UpperCase, CharType::UpperCase)
                    | (CharType::LowerCase, CharType::LowerCase)
                    | (CharType::Numeric, CharType::Numeric) => {
                        state.is_first_char = false;
                        state.current_char_offset = next_char_offset;
                        state.current_char = next_char;
                        None
                    }
                    _ => {
                        let offsets = TokenOffsets {
                            start: state.start_offset,
                            end: state.current_char_offset + state.current_char.len_utf8(),
                        };
                        state.is_first_char = true;
                        state.start_offset = next_char_offset;
                        state.current_char_offset = next_char_offset;
                        state.current_char = next_char;
                        state.current_char_type = next_char_type;
                        Some(offsets)
                    }
                }
            }
        }
    }

    fn finalize(&mut self) -> Option<TokenOffsets> {
        match self {
            Self::Empty => None,
            Self::ProcessingChars(char_state) => {
                let offsets = TokenOffsets {
                    start: char_state.start_offset,
                    end: char_state.current_char_offset + char_state.current_char.len_utf8(),
                };
                *self = Self::Empty;
                Some(offsets)
            }
        }
    }
}

/// Returns the type of the character:
/// - `UpperCase` for `p{Lu}`.
/// - `LowerCase` for `p{Ll}`.
/// - `Numeric` for `\d`.
/// - `Delimiter` for the remaining characters.
fn get_char_type(c: char) -> CharType {
    if c.is_alphabetic() {
        if c.is_uppercase() {
            CharType::UpperCase
        } else {
            CharType::LowerCase
        }
    } else if c.is_numeric() {
        return CharType::Numeric;
    } else {
        return CharType::Delimiter;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CharType {
    // Equivalent of regex `p{Lu}`.
    UpperCase,
    // Equivalent of regex `p{Ll}`.
    LowerCase,
    // Equivalent of regex `\d`.
    Numeric,
    // Other characters.
    Delimiter,
}

#[derive(Clone)]
struct ChineseTokenizer;

impl Tokenizer for ChineseTokenizer {
    type TokenStream<'a> = ChineseTokenStream<'a>;

    fn token_stream<'a>(&mut self, text: &'a str) -> Self::TokenStream<'a> {
        ChineseTokenStream {
            text,
            last_char: None,
            chars: text.char_indices(),
            token: Token::default(),
        }
    }
}

struct ChineseTokenStream<'a> {
    text: &'a str,
    last_char: Option<(usize, char)>,
    chars: CharIndices<'a>,
    token: Token,
}

fn char_is_cjk(c: char) -> bool {
    // Block                                   Range       Comment
    // CJK Unified Ideographs                  4E00-9FFF   Common
    // CJK Unified Ideographs Extension A      3400-4DBF   Rare
    // CJK Unified Ideographs Extension B      20000-2A6DF Rare, historic
    // CJK Unified Ideographs Extension C      2A700–2B73F Rare, historic
    // CJK Unified Ideographs Extension D      2B740–2B81F Uncommon, some in current use
    // CJK Unified Ideographs Extension E      2B820–2CEAF Rare, historic
    matches!(c,
        '\u{4500}'..='\u{9FFF}' |
        '\u{3400}'..='\u{4DBF}' |
        '\u{20000}'..='\u{2A6DF}' |
        '\u{2A700}'..='\u{2CEAF}' // merge of extension C,D and E.
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Grouping {
    Keep,
    SplitKeep,
    SplitIgnore,
}

fn char_grouping(c: char) -> Grouping {
    if c.is_alphanumeric() {
        if char_is_cjk(c) {
            Grouping::SplitKeep
        } else {
            Grouping::Keep
        }
    } else {
        Grouping::SplitIgnore
    }
}

impl<'a> TokenStream for ChineseTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);

        let mut iter = self.last_char.take().into_iter().chain(&mut self.chars);

        while let Some((offset_from, c)) = iter.next() {
            match char_grouping(c) {
                Grouping::Keep => {
                    let offset_to = if let Some((next_index, next_char)) =
                        iter.find(|&(_, c)| char_grouping(c) != Grouping::Keep)
                    {
                        self.last_char = Some((next_index, next_char));
                        next_index
                    } else {
                        self.text.len()
                    };

                    self.token.offset_from = offset_from;
                    self.token.offset_to = offset_to;
                    self.token.text.push_str(&self.text[offset_from..offset_to]);
                    return true;
                }
                Grouping::SplitKeep => {
                    let num_bytes_in_char = c.len_utf8();
                    self.token.offset_from = offset_from;
                    self.token.offset_to = offset_from + num_bytes_in_char;
                    self.token
                        .text
                        .push_str(&self.text[offset_from..(self.token.offset_to)]);
                    return true;
                }
                Grouping::SplitIgnore => (),
            }
        }
        false
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

pub fn get_quickwit_fastfield_normalizer_manager() -> &'static TokenizerManager {
    static QUICKWIT_FAST_FIELD_NORMALIZER_MANAGER: Lazy<TokenizerManager> =
        Lazy::new(create_quickwit_fastfield_normalizer_manager);
    &QUICKWIT_FAST_FIELD_NORMALIZER_MANAGER
}

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

    use super::{create_default_quickwit_tokenizer_manager, CodeTokenizer};

    #[test]
    fn test_raw_tokenizer() {
        let my_haiku = r#"
        white sandy beach
        a strong wind is coming
        sand in my face
        "#;

        let mut tokenizer = create_default_quickwit_tokenizer_manager()
            .get("raw")
            .unwrap();
        {
            let mut haiku_stream = tokenizer.token_stream(my_haiku);
            assert!(haiku_stream.advance());
            assert!(!haiku_stream.advance());
        }
        {
            let my_too_long_text = vec!["a".repeat(255)].join("");
            assert!(!tokenizer.token_stream(&my_too_long_text).advance());
        }
        {
            let my_long_text = vec!["a".repeat(254)].join("");
            assert!(tokenizer.token_stream(&my_long_text).advance());
        }
    }

    #[test]
    fn test_chinese_tokenizer() {
        let text = "Hello world, 你好世界, bonjour monde";

        let mut tokenizer = create_default_quickwit_tokenizer_manager()
            .get("chinese_compatible")
            .unwrap();
        let mut text_stream = tokenizer.token_stream(text);

        let mut res = Vec::new();
        while let Some(tok) = text_stream.next() {
            res.push(tok.clone());
        }

        // latin alphabet splited on white spaces, Han split on each char
        let expected = [
            Token {
                offset_from: 0,
                offset_to: 5,
                position: 0,
                text: "hello".to_owned(),
                position_length: 1,
            },
            Token {
                offset_from: 6,
                offset_to: 11,
                position: 1,
                text: "world".to_owned(),
                position_length: 1,
            },
            Token {
                offset_from: 13,
                offset_to: 16,
                position: 2,
                text: "你".to_owned(),
                position_length: 1,
            },
            Token {
                offset_from: 16,
                offset_to: 19,
                position: 3,
                text: "好".to_owned(),
                position_length: 1,
            },
            Token {
                offset_from: 19,
                offset_to: 22,
                position: 4,
                text: "世".to_owned(),
                position_length: 1,
            },
            Token {
                offset_from: 22,
                offset_to: 25,
                position: 5,
                text: "界".to_owned(),
                position_length: 1,
            },
            Token {
                offset_from: 27,
                offset_to: 34,
                position: 6,
                text: "bonjour".to_owned(),
                position_length: 1,
            },
            Token {
                offset_from: 35,
                offset_to: 40,
                position: 7,
                text: "monde".to_owned(),
                position_length: 1,
            },
        ];

        assert_eq!(res, expected);
    }

    #[test]
    fn test_chinese_tokenizer_no_space() {
        let text = "Hello你好bonjour";

        let mut tokenizer = create_default_quickwit_tokenizer_manager()
            .get("chinese_compatible")
            .unwrap();
        let mut text_stream = tokenizer.token_stream(text);

        let mut res = Vec::new();
        while let Some(tok) = text_stream.next() {
            res.push(tok.clone());
        }

        let expected = [
            Token {
                offset_from: 0,
                offset_to: 5,
                position: 0,
                text: "hello".to_owned(),
                position_length: 1,
            },
            Token {
                offset_from: 5,
                offset_to: 8,
                position: 1,
                text: "你".to_owned(),
                position_length: 1,
            },
            Token {
                offset_from: 8,
                offset_to: 11,
                position: 2,
                text: "好".to_owned(),
                position_length: 1,
            },
            Token {
                offset_from: 11,
                offset_to: 18,
                position: 3,
                text: "bonjour".to_owned(),
                position_length: 1,
            },
        ];

        assert_eq!(res, expected);
    }

    proptest::proptest! {
        #[test]
        fn test_proptest_ascii_default_chinese_equal(text in "[ -~]{0,64}") {
            let mut cn_tok = create_default_quickwit_tokenizer_manager().get("chinese_compatible").unwrap();
            let mut default_tok = create_default_quickwit_tokenizer_manager().get("default").unwrap();

            let mut text_stream = cn_tok.token_stream(&text);

            let mut cn_res = Vec::new();
            while let Some(tok) = text_stream.next() {
                cn_res.push(tok.clone());
            }

            let mut text_stream = default_tok.token_stream(&text);

            let mut default_res = Vec::new();
            while let Some(tok) = text_stream.next() {
                default_res.push(tok.clone());
            }

            assert_eq!(cn_res, default_res);
        }
    }

    #[test]
    fn test_code_tokenizer_in_tokenizer_manager() {
        let mut code_tokenizer = create_default_quickwit_tokenizer_manager()
            .get("source_code")
            .unwrap();
        let mut token_stream = code_tokenizer.token_stream("PigCaféFactory2");
        let mut tokens = Vec::new();
        while let Some(token) = token_stream.next() {
            tokens.push(token.text.to_string());
        }
        assert_eq!(tokens, vec!["pig", "café", "factory", "2"])
    }

    #[test]
    fn test_code_tokenizer() {
        let mut tokenizer = CodeTokenizer::default();
        {
            let mut token_stream = tokenizer.token_stream("PigCaféFactory2");
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.clone());
            }
            let expected_tokens = vec![
                Token {
                    offset_from: 0,
                    offset_to: 3,
                    position: 0,
                    text: "Pig".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 3,
                    offset_to: 8,
                    position: 1,
                    text: "Café".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 8,
                    offset_to: 15,
                    position: 2,
                    text: "Factory".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 15,
                    offset_to: 16,
                    position: 3,
                    text: "2".to_owned(),
                    position_length: 1,
                },
            ];
            assert_eq!(res, expected_tokens);
        }
        {
            let mut token_stream = tokenizer.token_stream("PIG_CAFE_FACTORY");
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.clone());
            }
            let expected_tokens = vec![
                Token {
                    offset_from: 0,
                    offset_to: 3,
                    position: 0,
                    text: "PIG".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 4,
                    offset_to: 8,
                    position: 1,
                    text: "CAFE".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 9,
                    offset_to: 16,
                    position: 2,
                    text: "FACTORY".to_owned(),
                    position_length: 1,
                },
            ];
            assert_eq!(res, expected_tokens);
        }
        {
            let mut token_stream = tokenizer.token_stream("TPigCafeFactory");
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.clone());
            }
            let expected_tokens = vec![
                Token {
                    offset_from: 0,
                    offset_to: 1,
                    position: 0,
                    text: "T".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 1,
                    offset_to: 4,
                    position: 1,
                    text: "Pig".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 4,
                    offset_to: 8,
                    position: 2,
                    text: "Cafe".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 8,
                    offset_to: 15,
                    position: 3,
                    text: "Factory".to_owned(),
                    position_length: 1,
                },
            ];
            assert_eq!(res, expected_tokens);
        }
        {
            let mut token_stream = tokenizer.token_stream("PIG# Cafe@FACTORY");
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.clone());
            }
            let expected_tokens = vec![
                Token {
                    offset_from: 0,
                    offset_to: 3,
                    position: 0,
                    text: "PIG".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 5,
                    offset_to: 9,
                    position: 1,
                    text: "Cafe".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 10,
                    offset_to: 17,
                    position: 2,
                    text: "FACTORY".to_owned(),
                    position_length: 1,
                },
            ];
            assert_eq!(res, expected_tokens);
        }
    }
}
