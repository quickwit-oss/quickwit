// Copyright (C) 2024 Quickwit, Inc.
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

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

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

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

    use super::CodeTokenizer;

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
