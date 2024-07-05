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

/// A Tokenizer spliting based on casing families often used in code such ase camelCase or
/// PascalCase.
///
/// For instance, it splits `PigCaféFactory2` as `[Pig, Café, Factory, 2]`, or `RPCResult` into
/// `|RPC, Result]`.
///
/// Optionally, it can keep sequences of hexadecimal chars together, which can be useful when
/// dealing with ids encoded in that way, such as UUIDs.
#[derive(Clone, Default)]
pub struct CodeTokenizer {
    token: Token,
    enable_hex: bool,
}

impl CodeTokenizer {
    /// When hex support is enabled, the tokenizer tries to keep group of hexadecimal digits as one
    /// token, instead of splitting them in groups of letters and numbers.
    pub fn with_hex_support() -> Self {
        CodeTokenizer {
            token: Token::default(),
            enable_hex: true,
        }
    }
}

impl Tokenizer for CodeTokenizer {
    type TokenStream<'a> = CodeTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.token.reset();
        CodeTokenStream {
            chars: text.char_indices(),
            state: CodeTokenStreamState::Empty,
            text,
            token: &mut self.token,
            enable_hex: self.enable_hex,
        }
    }
}

pub struct CodeTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: &'a mut Token,
    state: CodeTokenStreamState,
    enable_hex: bool,
}

impl<'a> TokenStream for CodeTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);

        while let Some((next_char_offset, next_char)) = self.chars.next() {
            match self
                .state
                .advance(next_char_offset, next_char, self.enable_hex)
            {
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

    fn advance(
        &mut self,
        next_char_offset: usize,
        next_char: char,
        hex_enabled: bool,
    ) -> Option<TokenOffsets> {
        let next_char_type = CharType::from_char(next_char, hex_enabled);
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
                let (result_state, emit) = state
                    .current_char_type
                    .merge_char_type(next_char_type, state.is_first_char);
                match result_state {
                    Some(result_type) => {
                        let current_char_offset = state.current_char_offset;

                        state.is_first_char = false;
                        state.current_char_offset = next_char_offset;
                        state.current_char = next_char;
                        state.current_char_type = result_type;
                        if emit {
                            let offsets = TokenOffsets {
                                start: state.start_offset,
                                // TODO actually this needs to be from before the update just above
                                end: current_char_offset,
                            };
                            state.start_offset = current_char_offset;
                            Some(offsets)
                        } else {
                            None
                        }
                    }
                    None => {
                        if next_char_type == CharType::Delimiter {
                            let offsets = TokenOffsets {
                                start: state.start_offset,
                                end: state.current_char_offset + state.current_char.len_utf8(),
                            };
                            self.reset();
                            Some(offsets)
                        } else {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CharType {
    // Equivalent of regex `p{Lu}`.
    UpperCase,
    // Equivalent of regex `p{Ll}`.
    LowerCase,
    // Equivalent of regex `\d`.
    Numeric,
    // intermediate state that can move to UpperHex or to UpperCase, when only A-F has been seen
    // yet
    UpperHexLetter,
    // Equivalent to \dA-F, an isolated char never has that class
    UpperHex,
    // intermediate state that can move to LowerHex or to LowerCase, when only a-f has been seen
    // yet
    LowerHexLetter,
    // Equivalent to \da-f, an isolated char never has that class
    LowerHex,
    // Other characters.
    Delimiter,
}

impl CharType {
    /// Returns the type of the character:
    /// - `UpperHexLetter` for `A-F` if hex_enabled is true.
    /// - `LowerHexLetter` for `a-f` if hex_enabled is true.
    /// - `UpperCase` for `p{Lu}`.
    /// - `LowerCase` for `p{Ll}`.
    /// - `Numeric` for `\d`.
    /// - `Delimiter` for the remaining characters.
    fn from_char(c: char, hex_enabled: bool) -> CharType {
        if c.is_alphabetic() {
            if hex_enabled && ('A'..='F').contains(&c) {
                CharType::UpperHexLetter
            } else if hex_enabled && ('a'..='f').contains(&c) {
                CharType::LowerHexLetter
            } else if c.is_uppercase() {
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

    fn merge_char_type(self, new: CharType, second_char: bool) -> (Option<CharType>, bool) {
        use CharType::*;
        match (self, new) {
            // upper
            (UpperCase, UpperCase | UpperHexLetter) => (Some(UpperCase), false),
            (UpperHexLetter, UpperCase) => (Some(UpperCase), false),
            // lower
            (LowerCase, LowerCase | LowerHexLetter) => (Some(LowerCase), false),
            (LowerHexLetter, LowerCase) => (Some(LowerCase), false),
            // upper hex
            (UpperHexLetter, UpperHexLetter) => (Some(UpperHexLetter), false),
            (UpperHexLetter, Numeric) => (Some(UpperHex), false),
            (UpperHex, UpperHexLetter | Numeric) => (Some(UpperHex), false),
            (Numeric, UpperHexLetter) => (Some(UpperHex), false),
            // lower hex
            (LowerHexLetter, LowerHexLetter) => (Some(LowerHexLetter), false),
            (LowerHexLetter, Numeric) => (Some(LowerHex), false),
            (LowerHex, LowerHexLetter | Numeric) => (Some(LowerHex), false),
            (Numeric, LowerHexLetter) => (Some(LowerHex), false),
            // number
            (Numeric, Numeric) => (Some(Numeric), false),
            // capitalized word
            (UpperCase | UpperHexLetter, LowerCase | LowerHexLetter) if second_char => {
                (Some(LowerCase), false)
            }
            (UpperCase | UpperHexLetter, LowerCase | LowerHexLetter) => (Some(LowerCase), true),
            _ => (None, true),
        }
    }
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
        {
            let mut token_stream = tokenizer.token_stream("fa63bbbf-0fb9-5ec8-ae63-561dc0f444aa");
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.clone());
            }
            assert_eq!(res.len(), 17);
        }
        {
            let mut token_stream = tokenizer.token_stream("301ms");
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.clone());
            }
            let expected_tokens = vec![
                Token {
                    offset_from: 0,
                    offset_to: 3,
                    position: 0,
                    text: "301".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 3,
                    offset_to: 5,
                    position: 1,
                    text: "ms".to_owned(),
                    position_length: 1,
                },
            ];
            assert_eq!(res, expected_tokens);
        }
        {
            let mut token_stream = tokenizer.token_stream("abcgabc123");
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.clone());
            }
            let expected_tokens = vec![
                Token {
                    offset_from: 0,
                    offset_to: 7,
                    position: 0,
                    text: "abcgabc".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 7,
                    offset_to: 10,
                    position: 1,
                    text: "123".to_owned(),
                    position_length: 1,
                },
            ];
            assert_eq!(res, expected_tokens);
        }
        {
            // hex, but case switch
            let mut token_stream = tokenizer.token_stream("Abc123");
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.clone());
            }
            let expected_tokens = vec![
                Token {
                    offset_from: 0,
                    offset_to: 3,
                    position: 0,
                    text: "Abc".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 3,
                    offset_to: 6,
                    position: 1,
                    text: "123".to_owned(),
                    position_length: 1,
                },
            ];
            assert_eq!(res, expected_tokens);
        }
    }

    #[test]
    fn test_code_hex_tokenizer() {
        let mut tokenizer = CodeTokenizer::with_hex_support();
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
        {
            let mut token_stream = tokenizer.token_stream("fa63bbbf-0fb9-5ec8-ae63-561dc0f444aa");
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.clone());
            }
            let expected_tokens = vec![
                Token {
                    offset_from: 0,
                    offset_to: 8,
                    position: 0,
                    text: "fa63bbbf".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 9,
                    offset_to: 13,
                    position: 1,
                    text: "0fb9".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 14,
                    offset_to: 18,
                    position: 2,
                    text: "5ec8".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 19,
                    offset_to: 23,
                    position: 3,
                    text: "ae63".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 24,
                    offset_to: 36,
                    position: 4,
                    text: "561dc0f444aa".to_owned(),
                    position_length: 1,
                },
            ];
            assert_eq!(res, expected_tokens);
        }
        {
            let mut token_stream = tokenizer.token_stream("301ms");
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.clone());
            }
            let expected_tokens = vec![
                Token {
                    offset_from: 0,
                    offset_to: 3,
                    position: 0,
                    text: "301".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 3,
                    offset_to: 5,
                    position: 1,
                    text: "ms".to_owned(),
                    position_length: 1,
                },
            ];
            assert_eq!(res, expected_tokens);
        }
        {
            let mut token_stream = tokenizer.token_stream("abcgabc123");
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.clone());
            }
            let expected_tokens = vec![
                Token {
                    offset_from: 0,
                    offset_to: 7,
                    position: 0,
                    text: "abcgabc".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 7,
                    offset_to: 10,
                    position: 1,
                    text: "123".to_owned(),
                    position_length: 1,
                },
            ];
            assert_eq!(res, expected_tokens);
        }
        {
            // hex, but case switch
            let mut token_stream = tokenizer.token_stream("Abc123");
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.clone());
            }
            let expected_tokens = vec![
                Token {
                    offset_from: 0,
                    offset_to: 3,
                    position: 0,
                    text: "Abc".to_owned(),
                    position_length: 1,
                },
                Token {
                    offset_from: 3,
                    offset_to: 6,
                    position: 1,
                    text: "123".to_owned(),
                    position_length: 1,
                },
            ];
            assert_eq!(res, expected_tokens);
        }
    }
}
