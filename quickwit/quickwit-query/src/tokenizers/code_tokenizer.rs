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

use std::ops::Range;
use std::str::CharIndices;

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

/// A Tokenizer splitting based on casing families often used in code such ase camelCase or
/// PascalCase.
///
/// For instance, it splits `PigCaféFactory2` as `[Pig, Café, Factory, 2]`, or `RPCResult` into
/// `[RPC, Result]`.
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

enum AdvanceResult {
    None,
    Emit(TokenOffsets),
    Backtrack,
}

impl CodeTokenStream<'_> {
    fn advance_inner(&mut self, enable_hex: bool) -> bool {
        // this is cheap, just a copy of a few ptrs and integers
        let checkpoint = self.chars.clone();

        while let Some((next_char_offset, next_char)) = self.chars.next() {
            match self.state.advance(next_char_offset, next_char, enable_hex) {
                AdvanceResult::None => {}
                AdvanceResult::Emit(token_offsets) => {
                    self.update_token(token_offsets);
                    return true;
                }
                AdvanceResult::Backtrack => {
                    self.chars = checkpoint;
                    self.state.reset();
                    // this can't recurse more than once, Backtrack is only emitted from hex state,
                    // and calling with false prevent that state from being generated.
                    return self.advance_inner(false);
                }
            }
        }

        // No more chars.
        match self.state.finalize() {
            AdvanceResult::None => {}
            AdvanceResult::Emit(token_offsets) => {
                self.update_token(token_offsets);
                return true;
            }
            AdvanceResult::Backtrack => {
                self.chars = checkpoint;
                self.state.reset();
                return self.advance_inner(false);
            }
        }

        false
    }
}

impl TokenStream for CodeTokenStream<'_> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);

        self.advance_inner(self.enable_hex)
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

impl CodeTokenStream<'_> {
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
    ProcessingHex(ProcessingHexState),
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
        allow_hex: bool,
    ) -> AdvanceResult {
        let next_char_type = get_char_type(next_char);
        match self {
            Self::Empty => {
                match next_char_type {
                    CharType::Delimiter => {
                        self.reset();
                    }
                    _ => {
                        let is_hex = next_char.is_ascii_digit()
                            || ('a'..='f').contains(&next_char)
                            || ('A'..='F').contains(&next_char);
                        if allow_hex && is_hex {
                            *self = CodeTokenStreamState::ProcessingHex(ProcessingHexState {
                                seen_lowercase: next_char_type == CharType::LowerCase,
                                seen_uppercase: next_char_type == CharType::UpperCase,
                                seen_number: next_char_type == CharType::Numeric,
                                start_offset: next_char_offset,
                                current_char: next_char,
                                current_char_offset: next_char_offset,
                            });
                        } else {
                            *self = CodeTokenStreamState::ProcessingChars(ProcessingCharsState {
                                is_first_char: true,
                                start_offset: next_char_offset,
                                current_char_offset: next_char_offset,
                                current_char: next_char,
                                current_char_type: next_char_type,
                            });
                        }
                    }
                }
                AdvanceResult::None
            }
            Self::ProcessingChars(state) => {
                match (state.current_char_type, next_char_type) {
                    (_, CharType::Delimiter) => {
                        let offsets = TokenOffsets {
                            start: state.start_offset,
                            end: state.current_char_offset + state.current_char.len_utf8(),
                        };
                        // this is the only case where we want to reset, otherwise we might get
                        // back to a hex-state in a place where we did not get a delimiter
                        self.reset();
                        AdvanceResult::Emit(offsets)
                    }
                    // We do not emit a token if we have only `Ac` (is_first_char = true).
                    // But we emit the token `AB` if we have `ABCa`,
                    (CharType::UpperCase, CharType::LowerCase) => {
                        if state.is_first_char {
                            state.is_first_char = false;
                            state.current_char_offset = next_char_offset;
                            state.current_char = next_char;
                            state.current_char_type = next_char_type;
                            AdvanceResult::None
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
                            AdvanceResult::Emit(offsets)
                        }
                    }
                    // Don't emit tokens on identical char types.
                    (CharType::UpperCase, CharType::UpperCase)
                    | (CharType::LowerCase, CharType::LowerCase)
                    | (CharType::Numeric, CharType::Numeric) => {
                        state.is_first_char = false;
                        state.current_char_offset = next_char_offset;
                        state.current_char = next_char;
                        AdvanceResult::None
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
                        AdvanceResult::Emit(offsets)
                    }
                }
            }
            Self::ProcessingHex(state) => {
                match state.consume_char(next_char_offset, next_char) {
                    HexResult::None => AdvanceResult::None,
                    HexResult::Emit(offsets) => {
                        self.reset();
                        AdvanceResult::Emit(offsets)
                    }
                    HexResult::RecoverableError(state) => {
                        *self = CodeTokenStreamState::ProcessingChars(state);
                        // the char wasn't actually consumed, we recurse once to make sure it is
                        self.advance(next_char_offset, next_char, allow_hex)
                    }
                    HexResult::IrrecoverableError => AdvanceResult::Backtrack,
                }
            }
        }
    }

    fn finalize(&mut self) -> AdvanceResult {
        match self {
            Self::Empty => AdvanceResult::None,
            Self::ProcessingChars(char_state) => {
                let offsets = TokenOffsets {
                    start: char_state.start_offset,
                    end: char_state.current_char_offset + char_state.current_char.len_utf8(),
                };
                *self = Self::Empty;
                AdvanceResult::Emit(offsets)
            }
            CodeTokenStreamState::ProcessingHex(hex_state) => match hex_state.finalize() {
                HexResult::None => unreachable!(),
                HexResult::Emit(offsets) => {
                    *self = Self::Empty;
                    AdvanceResult::Emit(offsets)
                }
                HexResult::RecoverableError(state) => {
                    *self = CodeTokenStreamState::ProcessingChars(state);
                    self.finalize()
                }
                HexResult::IrrecoverableError => AdvanceResult::Backtrack,
            },
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
        CharType::Numeric
    } else {
        CharType::Delimiter
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

#[derive(Debug)]
struct ProcessingHexState {
    seen_uppercase: bool,
    seen_lowercase: bool,
    seen_number: bool,

    start_offset: usize,
    current_char_offset: usize,
    current_char: char,
}

enum HexResult {
    // no token emitted
    None,
    // a token is being emitted, after that the state needs to be reset.
    Emit(TokenOffsets),
    // we got an error, but where able to generate a code tokenizer state
    RecoverableError(ProcessingCharsState),
    // we got an error and can't generate a code tokenizer state, we need to backtrack
    IrrecoverableError,
}

impl ProcessingHexState {
    // if this returns an error, the char was *not* consumed
    fn consume_char(&mut self, next_char_offset: usize, next_char: char) -> HexResult {
        match next_char {
            '0'..='9' => self.seen_number = true,
            'a'..='f' => {
                if !self.seen_uppercase {
                    self.seen_lowercase = true;
                } else {
                    return self.to_processing_chars_state();
                }
            }
            'A'..='F' => {
                if !self.seen_lowercase {
                    self.seen_uppercase = true;
                } else {
                    return self.to_processing_chars_state();
                }
            }
            c => {
                if get_char_type(c) == CharType::Delimiter {
                    // end of sequence, check if size is multiple of 2, or try to generate code
                    // state. We use next_char_offset as it already takes into account the size of
                    // the last character
                    if (next_char_offset - self.start_offset).is_multiple_of(2) {
                        return HexResult::Emit(self.start_offset..next_char_offset);
                    }
                }
                // we got an invalid non-delimiter, or our sequence is an odd-length. Either way,
                // we need to go switch to the code tokenizer
                return self.to_processing_chars_state();
            }
        }
        // char was accepted, update state
        self.current_char_offset = next_char_offset;
        self.current_char = next_char;
        HexResult::None
    }

    fn to_processing_chars_state(&self) -> HexResult {
        let current_char_type = match (self.seen_uppercase, self.seen_lowercase, self.seen_number) {
            // for Aab, we actually take this branch has a hasn't been consumed just yet.
            (true, false, false) => CharType::UpperCase,
            (false, true, false) => CharType::LowerCase,
            (false, false, true) => CharType::Numeric,
            _ => return HexResult::IrrecoverableError,
        };
        HexResult::RecoverableError(ProcessingCharsState {
            current_char: self.current_char,
            current_char_offset: self.current_char_offset,
            start_offset: self.start_offset,
            is_first_char: self.current_char_offset == self.start_offset,
            current_char_type,
        })
    }

    fn finalize(&self) -> HexResult {
        let next_char_offset = self.current_char_offset + self.current_char.len_utf8();
        if (next_char_offset - self.start_offset).is_multiple_of(2) {
            return HexResult::Emit(self.start_offset..next_char_offset);
        }
        self.to_processing_chars_state()
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
    }

    #[test]
    fn test_code_tokenizer_hex() {
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
    }

    #[test]
    fn test_code_tokenizer_hex_scenaris() {
        let test_vectors = vec![
            // simple hex, separated by delimiter, or at end of string
            (
                "fa63bbbf-0fb9-5ec8-ae63-561dc0f444aa",
                vec!["fa63bbbf", "0fb9", "5ec8", "ae63", "561dc0f444aa"],
            ),
            (
                "FA63BBBF-0FB9-5EC8-AE63-561DC0F444AA",
                vec!["FA63BBBF", "0FB9", "5EC8", "AE63", "561DC0F444AA"],
            ),
            // last token has odd len
            (
                "fa63bbbf-0fb9-5ec8-ae63-561dc0f444a",
                vec![
                    "fa63bbbf", "0fb9", "5ec8", "ae63", "561", "dc", "0", "f", "444", "a",
                ],
            ),
            // a middle token has odd len
            (
                "fa63bbbf-0fb9-5ec8-ae6-561dc0f444aa",
                vec!["fa63bbbf", "0fb9", "5ec8", "ae", "6", "561dc0f444aa"],
            ),
            // token starts with upper case
            (
                "Fa63bbbf-0fb9-5ec8-ae63-561dc0f444aa",
                vec!["Fa", "63", "bbbf", "0fb9", "5ec8", "ae63", "561dc0f444aa"],
            ),
            // change in case during a token
            (
                "fa63Bbbf-0fb9-5ec8-ae63-561dc0f444aa",
                vec!["fa", "63", "Bbbf", "0fb9", "5ec8", "ae63", "561dc0f444aa"],
            ),
            (
                "fa63bbBf-0fb9-5ec8-ae63-561dc0f444aa",
                vec![
                    "fa",
                    "63",
                    "bb",
                    "Bf",
                    "0fb9",
                    "5ec8",
                    "ae63",
                    "561dc0f444aa",
                ],
            ),
            // token starts with lower case
            (
                "fA63BBBF-0FB9-5EC8-AE63-561DC0F444AA",
                vec![
                    "f",
                    "A",
                    "63",
                    "BBBF",
                    "0FB9",
                    "5EC8",
                    "AE63",
                    "561DC0F444AA",
                ],
            ),
            // token contain non hex
            (
                "fa63bgbf-0fb9-5ec8-ae63-561dc0f444aa",
                vec!["fa", "63", "bgbf", "0fb9", "5ec8", "ae63", "561dc0f444aa"],
            ),
            // non 0-9 numeric
            (
                "fa6③bbbf-0fb9-5ec8-ae63-561dc0f444aa",
                vec!["fa", "6③", "bbbf", "0fb9", "5ec8", "ae63", "561dc0f444aa"],
            ),
            ("301ms", vec!["301", "ms"]),
            ("301cd", vec!["301", "cd"]),
            ("30ms", vec!["30", "ms"]),
            // we don't know if it's candelas or hex, and assume hex in this case
            ("30cd", vec!["30cd"]),
            ("ABCDef", vec!["ABC", "Def"]),
        ];

        let mut tokenizer = CodeTokenizer::with_hex_support();
        for (text, expected) in test_vectors {
            let mut token_stream = tokenizer.token_stream(text);
            let mut res = Vec::new();
            while let Some(tok) = token_stream.next() {
                res.push(tok.text.clone());
            }
            assert_eq!(res, expected);
        }
    }
}
