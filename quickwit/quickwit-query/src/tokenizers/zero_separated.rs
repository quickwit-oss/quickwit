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

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

/// Zero separated tokenizer for the field presence field.
///
/// We avoid having one value per field present in order to avoid
/// allocating too many strings.
///
/// The text is supposed to have a trailing \0.
#[derive(Clone, Default)]
pub struct ZeroSeparatedTokenizer {
    token: Token,
}

impl Tokenizer for ZeroSeparatedTokenizer {
    type TokenStream<'a> = ZeroSeparatedTokenizerStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.token.reset();
        ZeroSeparatedTokenizerStream {
            text,
            token: &mut self.token,
            offset: 0,
        }
    }
}

pub struct ZeroSeparatedTokenizerStream<'a> {
    text: &'a str,
    token: &'a mut Token,
    offset: usize,
}

impl<'a> TokenStream for ZeroSeparatedTokenizerStream<'a> {
    fn advance(&mut self) -> bool {
        if self.text.is_empty() {
            return false;
        }
        let (left, right) = self.text.split_once('\0').unwrap_or((self.text, ""));
        self.text = right;
        self.token.text.clear();
        self.token.text.push_str(left);
        self.token.position = self.token.position.wrapping_add(1);
        self.token.offset_from = self.offset;
        self.token.offset_to = self.offset + left.len();
        self.offset += left.len() + 1;
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

    use super::ZeroSeparatedTokenizer;

    #[test]
    fn test_zero_separated_tokenizer_empty() {
        let mut tokenizer = ZeroSeparatedTokenizer::default();
        let mut token_stream = tokenizer.token_stream("");
        assert!(!token_stream.advance());
    }

    #[test]
    fn test_zero_separated_tokenizer_simple() {
        let mut tokenizer = ZeroSeparatedTokenizer::default();
        let mut token_stream = tokenizer.token_stream("hello\0happy\0");
        let mut tokens = Vec::new();
        token_stream.process(&mut |token| {
            tokens.push(token.clone());
        });
        assert_eq!(
            tokens,
            &[
                Token {
                    offset_from: 0,
                    offset_to: 5,
                    position: 0,
                    text: "hello".to_string(),
                    position_length: 1
                },
                Token {
                    offset_from: 6,
                    offset_to: 11,
                    position: 1,
                    text: "happy".to_string(),
                    position_length: 1
                },
            ]
        )
    }

    // Technically we do not use this case, but it is good to fix its spec.
    #[test]
    fn test_zero_separated_tokenizer_trailing() {
        let mut tokenizer = ZeroSeparatedTokenizer::default();
        let mut token_stream = tokenizer.token_stream("hello\0happy\0trailing");
        let mut tokens = Vec::new();
        token_stream.process(&mut |token| {
            tokens.push(token.clone());
        });
        assert_eq!(
            tokens,
            &[
                Token {
                    offset_from: 0,
                    offset_to: 5,
                    position: 0,
                    text: "hello".to_string(),
                    position_length: 1
                },
                Token {
                    offset_from: 6,
                    offset_to: 11,
                    position: 1,
                    text: "happy".to_string(),
                    position_length: 1
                },
                Token {
                    offset_from: 12,
                    offset_to: 20,
                    position: 2,
                    text: "trailing".to_string(),
                    position_length: 1
                },
            ]
        )
    }
}
