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

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};
use unicode_segmentation::UnicodeSegmentation;

/// Unicode word tokenization with additional dot-separated parts.
///
/// The original token is emitted first and spans the positions occupied by its
/// parts. Tokens containing only numbers and dots (e.g. IP addresses and decimals)
/// stay whole. Only ASCII dots are separators.
#[derive(Clone, Default)]
pub struct DatadogV2Tokenizer;

pub struct DatadogV2TokenStream<'a> {
    words: unicode_segmentation::UnicodeWordIndices<'a>,
    parts: Option<std::str::Split<'a, char>>,
    part_offset: usize,
    position: usize,
    token: Token,
}

impl Tokenizer for DatadogV2Tokenizer {
    type TokenStream<'a> = DatadogV2TokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        DatadogV2TokenStream {
            words: text.unicode_word_indices(),
            parts: None,
            part_offset: 0,
            position: 0,
            token: Token::default(),
        }
    }
}

impl DatadogV2TokenStream<'_> {
    fn set_token(&mut self, text: &str, offset: usize, position_length: usize) {
        self.token.text.clear();
        self.token.text.push_str(text);
        self.token.offset_from = offset;
        self.token.offset_to = offset + text.len();
        self.token.position = self.position;
        self.token.position_length = position_length;
    }
}

impl TokenStream for DatadogV2TokenStream<'_> {
    fn advance(&mut self) -> bool {
        if let Some(part) = self.parts.as_mut().and_then(Iterator::next) {
            self.set_token(part, self.part_offset, 1);
            self.part_offset += part.len() + 1;
            self.position += 1;
            return true;
        }
        self.parts = None;

        let Some((offset, word)) = self.words.next() else {
            return false;
        };
        let split = word.contains('.')
            && !word
                .chars()
                .all(|character| character == '.' || character.is_numeric());
        if split {
            // Unicode word boundaries exclude leading, trailing, and consecutive
            // dots, so every part is nonempty. The first part shares the original's
            // position, and subsequent parts occupy consecutive positions.
            let parts = word.split('.');
            self.set_token(word, offset, parts.clone().count());
            self.parts = Some(parts);
            self.part_offset = offset;
        } else {
            self.set_token(word, offset, 1);
            self.position += 1;
        }
        true
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizers::create_default_quickwit_tokenizer_manager;

    fn tokens(text: &str) -> Vec<Token> {
        let mut tokenizer = DatadogV2Tokenizer;
        let mut stream = tokenizer.token_stream(text);
        let mut tokens = Vec::new();
        while stream.advance() {
            tokens.push(stream.token().clone());
        }
        tokens
    }

    #[test]
    fn test_datadog_v2_parts_offsets_and_positions() {
        let actual: Vec<_> = tokens("é Foo.Bär.baz end")
            .into_iter()
            .map(|token| {
                (
                    token.text,
                    token.offset_from,
                    token.offset_to,
                    token.position,
                    token.position_length,
                )
            })
            .collect();
        let expected = [
            ("é", 0, 2, 0, 1),
            ("Foo.Bär.baz", 3, 15, 1, 3),
            ("Foo", 3, 6, 1, 1),
            ("Bär", 7, 11, 2, 1),
            ("baz", 12, 15, 3, 1),
            ("end", 16, 19, 4, 1),
        ]
        .map(|(text, start, end, position, length)| {
            (text.to_string(), start, end, position, length)
        });
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_datadog_v2_numeric_tokens() {
        let actual: Vec<_> = tokens("192.168.1.1 32.3 １２.３ 1.2.3 abc.def 42")
            .into_iter()
            .map(|token| token.text)
            .collect();
        assert_eq!(
            actual,
            [
                "192.168.1.1",
                "32.3",
                "１２.３",
                "1.2.3",
                "abc.def",
                "abc",
                "def",
                "42"
            ]
        );
    }

    #[test]
    fn test_datadog_v2_word_boundaries() {
        for (text, expected) in [
            ("", vec![]),
            ("...", vec![]),
            (".foo..bar.", vec!["foo", "bar"]),
            (
                "can't foo.bar baz.qux",
                vec!["can't", "foo.bar", "foo", "bar", "baz.qux", "baz", "qux"],
            ),
            ("v1.2.3", vec!["v1.2.3", "v1", "2", "3"]),
        ] {
            let actual: Vec<_> = tokens(text).into_iter().map(|token| token.text).collect();
            assert_eq!(actual, expected, "{text}");
        }
    }

    #[test]
    fn test_datadog_v2_analyzer() {
        let manager = create_default_quickwit_tokenizer_manager();
        let long_part = "a".repeat(256);
        let text = format!("Foo.BAR 192.168.1.1 {long_part}.OK");
        for (name, expected) in [
            ("datadog", vec!["foo.bar", "192.168.1.1"]),
            (
                "datadog_v2",
                vec!["foo.bar", "foo", "bar", "192.168.1.1", "ok"],
            ),
        ] {
            let mut analyzer = manager.get_tokenizer(name).unwrap();
            let mut stream = analyzer.token_stream(&text);
            let mut actual = Vec::new();
            while stream.advance() {
                actual.push(stream.token().text.clone());
            }
            assert_eq!(actual, expected, "{name}");
        }
    }
}
