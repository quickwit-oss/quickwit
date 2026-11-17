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

/// Tokenizer based on Unicode word boundaries (Unicode Standard Annex #29).
///
/// Splits text into tokens at Unicode word boundaries, preserving words that
/// contain punctuation internally (e.g. "can't", "32.3"). This makes it
/// well-suited for log messages and natural language text with mixed content.
#[derive(Clone, Default)]
pub struct UnicodeSegmenterTokenizer;

pub struct UnicodeSegmenterTokenStream<'a> {
    words: unicode_segmentation::UnicodeWordIndices<'a>,
    emit_dot_parts: bool,
    parts: Option<std::str::Split<'a, char>>,
    part_offset: usize,
    position: usize,
    token: Token,
}

impl<'a> UnicodeSegmenterTokenStream<'a> {
    pub(super) fn new(text: &'a str, emit_dot_parts: bool) -> Self {
        Self {
            words: text.unicode_word_indices(),
            emit_dot_parts,
            parts: None,
            part_offset: 0,
            position: 0,
            token: Token::default(),
        }
    }

    fn set_token(&mut self, text: &str, offset: usize, position_length: usize) {
        self.token.text.clear();
        self.token.text.push_str(text);
        self.token.offset_from = offset;
        self.token.offset_to = offset + text.len();
        self.token.position = self.position;
        self.token.position_length = position_length;
    }
}

impl TokenStream for UnicodeSegmenterTokenStream<'_> {
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
        let split = self.emit_dot_parts
            && word.contains('.')
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

impl Tokenizer for UnicodeSegmenterTokenizer {
    type TokenStream<'a> = UnicodeSegmenterTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        UnicodeSegmenterTokenStream::new(text, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unicode_segmenter_tokenizer() {
        let text = r#"The quick ("brown") fox can't jump 32.3 feet, right? Foo.Bar end"#;
        let mut tokenizer = UnicodeSegmenterTokenizer;
        let mut stream = tokenizer.token_stream(text);

        let mut tokens = Vec::new();
        while stream.advance() {
            let token = stream.token().clone();
            assert_eq!(token.position, tokens.len());
            assert_eq!(token.position_length, 1);
            tokens.push((token.offset_from, token.offset_to, token.text));
        }

        let expected = vec![
            (0, 3, "The"),
            (4, 9, "quick"),
            (12, 17, "brown"),
            (20, 23, "fox"),
            (24, 29, "can't"),
            (30, 34, "jump"),
            (35, 39, "32.3"),
            (40, 44, "feet"),
            (46, 51, "right"),
            (53, 60, "Foo.Bar"),
            (61, 64, "end"),
        ];
        assert_eq!(
            tokens,
            expected
                .into_iter()
                .map(|(start, end, word)| (start, end, word.to_string()))
                .collect::<Vec<_>>()
        );
    }
}
