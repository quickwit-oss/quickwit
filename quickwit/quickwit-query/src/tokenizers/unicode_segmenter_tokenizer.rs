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
    iter: unicode_segmentation::UnicodeWordIndices<'a>,
    token: Token,
    position: usize,
}

impl<'a> TokenStream for UnicodeSegmenterTokenStream<'a> {
    fn advance(&mut self) -> bool {
        if let Some((start, word)) = self.iter.next() {
            let pos = self.position;
            let end = start + word.len();
            let token = self.token_mut();
            token.text.clear();
            token.text.push_str(word);
            token.offset_from = start;
            token.offset_to = end;
            token.position = pos;
            self.position += 1;
            true
        } else {
            false
        }
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
        UnicodeSegmenterTokenStream {
            iter: text.unicode_word_indices(),
            token: Token::default(),
            position: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unicode_segmenter_tokenizer() {
        let text = r#"The quick ("brown") fox can't jump 32.3 feet, right?"#;
        let mut tokenizer = UnicodeSegmenterTokenizer;
        let mut stream = tokenizer.token_stream(text);

        let mut tokens = Vec::new();
        while stream.advance() {
            let token = stream.token().clone();
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
