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

/// A Unicode-word tokenizer (https://www.unicode.org/reports/tr29/)
#[derive(Clone, Default)]
pub struct DatadogTokenizer;

pub struct DatadogTokenStream<'a> {
    iter: unicode_segmentation::UnicodeWordIndices<'a>,
    token: Token,
    position: usize,
}

impl<'a> TokenStream for DatadogTokenStream<'a> {
    fn advance(&mut self) -> bool {
        if let Some((start, word)) = self.iter.next() {
            let pos = self.position;
            let end = start + word.len();
            let t = self.token_mut();
            t.text.clear();
            t.text.push_str(word);
            t.offset_from = start;
            t.offset_to = end;
            t.position = pos;
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

impl Tokenizer for DatadogTokenizer {
    type TokenStream<'a> = DatadogTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        DatadogTokenStream {
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
    fn datadog_tokenizer() {
        let s = r#"The quick ("brown") fox can't jump 32.3 feet, right?"#;
        let mut tok = DatadogTokenizer;
        let mut stream = tok.token_stream(s);

        let mut got = Vec::new();
        while stream.advance() {
            let t = stream.token().clone();
            got.push((t.offset_from, t.offset_to, t.text));
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
        let got_slim: Vec<(usize, usize, String)> = got;
        assert_eq!(
            got_slim,
            expected
                .into_iter()
                .map(|(a, b, w)| (a, b, w.to_string()))
                .collect::<Vec<_>>()
        );
    }
}
