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

use std::str::CharIndices;

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

#[derive(Clone)]
pub(crate) struct ChineseTokenizer;

impl Tokenizer for ChineseTokenizer {
    type TokenStream<'a> = ChineseTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        ChineseTokenStream {
            text,
            last_char: None,
            chars: text.char_indices(),
            token: Token::default(),
        }
    }
}

pub(crate) struct ChineseTokenStream<'a> {
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

impl TokenStream for ChineseTokenStream<'_> {
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

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::{Token, TokenStream};

    #[test]
    fn test_chinese_tokenizer() {
        let text = "Hello world, 你好世界, bonjour monde";
        let tokenizer_manager = crate::create_default_quickwit_tokenizer_manager();
        let mut tokenizer = tokenizer_manager
            .get_tokenizer("chinese_compatible")
            .unwrap();
        let mut text_stream = tokenizer.token_stream(text);

        let mut res = Vec::new();
        while let Some(tok) = text_stream.next() {
            res.push(tok.clone());
        }

        // latin alphabet split on white spaces, Han split on each char
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
        let tokenizer_manager = crate::create_default_quickwit_tokenizer_manager();
        let mut tokenizer = tokenizer_manager
            .get_tokenizer("chinese_compatible")
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
            let tokenizer_manager = crate::create_default_quickwit_tokenizer_manager();
            let mut cn_tok = tokenizer_manager.get_tokenizer("chinese_compatible").unwrap();
            let mut default_tok = tokenizer_manager.get_tokenizer("default").unwrap();

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
}
