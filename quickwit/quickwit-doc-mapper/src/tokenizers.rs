// Copyright (C) 2022 Quickwit, Inc.
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

use std::str::CharIndices;

use once_cell::sync::Lazy;
use tantivy::tokenizer::{
    BoxTokenStream, LowerCaser, RawTokenizer, RemoveLongFilter, TextAnalyzer, Token, TokenStream,
    Tokenizer, TokenizerManager,
};

fn get_quickwit_tokenizer_manager() -> TokenizerManager {
    let raw_tokenizer = TextAnalyzer::from(RawTokenizer).filter(RemoveLongFilter::limit(100));

    let chinese_tokenizer = TextAnalyzer::from(ChineseTokenizer)
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser);

    let tokenizer_manager = TokenizerManager::default();

    tokenizer_manager.register("raw", raw_tokenizer);
    tokenizer_manager.register("chinese", chinese_tokenizer);

    tokenizer_manager
}

#[derive(Clone)]
struct ChineseTokenizer;

impl Tokenizer for ChineseTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a> {
        BoxTokenStream::from(ChineseTokenStream {
            text,
            last_char: None,
            chars: text.char_indices(),
            token: Token::default(),
        })
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
                    let offset_to = if let Some((next_index, next_char)) = iter
                        .filter(|&(_, c)| char_grouping(c) != Grouping::Keep)
                        .next()
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
                    let len = c.len_utf8();
                    self.token.offset_from = offset_from;
                    self.token.offset_to = offset_from + len;
                    self.token
                        .text
                        .push_str(&self.text[offset_from..(offset_from + len)]);
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

/// Quickwits default tokenizer
pub static QUICKWIT_TOKENIZER_MANAGER: Lazy<TokenizerManager> =
    Lazy::new(get_quickwit_tokenizer_manager);

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::Token;

    use super::get_quickwit_tokenizer_manager;

    #[test]
    fn test_raw_tokenizer() {
        let my_haiku = r#"
        white sandy beach
        a strong wind is coming 
        sand in my face
        "#;
        let my_long_text = "a text, that is just too long, no one will type it, no one will like \
                            it, no one shall find it. I just need some more chars, now you may \
                            not pass.";

        let tokenizer = get_quickwit_tokenizer_manager().get("raw").unwrap();
        let mut haiku_stream = tokenizer.token_stream(my_haiku);
        assert!(haiku_stream.advance());
        assert!(!haiku_stream.advance());
        assert!(!tokenizer.token_stream(my_long_text).advance());
    }

    #[test]
    fn test_chinese_tokenizer() {
        let text = "Hello world, 你好世界, bonjour monde";

        let tokenizer = get_quickwit_tokenizer_manager().get("chinese").unwrap();
        let mut text_stream = tokenizer.token_stream(text);

        let mut res = Vec::new();
        while let Some(tok) = text_stream.next() {
            res.push(tok.clone());
        }

        // latin alphabet splited on white spaces, Han splitted on each char
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

        assert_eq!(dbg!(res), dbg!(expected));
    }

    #[test]
    fn test_chinese_tokenizer_no_space() {
        let text = "Hello你好bonjour";

        let tokenizer = get_quickwit_tokenizer_manager().get("chinese").unwrap();
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

        assert_eq!(dbg!(res), dbg!(expected));
    }
}
