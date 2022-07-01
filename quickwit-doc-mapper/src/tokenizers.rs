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

use once_cell::sync::Lazy;
use regex::Regex;
use std::str::CharIndices;
use tantivy::tokenizer::{
    BoxTokenStream, RawTokenizer, RemoveLongFilter, TextAnalyzer, Token, TokenStream, Tokenizer,
    TokenizerManager,
};

static VALID_CHAR_IN_NUMBER : Lazy<Regex> = Lazy::new(|| Regex::new("[-_.:a-zA-Z]").unwrap());

/// Tokenize the text without splitting on ".", "-" and "_" in numbers.
#[derive(Clone)]
pub struct LogTokenizer;

pub struct LogTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: Token,
}

impl Tokenizer for LogTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a> {
        BoxTokenStream::from(LogTokenStream {
            text,
            chars: text.char_indices(),
            token: Token::default(),
        })
    }
}

impl<'a> LogTokenStream<'a> {
    fn search_token_end(&mut self) -> usize {
        (&mut self.chars)
            .filter(|&(_, ref c)| !c.is_alphanumeric())
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or_else(|| self.text.len())
    }

    fn handle_chars_in_number(&mut self) -> usize {
        (&mut self.chars)
            .filter(|&(_, ref c)| {
                c.is_ascii_whitespace()
                    || !(c.is_alphanumeric() || VALID_CHAR_IN_NUMBER.is_match(&c.to_string()))
            })
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or_else(|| self.text.len())
    }

    fn push_token(&mut self, offset_from : usize, offset_to : usize) -> () {
        self.token.offset_from = offset_from;
        self.token.offset_to = offset_to;
        self.token.text.push_str(&self.text[offset_from..offset_to]);
    }
}

impl<'a> TokenStream for LogTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        while let Some((offset_from, c)) = self.chars.next() {
            // if the token starts with a number, it must be handled differently
            if c.is_numeric() {
                let offset_to = self.handle_chars_in_number();
                self.push_token(offset_from, offset_to);

                return true;

            } else if c.is_alphabetic() {
                let offset_to = self.search_token_end();
                self.push_token(offset_from, offset_to);

                return true;
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

fn get_quickwit_tokenizer_manager() -> TokenizerManager {
    let raw_tokenizer = TextAnalyzer::from(RawTokenizer).filter(RemoveLongFilter::limit(100));

    // TODO eventually check for other restrictions
    let log_tokenizer = TextAnalyzer::from(LogTokenizer).filter(RemoveLongFilter::limit(100));

    let tokenizer_manager = TokenizerManager::default();
    tokenizer_manager.register("raw", raw_tokenizer);
    tokenizer_manager.register("log", log_tokenizer);
    tokenizer_manager
}

/// Quickwits default tokenizer
pub static QUICKWIT_TOKENIZER_MANAGER: Lazy<TokenizerManager> =
    Lazy::new(get_quickwit_tokenizer_manager);

#[test]
fn raw_tokenizer_test() {
    let my_haiku = r#"
        white sandy beach
        a strong wind is coming
        sand in my face
        "#;
    let my_long_text = "a text, that is just too long, no one will type it, no one will like it, \
                        no one shall find it. I just need some more chars, now you may not pass.";

    let tokenizer = get_quickwit_tokenizer_manager().get("raw").unwrap();
    let mut haiku_stream = tokenizer.token_stream(my_haiku);
    assert!(haiku_stream.advance());
    assert!(!haiku_stream.advance());
    assert!(!tokenizer.token_stream(my_long_text).advance());
}

#[cfg(test)]
mod tests {
    use crate::tokenizers::get_quickwit_tokenizer_manager;
    use tantivy::tokenizer::{SimpleTokenizer, TextAnalyzer};

    #[test]
    fn log_tokenizer_basic_test() {
        let numbers = "255.255.255.255 test \n\ttest\t 27-05-2022 \t\t  \n \tat\r\n 02:51";
        let tokenizer = get_quickwit_tokenizer_manager().get("log").unwrap();
        let mut token_stream = tokenizer.token_stream(numbers);
        let array_ref: [&str; 6] = [
            "255.255.255.255",
            "test",
            "test",
            "27-05-2022",
            "at",
            "02:51",
        ];

        array_ref.iter().for_each(|ref_token| {
            if token_stream.advance() {
                assert_eq!(&token_stream.token().text, ref_token)
            } else {
                assert!(false);
            }
        });
    }

    // The only difference with the default tantivy is within numbers, this test is
    // to check if the behaviour is affected
    #[test]
    fn log_tokenizer_compare_with_simple() {
        let test_string = "this,is,the,test 42 here\n3932\t20dk,3093raopxa'wd";
        let tokenizer = get_quickwit_tokenizer_manager().get("log").unwrap();
        let ref_tokenizer = TextAnalyzer::from(SimpleTokenizer);
        let mut token_stream = tokenizer.token_stream(test_string);
        let mut ref_token_stream = ref_tokenizer.token_stream(test_string);

        while token_stream.advance() && ref_token_stream.advance() {
            assert_eq!(&token_stream.token().text, &ref_token_stream.token().text);
        }

        assert!(!(token_stream.advance() || ref_token_stream.advance()));
    }

    // The tokenizer should still be able to work on normal texts
    #[test]
    fn log_tokenizer_basic_text() {
        let test_string = r#"
        Aujourd'hui, maman est morte. Ou peut-
    être hier, je ne sais pas. J'ai reçu un télégramme de l'asile : « Mère décédée. Enterrement demain. Sentiments distingués.»
    Cela ne veut rien dire. C'était peut-être
    hier.
        "#;
        let tokenizer = get_quickwit_tokenizer_manager().get("log").unwrap();
        let ref_tokenizer = TextAnalyzer::from(SimpleTokenizer);
        let mut token_stream = tokenizer.token_stream(test_string);
        let mut ref_token_stream = ref_tokenizer.token_stream(test_string);

        while token_stream.advance() && ref_token_stream.advance() {
            assert_eq!(&token_stream.token().text, &ref_token_stream.token().text);
        }

        assert!(!(token_stream.advance() || ref_token_stream.advance()));
    }

    #[test]
    fn log_tokenizer_log_test() {
        let test_string = "Dec 10 06:55:48 LabSZ sshd[24200]: Failed password for invalid user webmaster from 173.234.31.186 port 38926 ssh2";
        let array_ref: [&str; 17] = [
            "Dec",
            "10",
            "06:55:48",
            "LabSZ",
            "sshd",
            "24200",
            "Failed",
            "password",
            "for",
            "invalid",
            "user",
            "webmaster",
            "from",
            "173.234.31.186",
            "port",
            "38926",
            "ssh2",
        ];
        let tokenizer = get_quickwit_tokenizer_manager().get("log").unwrap();
        let mut token_stream = tokenizer.token_stream(test_string);

        array_ref.iter().for_each(|ref_token| {
            if token_stream.advance() {
                assert_eq!(&token_stream.token().text, ref_token)
            } else {
                assert!(false);
            }
        });
    }

    #[test]
    fn log_tokenizer_log_test_2() {
        let test_string =
            "1331901000.000000    CHEt7z3AzG4gyCNgci    192.168.202.79    50465    192.168.229.251    80    1    HEAD 192.168.229.251    /DEASLog02.nsf    -    Mozilla/5.0";
        let array_ref: [&str; 13] = [
            "1331901000.000000",
            "CHEt7z3AzG4gyCNgci",
            "192.168.202.79",
            "50465",
            "192.168.229.251",
            "80",
            "1",
            "HEAD",
            "192.168.229.251",
            "DEASLog02",
            "nsf",
            "Mozilla",
            "5.0",
        ];
        let tokenizer = get_quickwit_tokenizer_manager().get("log").unwrap();
        let mut token_stream = tokenizer.token_stream(test_string);

        array_ref.iter().for_each(|ref_token| {
            if token_stream.advance() {
                assert_eq!(&token_stream.token().text, ref_token)
            } else {
                assert!(false);
            }
        });
    }
}
