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
use regex::Regex;
use tantivy::tokenizer::{
    BoxTokenStream, LowerCaser, RawTokenizer, RemoveLongFilter, TextAnalyzer, Token, TokenStream,
    Tokenizer, TokenizerManager,
};

fn get_quickwit_tokenizer_manager() -> TokenizerManager {
    let raw_tokenizer = TextAnalyzer::from(RawTokenizer).filter(RemoveLongFilter::limit(100));
    let log_tokenizer = TextAnalyzer::from(LogTokenizer).filter(RemoveLongFilter::limit(100));
    let chinese_tokenizer = TextAnalyzer::from(ChineseTokenizer)
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser);

    let tokenizer_manager = TokenizerManager::default();

    tokenizer_manager.register("raw", raw_tokenizer);
    tokenizer_manager.register("chinese_compatible", chinese_tokenizer);
    tokenizer_manager.register("log", log_tokenizer);

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

static REGEX_ERROR_MSG: &str = "Failed to compile regular expression. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.";

// Regex array ordered by the most frequent pattern encountered in logs.
// If an expression appears a lot, it should be place at the smallest index possible
// to avoid iterating through the array as much as possible.
static IDENTIFIER_REGEX: Lazy<Regex> = Lazy::new(|| {
    // Regex to match identifiers: IP, URI, UUID, Dates...
    Regex::new(
        r"(?xi)             # Multiline regex that ignores case.
    ^
    ([a-z0-9]+://)?             # Optional scheme: https, file, s3,...
    [a-z0-9]+                   # Identifier starts with an alphanumeric...
    [-/%_\\.:]                  # And must be followed by a special character to form an ID.
    [-/%_\\.:$@,a-z0-9]+        # Authorized identifier characters. 
    [/a-z0-9]                   # Identifier must end with an alphanumeric.
    ",
    )
    .expect(REGEX_ERROR_MSG)
});

/// Log friendly tokenizer that avoids splittings on ponctuation in:
/// - IP addresses (both ipv4 and ipv6).
/// - Common characters found in identifiers (".", "-", alphanumeric characters...).
/// - Date-time formats (some examples): + ISO 8601. + Any combination of d, m and y seperated by
///   '.', '-', ':', '_' and '/'. + Any combination of h, m and s seperated by '.', '-', ':', '_'
///   and '/'. + MMM d yyyy. + ...
/// - URIs such as URL and filepath.
#[derive(Clone)]
pub struct LogTokenizer;

#[allow(missing_docs)]
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
            .filter(|(_, ref character)| !character.is_alphanumeric())
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or(self.text.len())
    }

    fn handle_match(&mut self, offset_to: usize) -> usize {
        (&mut self.chars)
            .filter(|(index, _)| *index == offset_to)
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or(self.text.len())
    }

    fn push_token(&mut self, offset_from: usize, offset_to: usize) {
        self.token.offset_from = offset_from;
        self.token.offset_to = offset_to;
        self.token.text.push_str(&self.text[offset_from..offset_to]);
    }
}

impl<'a> TokenStream for LogTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);

        while let Some((offset_from, current_character)) = self.chars.next() {
            let text_substring = &self.text[offset_from..];

            // Tries first to find a matching regex. If found, advances the iterator to the
            // start of the next token and push the token in the stream.
            if let Some(regex_match) = IDENTIFIER_REGEX.find(text_substring) {
                let offset_to = self.handle_match(offset_from + regex_match.end());
                self.push_token(offset_from, offset_to);
                return true;
            }

            // When no regex is match, falls back to the simple tokenizer that splits on non
            // alphanumeric characters.
            if current_character.is_alphanumeric() {
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

/// Quickwits default tokenizer
pub static QUICKWIT_TOKENIZER_MANAGER: Lazy<TokenizerManager> =
    Lazy::new(get_quickwit_tokenizer_manager);

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::{SimpleTokenizer, TextAnalyzer, Token};

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

        let tokenizer = get_quickwit_tokenizer_manager()
            .get("chinese_compatible")
            .unwrap();
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

        let tokenizer = get_quickwit_tokenizer_manager()
            .get("chinese_compatible")
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

        assert_eq!(dbg!(res), dbg!(expected));
    }

    proptest::proptest! {
        #[test]
        fn test_proptest_ascii_default_chinese_equal(text in "[ -~]{0,64}") {
            let cn_tok = get_quickwit_tokenizer_manager().get("chinese_compatible").unwrap();
            let default_tok = get_quickwit_tokenizer_manager().get("default").unwrap();

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

    // Compare expected tokens in array_ref with actual tokens in stream from test_string
    fn log_tokenizer_test_helper(test_string: &str, array_ref: &[&str]) {
        let mut token_stream = get_quickwit_tokenizer_manager()
            .get("log")
            .unwrap()
            .token_stream(test_string);

        array_ref.iter().for_each(|ref_token| {
            if token_stream.advance() {
                assert_eq!(&token_stream.token().text, ref_token)
            } else {
                panic!()
            }
        });
    }

    #[test]
    fn log_tokenizer_basic_test() {
        let test_string =
            "255.255.255.255 test \n\ttest\t 27-05-2022 \t\t  \n \tat\r\n 02:51\n\nJul-10 -";
        let array_ref: [&str; 7] = [
            "255.255.255.255",
            "test",
            "test",
            "27-05-2022",
            "at",
            "02:51",
            "Jul-10",
        ];

        log_tokenizer_test_helper(test_string, &array_ref)
    }

    // The only difference with the default tantivy is within numbers, this test is
    // to check if the behaviour is affected
    #[test]
    fn log_tokenizer_compare_with_simple() {
        let test_string = "this,is,the,test 42 here\n3932\t20dk,3093raopxa'wd";
        let mut token_stream = get_quickwit_tokenizer_manager()
            .get("log")
            .unwrap()
            .token_stream(test_string);
        let mut ref_token_stream = TextAnalyzer::from(SimpleTokenizer).token_stream(test_string);

        while token_stream.advance() && ref_token_stream.advance() {
            assert_eq!(&token_stream.token().text, &ref_token_stream.token().text);
        }

        assert!(!(token_stream.advance() || ref_token_stream.advance()));
    }

    // The tokenizer should still be able to work on normal texts
    #[test]
    fn log_tokenizer_basic_text() {
        let test_string = r#"
        Aujourd'hui, maman est morte. Ou peut
    être hier, je ne sais pas. J'ai reçu un télégramme de l'asile : « Mère décédée. Enterrement demain. Sentiments distingués.»
    Cela ne veut rien dire. C'était peut être
    hier.
        "#;

        let mut token_stream = get_quickwit_tokenizer_manager()
            .get("log")
            .unwrap()
            .token_stream(test_string);
        let mut ref_token_stream = TextAnalyzer::from(SimpleTokenizer).token_stream(test_string);

        while token_stream.advance() && ref_token_stream.advance() {
            assert_eq!(&token_stream.token().text, &ref_token_stream.token().text);
        }

        assert!(!(token_stream.advance() || ref_token_stream.advance()));
    }

    #[test]
    fn log_tokenizer_log_2() {
        let test_string = "1331901000.000000    CHEt7z3AzG4gyCNgci    192.168.202.79    50465    \
                           192.168.229.251    80    1    HEAD 192.168.229.251    /DEASLog02.nsf    \
                           -    Mozilla/5.0";

        let array_ref: [&str; 11] = [
            "1331901000.000000",
            "CHEt7z3AzG4gyCNgci",
            "192.168.202.79",
            "50465",
            "192.168.229.251",
            "80",
            "1",
            "HEAD",
            "192.168.229.251",
            "DEASLog02.nsf",
            "Mozilla/5.0",
        ];

        log_tokenizer_test_helper(test_string, &array_ref)
    }

    #[test]
    fn log_tokenizer_log_test_http() {
        let test_string = "{\"message\" : \"211.11.9.0 - - [1998-06-21T15:00:01-05:00] \"GET \
                           /english/index.html HTTP/1.0\" 304 0\"}";

        let array_ref: [&str; 8] = [
            "message",
            "211.11.9.0",
            "1998-06-21T15:00:01-05:00",
            "GET",
            "english/index.html",
            "HTTP/1.0",
            "304",
            "0",
        ];

        log_tokenizer_test_helper(test_string, &array_ref)
    }

    #[test]
    fn log_tokenizer_ip_test() {
        let test_string = r"255.255.255.255
            0f31:e019:5e74:6679:3134:99f1:8f55:fa2a
            e6c5:5182:b404:7e64:d91f:ba40:bfb7:c184
            12.32.75.221
            ";

        let array_ref: [&str; 4] = [
            "255.255.255.255",
            "0f31:e019:5e74:6679:3134:99f1:8f55:fa2a",
            "e6c5:5182:b404:7e64:d91f:ba40:bfb7:c184",
            "12.32.75.221",
        ];

        log_tokenizer_test_helper(test_string, &array_ref)
    }

    #[test]
    fn log_tokenizer_paths_test() {
        let test_string = r"./quickwit/quickwit-doc-mapper/src/tokenizers.rs
            /endpoint/index.html
            /bin/sh src/bin/ test_files.cc 

            .././folder/_trying-stuff_out.cc  

            peut-etre.out ";

        let array_ref: [&str; 7] = [
            "quickwit/quickwit-doc-mapper/src/tokenizers.rs",
            "endpoint/index.html",
            "bin/sh",
            "src/bin/",
            "test_files.cc",
            "folder/_trying-stuff_out.cc",
            "peut-etre.out",
        ];

        log_tokenizer_test_helper(test_string, &array_ref)
    }

    #[test]
    fn log_tokenizer_log_wsa() {
        let test_string = "54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] \"GET /filter/27 HTTP/1.1\" 200 30577 \"-\" \"Mozilla/5.0 (compatible; AhrefsBot/6.1; +http://ahrefs.com/robot/)\" \"-\"";

        let array_ref: [&str; 12] = [
            "54.36.149.41",
            "22/Jan/2019:03:56:14",
            "0330",
            "GET",
            "filter/27",
            "HTTP/1.1",
            "200",
            "30577",
            "Mozilla/5.0",
            "compatible",
            "AhrefsBot/6.1",
            "http://ahrefs.com/robot/",
        ];

        log_tokenizer_test_helper(test_string, &array_ref)
    }

    #[test]
    fn log_tokenizer_links_test() {
        let test_string = r"
        www.google.com
        https://stackoverflow.com/
        https://quickwit.io/docs/get-started/installation
        http://www.domain.com/url?variable=value&variable=value
        ";

        let array_ref: [&str; 8] = [
            "www.google.com",
            "https://stackoverflow.com/",
            "https://quickwit.io/docs/get-started/installation",
            "http://www.domain.com/url",
            "variable",
            "value",
            "variable",
            "value",
        ];

        log_tokenizer_test_helper(test_string, &array_ref)
    }

    #[test]
    fn test_log_proxifier() {
        // Source: https://github.com/logpai/loghub/blob/master/Proxifier/Proxifier_2k.log.
        let test_string = r"[10.30 16:49:06] chrome.exe - proxy.cse.cuhk.edu.hk:5070 open through proxy proxy.cse.cuhk.edu.hk:5070 HTTPS";
        let array_ref: [&str; 9] = [
            "10.30",
            "16:49:06",
            "chrome.exe",
            "proxy.cse.cuhk.edu.hk:5070",
            "open",
            "through",
            "proxy",
            "proxy.cse.cuhk.edu.hk:5070",
            "HTTPS",
        ];

        log_tokenizer_test_helper(test_string, &array_ref)
    }

    #[test]
    fn test_log_zookeeper() {
        // Source: https://github.com/logpai/loghub/blob/master/Zookeeper/Zookeeper_2k.log.
        let test_string = r"2015-07-29 19:04:12,394 - INFO  [/10.10.34.11:3888:QuorumCnxManager$Listener@493] - Received connection request /10.10.34.11:45307";
        let array_ref: [&str; 8] = [
            "2015-07-29",
            "19:04:12,394",
            "INFO",
            "10.10.34.11:3888:QuorumCnxManager$Listener@493",
            "Received",
            "connection",
            "request",
            "10.10.34.11:45307",
        ];

        log_tokenizer_test_helper(test_string, &array_ref)
    }

    #[test]
    fn test_log_hadoop() {
        // Source: https://github.com/logpai/loghub/blob/master/Hadoop/Hadoop_2k.log.
        let test_string = r"2015-10-18 18:01:50,556 INFO [main] org.apache.hadoop.yarn.event.AsyncDispatcher: Registering class org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType for class org.apache.hadoop.mapreduce.v2.app.MRAppMaster$TaskAttemptEventDispatcher";
        let array_ref: [&str; 11] = [
            "2015-10-18",
            "18:01:50,556",
            "INFO",
            "main",
            "org.apache.hadoop.yarn.event.AsyncDispatcher",
            "Registering",
            "class",
            "org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType",
            "for",
            "class",
            "org.apache.hadoop.mapreduce.v2.app.MRAppMaster$TaskAttemptEventDispatcher",
        ];

        log_tokenizer_test_helper(test_string, &array_ref)
    }
}
