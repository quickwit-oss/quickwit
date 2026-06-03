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

use anyhow::bail;

/// Hard cap on the byte length of a single bare token. Bounds memory per
/// request when a client (accidentally or hostilely) sends a giant token.
pub(crate) const MAX_BARE_TOKEN_LEN: usize = 1024;

/// Hard cap on the byte length of a single quoted phrase. Same rationale as
/// `MAX_BARE_TOKEN_LEN` but for double-quoted content.
pub(crate) const MAX_PHRASE_LEN: usize = 4096;

/// Lexical token emitted by the KQL tokenizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Token {
    LParen,
    RParen,
    Colon,
    Gt,
    Gte,
    Lt,
    Lte,
    KwAnd,
    KwOr,
    KwNot,
    /// A double-quoted phrase. The string holds the unescaped contents.
    Phrase(String),
    /// An unquoted bare term. The string holds the unescaped contents and may
    /// contain `*` or `?` wildcards.
    Bare(String),
}

impl Token {
    pub(crate) fn describe(&self) -> &'static str {
        match self {
            Token::LParen => "'('",
            Token::RParen => "')'",
            Token::Colon => "':'",
            Token::Gt => "'>'",
            Token::Gte => "'>='",
            Token::Lt => "'<'",
            Token::Lte => "'<='",
            Token::KwAnd => "'and'",
            Token::KwOr => "'or'",
            Token::KwNot => "'not'",
            Token::Phrase(_) => "phrase",
            Token::Bare(_) => "term",
        }
    }
}

/// Tokenize a KQL input string into a flat vector of tokens.
pub(crate) fn tokenize(input: &str) -> anyhow::Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut iter = input.char_indices().peekable();
    while let Some(&(_, ch)) = iter.peek() {
        if ch.is_whitespace() {
            iter.next();
            continue;
        }
        match ch {
            '(' => {
                iter.next();
                tokens.push(Token::LParen);
            }
            ')' => {
                iter.next();
                tokens.push(Token::RParen);
            }
            ':' => {
                iter.next();
                tokens.push(Token::Colon);
            }
            '{' | '}' => {
                bail!(
                    "unsupported character {ch:?} in KQL — `{{...}}` nested-field syntax is not \
                     supported (Quickwit has no nested-field type); use flat dotted paths like \
                     `nested.field:value` instead"
                );
            }
            '>' => {
                iter.next();
                if let Some(&(_, '=')) = iter.peek() {
                    iter.next();
                    tokens.push(Token::Gte);
                } else {
                    tokens.push(Token::Gt);
                }
            }
            '<' => {
                iter.next();
                if let Some(&(_, '=')) = iter.peek() {
                    iter.next();
                    tokens.push(Token::Lte);
                } else {
                    tokens.push(Token::Lt);
                }
            }
            '"' => {
                iter.next();
                let mut phrase = String::new();
                let mut closed = false;
                while let Some((_, c)) = iter.next() {
                    if c == '\\' {
                        match iter.next() {
                            Some((_, esc)) => phrase.push(esc),
                            None => bail!("unterminated escape sequence inside phrase"),
                        }
                    } else if c == '"' {
                        closed = true;
                        break;
                    } else {
                        phrase.push(c);
                    }
                    if phrase.len() > MAX_PHRASE_LEN {
                        bail!("KQL phrase exceeds maximum length of {MAX_PHRASE_LEN} bytes");
                    }
                }
                if !closed {
                    bail!("unterminated quoted phrase");
                }
                tokens.push(Token::Phrase(phrase));
            }
            _ => {
                let mut bare = String::new();
                let mut had_escape = false;
                while let Some(&(_, c)) = iter.peek() {
                    if c == '\\' {
                        iter.next();
                        match iter.next() {
                            Some((_, esc)) => {
                                bare.push(esc);
                                had_escape = true;
                            }
                            None => bail!("trailing backslash in input"),
                        }
                    } else if c.is_whitespace()
                        || matches!(c, '(' | ')' | ':' | '<' | '>' | '"' | '{' | '}')
                    {
                        // `{` and `}` are not part of KQL's bare-token alphabet —
                        // Kibana uses them for nested-field object syntax which we
                        // do not support. Stopping the bare-token scan here turns
                        // `field:{...}` into a clean parse rejection at the
                        // surrounding parser rather than silently accepting `{`
                        // as part of the value.
                        break;
                    } else {
                        bare.push(c);
                        iter.next();
                    }
                    if bare.len() > MAX_BARE_TOKEN_LEN {
                        bail!("KQL term exceeds maximum length of {MAX_BARE_TOKEN_LEN} bytes");
                    }
                }
                if bare.is_empty() {
                    bail!("unexpected character: {ch:?}");
                }
                // A token containing any backslash escape preserves its
                // literal form — the user clearly intends `\and` as a field
                // name, not the boolean keyword.
                tokens.push(if had_escape {
                    Token::Bare(bare)
                } else {
                    classify_bare(bare)
                });
            }
        }
    }
    Ok(tokens)
}

fn classify_bare(bare: String) -> Token {
    if bare.eq_ignore_ascii_case("and") {
        Token::KwAnd
    } else if bare.eq_ignore_ascii_case("or") {
        Token::KwOr
    } else if bare.eq_ignore_ascii_case("not") {
        Token::KwNot
    } else {
        Token::Bare(bare)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(input: &str) -> Vec<Token> {
        tokenize(input).unwrap()
    }

    #[test]
    fn test_simple_field_value() {
        assert_eq!(
            tok("level:error"),
            vec![
                Token::Bare("level".into()),
                Token::Colon,
                Token::Bare("error".into()),
            ]
        );
    }

    #[test]
    fn test_boolean_keywords_are_case_insensitive() {
        assert_eq!(
            tok("a AND b"),
            vec![
                Token::Bare("a".into()),
                Token::KwAnd,
                Token::Bare("b".into()),
            ]
        );
        assert_eq!(
            tok("a Or b"),
            vec![
                Token::Bare("a".into()),
                Token::KwOr,
                Token::Bare("b".into()),
            ]
        );
        assert_eq!(tok("NOT a"), vec![Token::KwNot, Token::Bare("a".into())]);
    }

    #[test]
    fn test_keywords_can_be_quoted_to_be_literal() {
        assert_eq!(
            tok("\"and\""),
            vec![Token::Phrase("and".into())],
            "quoted keywords are literal"
        );
    }

    #[test]
    fn test_range_operators() {
        assert_eq!(
            tok("size:>=10"),
            vec![
                Token::Bare("size".into()),
                Token::Colon,
                Token::Gte,
                Token::Bare("10".into()),
            ]
        );
        assert_eq!(
            tok("size:<10"),
            vec![
                Token::Bare("size".into()),
                Token::Colon,
                Token::Lt,
                Token::Bare("10".into()),
            ]
        );
    }

    #[test]
    fn test_phrase_with_escape() {
        assert_eq!(
            tok(r#""he said \"hi\"""#),
            vec![Token::Phrase("he said \"hi\"".into())]
        );
    }

    #[test]
    fn test_dotted_field_name() {
        assert_eq!(
            tok("nested.field:value"),
            vec![
                Token::Bare("nested.field".into()),
                Token::Colon,
                Token::Bare("value".into()),
            ]
        );
    }

    #[test]
    fn test_wildcard_in_bare_token() {
        assert_eq!(
            tok("status:err*"),
            vec![
                Token::Bare("status".into()),
                Token::Colon,
                Token::Bare("err*".into()),
            ]
        );
    }

    #[test]
    fn test_escaped_special_in_bare_token() {
        assert_eq!(
            tok(r"value\:with\(parens"),
            vec![Token::Bare("value:with(parens".into())]
        );
    }

    #[test]
    fn test_unterminated_phrase_errors() {
        assert!(tokenize("\"unterminated").is_err());
    }

    #[test]
    fn test_empty_quoted_phrase() {
        assert_eq!(tok(r#""""#), vec![Token::Phrase(String::new())]);
    }

    #[test]
    fn test_escaped_backslash_in_phrase() {
        // `\\` inside a phrase yields a single literal backslash.
        assert_eq!(tok(r#""a\\b""#), vec![Token::Phrase("a\\b".into())]);
    }

    #[test]
    fn test_escaped_quote_in_bare_token() {
        // `\"` inside a bare token yields a literal `"`. Unlikely in practice,
        // but the escape rule should be symmetric with phrase contents.
        assert_eq!(
            tok(r#"value\"weird"#),
            vec![Token::Bare("value\"weird".into())]
        );
    }

    #[test]
    fn test_trailing_backslash_errors() {
        assert!(tokenize("abc\\").is_err());
    }

    #[test]
    fn test_trailing_backslash_inside_phrase_errors() {
        assert!(tokenize("\"abc\\").is_err());
    }

    #[test]
    fn test_unicode_field_and_value() {
        // The lexer is byte-agnostic for non-special chars; CJK / accented
        // characters round-trip through Bare tokens unchanged.
        assert_eq!(
            tok("café:naïve"),
            vec![
                Token::Bare("café".into()),
                Token::Colon,
                Token::Bare("naïve".into()),
            ]
        );
    }

    #[test]
    fn test_lexer_rejects_oversize_bare_token() {
        let oversize = "a".repeat(MAX_BARE_TOKEN_LEN + 1);
        let err = tokenize(&oversize).expect_err("oversize bare token must be rejected");
        assert!(err.to_string().contains("maximum length"));
    }

    #[test]
    fn test_lexer_rejects_oversize_phrase() {
        let oversize = format!("\"{}\"", "a".repeat(MAX_PHRASE_LEN + 1));
        let err = tokenize(&oversize).expect_err("oversize phrase must be rejected");
        assert!(err.to_string().contains("maximum length"));
    }

    #[test]
    fn test_token_describe_covers_every_variant() {
        // `describe()` only fires via error messages — directly assert each
        // variant returns a non-empty label so no future variant ships
        // without a description.
        assert_eq!(Token::LParen.describe(), "'('");
        assert_eq!(Token::RParen.describe(), "')'");
        assert_eq!(Token::Colon.describe(), "':'");
        assert_eq!(Token::Gt.describe(), "'>'");
        assert_eq!(Token::Gte.describe(), "'>='");
        assert_eq!(Token::Lt.describe(), "'<'");
        assert_eq!(Token::Lte.describe(), "'<='");
        assert_eq!(Token::KwAnd.describe(), "'and'");
        assert_eq!(Token::KwOr.describe(), "'or'");
        assert_eq!(Token::KwNot.describe(), "'not'");
        assert_eq!(Token::Phrase(String::new()).describe(), "phrase");
        assert_eq!(Token::Bare(String::new()).describe(), "term");
    }

    #[test]
    fn test_keyword_inside_phrase_is_literal() {
        // Quoting prevents keyword classification.
        assert_eq!(tok(r#""AND""#), vec![Token::Phrase("AND".into())]);
    }
}
