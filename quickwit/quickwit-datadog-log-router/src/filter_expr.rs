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

use core::fmt;
use std::fmt::Display;
use std::str::FromStr;

/// The AST for filter expressions.
#[derive(Debug, Clone)]
pub enum FilterExpr {
    /// Matches everything
    All,
    /// Matches nothing
    Never,
    /// A key:value match with optional wildcards
    Match { key: Key, pattern: StringPattern },
    /// Logical AND of expressions
    And(Vec<FilterExpr>),
    /// Logical OR of expressions
    Or(Vec<FilterExpr>),
    /// Logical NOT of an expression
    Not(Box<FilterExpr>),
}

#[derive(Debug, Clone)]
pub enum Key {
    Tag(String),
    CustomField(Vec<String>),
}

/// A pattern for matching string values.
#[derive(Debug, Clone)]
pub enum StringPattern {
    /// Any string
    Any,
    /// Exact string match
    Exact(String),
    /// Prefix match (value*)
    Prefix(String),
    /// Suffix match (*value)
    Suffix(String),
    /// Prefix and suffix match (prefix*suffix)
    PrefixAndSuffix {
        prefix: String,
        suffix: String,
    },
    /// Contains the string (*value*)
    Contains(String),

    Or(Vec<StringPattern>),
    And(Vec<StringPattern>),
    Not(Box<StringPattern>),
}

impl FromStr for FilterExpr {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        expression_dsl::parse(input)
            .map_err(|e| anyhow::anyhow!("Parse error: {:?} at '{}'", e.code, e.input))
    }
}

impl Display for FilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            FilterExpr::Never => write!(f, "")?,
            FilterExpr::All => write!(f, "*")?,
            FilterExpr::Match { key, pattern } => write!(f, "{}:{}", key, pattern)?,
            FilterExpr::And(exprs) => {
                let expr_strs: Vec<String> = exprs.iter().map(|e| e.to_string()).collect();
                write!(f, "({})", expr_strs.join(" AND "))?;
            }
            FilterExpr::Or(exprs) => {
                let expr_strs: Vec<String> = exprs.iter().map(|e| e.to_string()).collect();
                write!(f, "({})", expr_strs.join(" OR "))?;
            }
            FilterExpr::Not(inner) => write!(f, "NOT ({})", inner)?,
        }
        Ok(())
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Key::Tag(s) => write!(f, "{}", s)?,
            Key::CustomField(path) => write!(f, "@{}", path.join("."))?,
        }
        Ok(())
    }
}

impl Display for StringPattern {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            StringPattern::Any => write!(f, "*")?,
            StringPattern::Exact(s) => write!(f, "{}", s)?,
            StringPattern::Prefix(prefix) => write!(f, "{}*", prefix)?,
            StringPattern::Suffix(suffix) => write!(f, "*{}", suffix)?,
            StringPattern::PrefixAndSuffix { prefix, suffix } => {
                write!(f, "{}*{}", prefix, suffix)?
            }
            StringPattern::Contains(s) => write!(f, "*{}*", s)?,
            StringPattern::Or(patterns) => {
                let pattern_strs: Vec<String> = patterns.iter().map(|e| e.to_string()).collect();
                write!(f, "({})", pattern_strs.join(" OR "))?;
            }
            StringPattern::And(patterns) => {
                let pattern_strs: Vec<String> = patterns.iter().map(|e| e.to_string()).collect();
                write!(f, "({})", pattern_strs.join(" AND "))?;
            }
            StringPattern::Not(inner) => write!(f, "NOT ({})", inner)?,
        }
        Ok(())
    }
}

mod expression_dsl {
    // This grammar implements a compatible subset of the Datadog Events Platform Syntax.
    // Reference: https://github.com/DataDog/web-ui/blob/preprod/packages/lib/events-syntax/cst/events-syntax.pegjs
    //
    // Not supported (intentionally):
    // - Full-text search (queries without field:)
    // - Range expressions: [a TO b], {a TO b}, >=, <=, >, <
    // - CIDR function: CIDR(field, ip/mask)
    // - Timeline expressions: [| ... |]
    // - Single-character wildcard: ?
    //
    // Grammar (EBNF-like):
    //
    // FilterExpr    := OrExpr
    // OrExpr        := AndExpr [ ('OR' | '||') OrExpr ]
    // AndExpr       := NotExpr [ ('AND' | '&&') AndExpr ]
    // NotExpr       := 'NOT' Primary | '-' Primary | '!' Primary | Primary
    // Primary       := '(' OrExpr ')' | Match
    // Match         := Key ':' Pattern
    //
    // Key           := KeyChar { KeyChar }
    // KeyChar       := { any char except : ( ) * - whitespace, or escaped }
    //
    // Pattern       := '(' PatternOr ')' | TerminalPattern
    // PatternOr     := PatternAnd [ ('OR' | '||') PatternOr ]
    // PatternAnd    := PatternNot [ ('AND' | '&&') PatternAnd ]
    // PatternNot    := 'NOT' PatternPrimary | '-' PatternPrimary | '!' PatternPrimary |
    // PatternPrimary PatternPrimary := '(' PatternOr ')' | TerminalPattern
    //
    // TerminalPattern := QuotedString | '*' ValueString '*' | ValueString '*' ValueString
    //                  | '*' ValueString | ValueString '*' | '*' | ValueString
    // QuotedString  := '"' { any char except " \, or \" \\ } '"'
    // ValueString   := ValueChar { ValueChar }
    // ValueChar     := { any char except ( ) * - whitespace, or escaped }
    //
    // Escaping:
    // - Keys/Values: shell-style, backslash escapes the next character literally
    // - Quoted strings: only \" and \\ are escape sequences
    //
    // Notes:
    // - 'AND', 'OR', 'NOT' are case-insensitive
    // - Operators require whitespace or parentheses as delimiters
    // - Implicit AND: "a:1 b:2" is equivalent to "a:1 AND b:2"
    // - Precedence: NOT > AND > OR

    use nom::branch::alt;
    use nom::bytes::complete::{escaped_transform, is_not, tag, tag_no_case, take};
    use nom::character::complete::{multispace0, multispace1};
    use nom::combinator::{all_consuming, consumed, map, not, peek, verify};
    use nom::error::Error;
    use nom::sequence::{delimited, preceded, terminated};
    use nom::{Finish, IResult, Parser};

    use super::*;

    /// Wraps a parser with parentheses, allowing optional whitespace inside
    fn wrap_with_paren<'a, O, P>(
        parser: P,
    ) -> impl Parser<&'a str, Output = O, Error = Error<&'a str>>
    where P: Parser<&'a str, Output = O, Error = Error<&'a str>> {
        delimited((tag("("), multispace0), parser, (multispace0, tag(")")))
    }

    /// Wraps a parser and verifies that the consumed input ends with ')' or whitespace
    fn ends_with_paren_or_space<'a, O, P>(
        parser: P,
    ) -> impl Parser<&'a str, Output = O, Error = Error<&'a str>>
    where P: Parser<&'a str, Output = O, Error = Error<&'a str>> {
        map(
            verify(
                consumed(terminated(parser, multispace0)),
                |(consumed_str, _): &(&str, _)| {
                    consumed_str.ends_with(')')
                        || consumed_str.ends_with(|c: char| c.is_whitespace())
                },
            ),
            |(_, output)| output,
        )
    }

    /// Wraps a parser and verifies that the consumed input starts with '(' or whitespace
    fn starts_with_paren_or_space<'a, O, P>(
        parser: P,
    ) -> impl Parser<&'a str, Output = O, Error = Error<&'a str>>
    where P: Parser<&'a str, Output = O, Error = Error<&'a str>> {
        map(
            verify(
                consumed(preceded(multispace0, parser)),
                |(consumed_str, _): &(&str, _)| {
                    consumed_str.starts_with('(')
                        || consumed_str.starts_with(|c: char| c.is_whitespace())
                },
            ),
            |(_, output)| output,
        )
    }

    /// Parses a value string (right side of key:value).
    /// Stops at unescaped `(`, `)`, `*`, or whitespace.
    /// Dash `-` is allowed in values (e.g., `service:my-service`).
    /// Backslash escapes the next character literally (shell-style).
    fn parse_value_string(input: &str) -> IResult<&str, String> {
        escaped_transform(
            is_not("\\()* \t\n\r"),
            '\\',
            alt((
                map(tag("("), |_| "("),
                map(tag(")"), |_| ")"),
                map(tag("*"), |_| "*"),
                map(tag("\\"), |_| "\\"),
                map(tag(" "), |_| " "),
            )),
        )
        .parse(input)
    }

    /// Parses a quoted string "..." or <curly quotes>...<curly quotes>
    /// Inside quotes, only \" and \\ are escape sequences.
    /// Wildcards (*) are treated as literal characters.
    /// Supports straight quotes (") and curly quotes (" ") for copy-paste friendliness.
    fn parse_quoted_string(input: &str) -> IResult<&str, String> {
        delimited(
            alt((tag("\""), tag("\u{201C}"), tag("\u{201D}"))),
            alt((
                escaped_transform(
                    is_not("\\\"\u{201C}\u{201D}"),
                    '\\',
                    alt((
                        map(tag("\""), |_| "\""),
                        map(tag("\u{201C}"), |_| "\u{201C}"),
                        map(tag("\u{201D}"), |_| "\u{201D}"),
                        map(tag("\\"), |_| "\\"),
                    )),
                ),
                // Handle empty string case
                map(tag(""), |_| String::new()),
            )),
            alt((tag("\""), tag("\u{201C}"), tag("\u{201D}"))),
        )
        .parse(input)
    }

    /// Parses a key string with shell-style escaping.
    /// Stops at unescaped `:`, `(`, `)`, `*`, or whitespace.
    /// Backslash escapes the next character literally.
    fn parse_key_string(input: &str) -> IResult<&str, String> {
        escaped_transform(
            is_not("\\:()* \t\n\r"),
            '\\',
            alt((
                map(tag(":"), |_| ":"),
                map(tag("("), |_| "("),
                map(tag(")"), |_| ")"),
                map(tag("*"), |_| "*"),
                map(tag("\\"), |_| "\\"),
                map(tag("-"), |_| "-"),
                map(tag(" "), |_| " "),
                take(1usize), // Unknown escapes pass through (e.g., \/ -> /)
            )),
        )
        .parse(input)
    }

    /// Parses a single custom field path component with shell-style escaping.
    /// Stops at unescaped `.`, `:`, `(`, `)`, `*`, or whitespace.
    /// Backslash escapes the next character literally, including dots.
    fn parse_custom_field_component(input: &str) -> IResult<&str, String> {
        escaped_transform(
            is_not("\\.:()* \t\n\r"),
            '\\',
            alt((
                map(tag("."), |_| "."),
                map(tag(":"), |_| ":"),
                map(tag("("), |_| "("),
                map(tag(")"), |_| ")"),
                map(tag("*"), |_| "*"),
                map(tag("\\"), |_| "\\"),
                map(tag("-"), |_| "-"),
                map(tag(" "), |_| " "),
                take(1usize), // Unknown escapes pass through
            )),
        )
        .parse(input)
    }

    /// Parses a custom field path (after the `@`), split on unescaped dots.
    /// Example: `foo.bar.baz` -> vec!["foo", "bar", "baz"]
    /// Example: `foo\.bar.baz` -> vec!["foo.bar", "baz"]
    fn parse_custom_field_path(input: &str) -> IResult<&str, Vec<String>> {
        use nom::multi::separated_list1;
        separated_list1(tag("."), parse_custom_field_component).parse(input)
    }

    /// Parses a key (left side of key:value).
    /// - Cannot start with unescaped `-`, since `-` at start means NOT
    /// - If prefixed with `@`, it's a custom field key with path split on dots
    /// - Stops at unescaped `:`, `(`, `)`, `*`, or whitespace
    /// - Dash `-` is allowed after the first character (e.g., `my-key:value`)
    /// - Backslash escapes the next character literally (shell-style), including `\-` at start
    fn parse_key(input: &str) -> IResult<&str, Key> {
        preceded(
            not(peek(tag("-"))),
            alt((
                map(
                    preceded(tag("@"), parse_custom_field_path),
                    Key::CustomField,
                ),
                map(parse_key_string, Key::Tag),
            )),
        )
        .parse(input)
    }

    fn parse_terminal_string_pattern(input: &str) -> IResult<&str, StringPattern> {
        alt((
            // Quoted string - exact match, wildcards inactive
            map(parse_quoted_string, StringPattern::Exact),
            // *middle*
            map(
                delimited(tag("*"), parse_value_string, tag("*")),
                StringPattern::Contains,
            ),
            // prefix*suffix
            map(
                verify(
                    (parse_value_string, tag("*"), parse_value_string),
                    |(prefix, _, suffix)| !suffix.is_empty() && !prefix.is_empty(),
                ),
                |(prefix, _, suffix)| StringPattern::PrefixAndSuffix { prefix, suffix },
            ),
            // *suffix
            map(
                preceded(tag("*"), parse_value_string),
                StringPattern::Suffix,
            ),
            // prefix*
            map(
                terminated(parse_value_string, tag("*")),
                StringPattern::Prefix,
            ),
            // just *
            map(tag("*"), |_| StringPattern::Any),
            // exact
            map(parse_value_string, StringPattern::Exact),
        ))
        .parse(input)
    }

    fn parse_string_pattern_primary(input: &str) -> IResult<&str, StringPattern> {
        alt((
            wrap_with_paren(parse_string_pattern_or),
            parse_terminal_string_pattern,
        ))
        .parse(input)
    }

    fn parse_string_pattern_not(input: &str) -> IResult<&str, StringPattern> {
        alt((
            // NOT keyword (requires parentheses or space after)
            map(
                preceded(
                    tag_no_case("not"),
                    starts_with_paren_or_space(parse_string_pattern_primary),
                ),
                |pattern| StringPattern::Not(Box::new(pattern)),
            ),
            // `-` and `!` prefix operators
            map(
                preceded(
                    alt((tag("-"), tag("!"))),
                    preceded(multispace0, parse_string_pattern_primary),
                ),
                |pattern| StringPattern::Not(Box::new(pattern)),
            ),
            parse_string_pattern_primary,
        ))
        .parse(input)
    }

    fn parse_string_pattern_and(input: &str) -> IResult<&str, StringPattern> {
        alt((
            // "AND" keyword or "&&" operator
            map(
                (
                    ends_with_paren_or_space(parse_string_pattern_not),
                    alt((tag_no_case("and"), tag("&&"))),
                    starts_with_paren_or_space(parse_string_pattern_and),
                ),
                |(pattern_a, _, pattern_b)| StringPattern::And(vec![pattern_a, pattern_b]),
            ),
            parse_string_pattern_not,
        ))
        .parse(input)
    }

    fn parse_string_pattern_or(input: &str) -> IResult<&str, StringPattern> {
        alt((
            // "OR" keyword or "||" operator
            map(
                (
                    ends_with_paren_or_space(parse_string_pattern_and),
                    alt((tag_no_case("or"), tag("||"))),
                    starts_with_paren_or_space(parse_string_pattern_or),
                ),
                |(pattern_a, _, pattern_b)| StringPattern::Or(vec![pattern_a, pattern_b]),
            ),
            parse_string_pattern_and,
        ))
        .parse(input)
    }

    fn parse_string_pattern(input: &str) -> IResult<&str, StringPattern> {
        alt((
            wrap_with_paren(parse_string_pattern_or),
            parse_terminal_string_pattern,
        ))
        .parse(input)
    }

    fn parse_filter_match(input: &str) -> IResult<&str, FilterExpr> {
        map(
            (parse_key, tag(":"), parse_string_pattern),
            |(key, _, pattern)| FilterExpr::Match { key, pattern },
        )
        .parse(input)
    }

    fn parse_filter_primary(input: &str) -> IResult<&str, FilterExpr> {
        alt((wrap_with_paren(parse_filter_or), parse_filter_match)).parse(input)
    }

    fn parse_filter_not(input: &str) -> IResult<&str, FilterExpr> {
        alt((
            map(
                (
                    tag_no_case("not"),
                    starts_with_paren_or_space(parse_filter_primary),
                ),
                |(_, expr)| FilterExpr::Not(Box::new(expr)),
            ),
            // `-` and `!` prefix operators (no space allowed after)
            map(
                (alt((tag("-"), tag("!"))), multispace0, parse_filter_primary),
                |(_, _, expr)| FilterExpr::Not(Box::new(expr)),
            ),
            parse_filter_primary,
        ))
        .parse(input)
    }

    fn parse_filter_and(input: &str) -> IResult<&str, FilterExpr> {
        alt((
            // Explicit "AND" keyword or "&&" operator
            map(
                (
                    ends_with_paren_or_space(parse_filter_not),
                    alt((tag_no_case("and"), tag("&&"))),
                    starts_with_paren_or_space(parse_filter_and),
                ),
                |(expr_a, _, expr_b)| FilterExpr::And(vec![expr_a, expr_b]),
            ),
            // Implicit AND: "a:1 a:2" (requires whitespace)
            map(
                (parse_filter_not, multispace1, parse_filter_and),
                |(expr_a, _, expr_b)| FilterExpr::And(vec![expr_a, expr_b]),
            ),
            parse_filter_not,
        ))
        .parse(input)
    }

    fn parse_filter_or(input: &str) -> IResult<&str, FilterExpr> {
        alt((
            // "OR" keyword or "||" operator
            map(
                (
                    ends_with_paren_or_space(parse_filter_and),
                    alt((tag_no_case("or"), tag("||"))),
                    starts_with_paren_or_space(parse_filter_or),
                ),
                |(expr_a, _, expr_b)| FilterExpr::Or(vec![expr_a, expr_b]),
            ),
            parse_filter_and,
        ))
        .parse(input)
    }

    /// Flattens nested associative operations (AND/OR) into a single level.
    ///
    /// The parser produces right-associative binary trees. This function
    /// transforms them into flat n-ary nodes for cleaner representation.
    ///
    /// ```text
    /// Before:          After:
    ///
    ///     OR             OR
    ///    /  \          / | \
    ///   a   OR   =>   a  b  c
    ///      /  \
    ///     b    c
    /// ```
    ///
    /// Same transformation applies to AND nodes.
    fn merge_associative_operations(expr: FilterExpr) -> FilterExpr {
        match expr {
            FilterExpr::Or(exprs) => {
                let children: Vec<_> = exprs
                    .into_iter()
                    .map(merge_associative_operations)
                    .flat_map(|e| {
                        if let FilterExpr::Or(children) = e {
                            children
                        } else {
                            vec![e]
                        }
                    })
                    .collect();

                FilterExpr::Or(children)
            }
            FilterExpr::And(exprs) => {
                let children: Vec<_> = exprs
                    .into_iter()
                    .map(merge_associative_operations)
                    .flat_map(|e| {
                        if let FilterExpr::And(children) = e {
                            children
                        } else {
                            vec![e]
                        }
                    })
                    .collect();

                FilterExpr::And(children)
            }
            FilterExpr::Not(expr) => FilterExpr::Not(Box::new(merge_associative_operations(*expr))),
            FilterExpr::Match { key, pattern } => FilterExpr::Match {
                key,
                pattern: merge_associative_operations_for_string_pattern(pattern),
            },
            _ => expr,
        }
    }

    /// Same as [`merge_associative_operations`] but for StringPattern AND/OR nodes.
    fn merge_associative_operations_for_string_pattern(pattern: StringPattern) -> StringPattern {
        match pattern {
            StringPattern::Or(exprs) => {
                let children: Vec<_> = exprs
                    .into_iter()
                    .map(merge_associative_operations_for_string_pattern)
                    .flat_map(|e| {
                        if let StringPattern::Or(children) = e {
                            children
                        } else {
                            vec![e]
                        }
                    })
                    .collect();

                StringPattern::Or(children)
            }
            StringPattern::And(exprs) => {
                let children: Vec<_> = exprs
                    .into_iter()
                    .map(merge_associative_operations_for_string_pattern)
                    .flat_map(|e| {
                        if let StringPattern::And(children) = e {
                            children
                        } else {
                            vec![e]
                        }
                    })
                    .collect();

                StringPattern::And(children)
            }
            StringPattern::Not(inner) => StringPattern::Not(Box::new(
                merge_associative_operations_for_string_pattern(*inner),
            )),
            _ => pattern,
        }
    }

    pub fn parse(input: &str) -> Result<FilterExpr, Error<&str>> {
        let trimmed = input.trim();
        if trimmed == "*" {
            Ok(FilterExpr::All)
        } else if trimmed.is_empty() {
            Ok(FilterExpr::Never)
        } else {
            all_consuming(parse_filter_or)
                .parse(trimmed)
                .finish()
                .map(|(_, expr)| merge_associative_operations(expr))
        }
    }
}
