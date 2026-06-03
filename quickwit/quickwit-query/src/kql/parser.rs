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

use anyhow::{anyhow, bail};

use crate::kql::ast::{KqlAst, KqlValue, RangeOp};
use crate::kql::lexer::{Token, tokenize};

/// Maximum nesting depth the recursive-descent parser will traverse before
/// rejecting the input. Bounds the call-stack footprint of pathological
/// queries like `((((...))))` so a single request can't crash a worker.
pub(crate) const MAX_KQL_DEPTH: u32 = 64;

/// Parse a KQL query string into a `KqlAst`.
pub(crate) fn parse_kql(input: &str) -> anyhow::Result<KqlAst> {
    // Surface the lexer's specific error directly — wrapping with
    // `context("KQL tokenization failed")` makes the actual reason
    // ("unsupported character", "unterminated phrase", etc.) invisible to
    // callers since anyhow's `Display` only shows the outermost message.
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        bail!("empty KQL query");
    }
    let mut parser = Parser::new(tokens);
    let ast = parser.parse_or()?;
    if parser.pos < parser.tokens.len() {
        let tok = &parser.tokens[parser.pos];
        bail!(
            "unexpected {} at position {} (parser did not consume the whole input)",
            tok.describe(),
            parser.pos
        );
    }
    Ok(ast)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    /// Current grouping nesting depth — incremented at each `(` and at each
    /// recursive `parse_or` call so both paren-depth and operator-depth are
    /// counted toward the same budget.
    depth: u32,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            pos: 0,
            depth: 0,
        }
    }

    /// Increment the depth counter; return an error if the budget is
    /// exceeded. Must be paired with `leave_depth` after the recursive call
    /// returns (`finally`-style — even on the error path).
    fn enter_depth(&mut self) -> anyhow::Result<()> {
        if self.depth >= MAX_KQL_DEPTH {
            bail!("KQL query nesting exceeds maximum depth of {MAX_KQL_DEPTH}");
        }
        self.depth += 1;
        Ok(())
    }

    fn leave_depth(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn bump(&mut self) -> Option<Token> {
        if self.pos >= self.tokens.len() {
            return None;
        }
        let tok = self.tokens[self.pos].clone();
        self.pos += 1;
        Some(tok)
    }

    fn eat(&mut self, expected: &Token) -> bool {
        if self.peek() == Some(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: &Token) -> anyhow::Result<()> {
        if self.eat(expected) {
            Ok(())
        } else {
            let found = self.peek().map(Token::describe).unwrap_or("end of input");
            Err(anyhow!("expected {}, found {}", expected.describe(), found))
        }
    }

    // or ::= and ('or' and)*
    fn parse_or(&mut self) -> anyhow::Result<KqlAst> {
        self.enter_depth()?;
        let result = self.parse_or_inner();
        self.leave_depth();
        result
    }

    fn parse_or_inner(&mut self) -> anyhow::Result<KqlAst> {
        let mut clauses = vec![self.parse_and()?];
        while self.eat(&Token::KwOr) {
            clauses.push(self.parse_and()?);
        }
        Ok(flatten_or(clauses))
    }

    // and ::= not (('and')? not)*
    //
    // KQL treats juxtaposition (space-separated terms with no explicit operator)
    // as an implicit AND, matching Kibana semantics: `level:error status:500`
    // is equivalent to `level:error and status:500`.
    fn parse_and(&mut self) -> anyhow::Result<KqlAst> {
        let mut clauses = vec![self.parse_not()?];
        loop {
            if self.eat(&Token::KwAnd) {
                clauses.push(self.parse_not()?);
                continue;
            }
            if self.can_start_clause() {
                clauses.push(self.parse_not()?);
                continue;
            }
            break;
        }
        Ok(flatten_and(clauses))
    }

    fn can_start_clause(&self) -> bool {
        matches!(
            self.peek(),
            Some(Token::LParen)
                | Some(Token::KwNot)
                | Some(Token::Bare(_))
                | Some(Token::Phrase(_))
        )
    }

    // not ::= 'not' not | primary
    //
    // Recurses on consecutive `not not not ...` chains, so this also needs to
    // be depth-guarded.
    fn parse_not(&mut self) -> anyhow::Result<KqlAst> {
        if self.eat(&Token::KwNot) {
            self.enter_depth()?;
            let inner_result = self.parse_not();
            self.leave_depth();
            return Ok(KqlAst::Not(Box::new(inner_result?)));
        }
        self.parse_primary()
    }

    // primary ::= '(' or ')' | clause
    fn parse_primary(&mut self) -> anyhow::Result<KqlAst> {
        if self.eat(&Token::LParen) {
            let inner = self.parse_or()?;
            self.expect(&Token::RParen)?;
            return Ok(inner);
        }
        self.parse_clause()
    }

    // clause ::= bare ':' field_rhs | bare | phrase
    //
    // A `field_rhs` can be a value group like `(a or b)`, a range bound, or a
    // single value atom (literal/phrase/wildcard/exists).
    fn parse_clause(&mut self) -> anyhow::Result<KqlAst> {
        let tok = self
            .bump()
            .ok_or_else(|| anyhow!("unexpected end of input"))?;
        let lhs_text = match tok {
            Token::Bare(text) => text,
            Token::Phrase(text) => {
                return Ok(KqlAst::DefaultValue(KqlValue::Phrase(text)));
            }
            other => bail!("unexpected {} at start of clause", other.describe()),
        };
        if !self.eat(&Token::Colon) {
            // It's just a bare value matched against default fields.
            return Ok(KqlAst::DefaultValue(KqlValue::Literal(lhs_text)));
        }
        self.parse_field_rhs(lhs_text)
    }

    fn parse_field_rhs(&mut self, field: String) -> anyhow::Result<KqlAst> {
        // Range bounds: field:>N field:>=N field:<N field:<=N
        let range_op_opt = match self.peek() {
            Some(Token::Gt) => Some(RangeOp::Gt),
            Some(Token::Gte) => Some(RangeOp::Gte),
            Some(Token::Lt) => Some(RangeOp::Lt),
            Some(Token::Lte) => Some(RangeOp::Lte),
            _ => None,
        };
        if let Some(op) = range_op_opt {
            self.pos += 1;
            let value = self.parse_range_value()?;
            return Ok(KqlAst::FieldRange { field, op, value });
        }
        // Value group: field:(a or b)
        if self.eat(&Token::LParen) {
            let group_ast = self.parse_or()?;
            self.expect(&Token::RParen)?;
            return distribute_field(&field, group_ast);
        }
        // Single value atom — either an exists marker (`*`), a wildcard
        // literal, a plain literal, or a phrase.
        let value_tok = self
            .bump()
            .ok_or_else(|| anyhow!("expected value after '{}:'", field))?;
        match value_tok {
            Token::Bare(text) => {
                if text == "*" {
                    Ok(KqlAst::FieldExists { field })
                } else {
                    Ok(KqlAst::FieldValue {
                        field,
                        value: KqlValue::Literal(text),
                    })
                }
            }
            Token::Phrase(text) => Ok(KqlAst::FieldValue {
                field,
                value: KqlValue::Phrase(text),
            }),
            other => bail!(
                "expected a value after '{}:', found {}",
                field,
                other.describe()
            ),
        }
    }

    fn parse_range_value(&mut self) -> anyhow::Result<String> {
        let tok = self
            .bump()
            .ok_or_else(|| anyhow!("expected value after range operator"))?;
        match tok {
            Token::Bare(text) => Ok(text),
            Token::Phrase(text) => Ok(text),
            other => bail!(
                "expected a value after range operator, found {}",
                other.describe()
            ),
        }
    }
}

/// Re-attach a field qualifier to every leaf of a value-group sub-AST so that
/// `field:(a or b)` lowers as `(field:a or field:b)`.
///
/// Returns an error if the inner expression contains an already-qualified
/// clause (`field:(other:value)`). Silently rebinding the field is a footgun:
/// users debugging "why does my query target the wrong column" deserve an
/// explicit failure, matching Kibana's own rejection of that syntax.
fn distribute_field(field: &str, ast: KqlAst) -> anyhow::Result<KqlAst> {
    match ast {
        KqlAst::And(children) => {
            let distributed: anyhow::Result<Vec<KqlAst>> = children
                .into_iter()
                .map(|c| distribute_field(field, c))
                .collect();
            Ok(KqlAst::And(distributed?))
        }
        KqlAst::Or(children) => {
            let distributed: anyhow::Result<Vec<KqlAst>> = children
                .into_iter()
                .map(|c| distribute_field(field, c))
                .collect();
            Ok(KqlAst::Or(distributed?))
        }
        KqlAst::Not(inner) => {
            let inner = distribute_field(field, *inner)?;
            Ok(KqlAst::Not(Box::new(inner)))
        }
        KqlAst::DefaultValue(value) => match value {
            KqlValue::Literal(text) if text == "*" => Ok(KqlAst::FieldExists {
                field: field.to_string(),
            }),
            other => Ok(KqlAst::FieldValue {
                field: field.to_string(),
                value: other,
            }),
        },
        KqlAst::FieldValue {
            field: inner_field, ..
        }
        | KqlAst::FieldRange {
            field: inner_field, ..
        }
        | KqlAst::FieldExists { field: inner_field } => bail!(
            "nested field qualifier `{inner_field}:` inside value group for `{field}:(...)` is \
             not allowed"
        ),
    }
}

fn flatten_and(mut clauses: Vec<KqlAst>) -> KqlAst {
    if clauses.len() == 1 {
        return clauses.pop().unwrap();
    }
    let mut flattened: Vec<KqlAst> = Vec::with_capacity(clauses.len());
    for clause in clauses {
        match clause {
            KqlAst::And(children) => flattened.extend(children),
            other => flattened.push(other),
        }
    }
    KqlAst::And(flattened)
}

fn flatten_or(mut clauses: Vec<KqlAst>) -> KqlAst {
    if clauses.len() == 1 {
        return clauses.pop().unwrap();
    }
    let mut flattened: Vec<KqlAst> = Vec::with_capacity(clauses.len());
    for clause in clauses {
        match clause {
            KqlAst::Or(children) => flattened.extend(children),
            other => flattened.push(other),
        }
    }
    KqlAst::Or(flattened)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> KqlAst {
        parse_kql(input).unwrap_or_else(|err| panic!("failed to parse {input:?}: {err}"))
    }

    fn fv(field: &str, val: &str) -> KqlAst {
        KqlAst::FieldValue {
            field: field.into(),
            value: KqlValue::Literal(val.into()),
        }
    }

    #[test]
    fn test_parse_simple_field_value() {
        assert_eq!(parse("level:error"), fv("level", "error"));
    }

    #[test]
    fn test_parse_implicit_and() {
        // Space-separated terms = AND, per Kibana semantics.
        assert_eq!(
            parse("level:error status:500"),
            KqlAst::And(vec![fv("level", "error"), fv("status", "500")])
        );
    }

    #[test]
    fn test_parse_explicit_and_or_precedence() {
        // OR binds looser than AND: `a or b and c` parses as `a or (b and c)`.
        assert_eq!(
            parse("a:1 or b:2 and c:3"),
            KqlAst::Or(vec![
                fv("a", "1"),
                KqlAst::And(vec![fv("b", "2"), fv("c", "3")])
            ])
        );
    }

    #[test]
    fn test_parse_not() {
        assert_eq!(
            parse("not level:error"),
            KqlAst::Not(Box::new(fv("level", "error")))
        );
    }

    #[test]
    fn test_parse_paren_overrides_precedence() {
        assert_eq!(
            parse("(a:1 or b:2) and c:3"),
            KqlAst::And(vec![
                KqlAst::Or(vec![fv("a", "1"), fv("b", "2")]),
                fv("c", "3"),
            ])
        );
    }

    #[test]
    fn test_parse_value_group_distributes() {
        // `field:(a or b)` → `field:a or field:b`
        assert_eq!(
            parse("level:(error or warn)"),
            KqlAst::Or(vec![fv("level", "error"), fv("level", "warn")])
        );
    }

    #[test]
    fn test_parse_value_group_with_and() {
        // `field:(a and b)` is supported even though it's rarely semantically
        // useful at the doc-level — Kibana accepts it.
        assert_eq!(
            parse("tag:(prod and critical)"),
            KqlAst::And(vec![fv("tag", "prod"), fv("tag", "critical")])
        );
    }

    #[test]
    fn test_parse_range() {
        assert_eq!(
            parse("size:>=10"),
            KqlAst::FieldRange {
                field: "size".into(),
                op: RangeOp::Gte,
                value: "10".into(),
            }
        );
        // Range values containing `:` (e.g. ISO timestamps) must be quoted so
        // the lexer treats them as a single phrase token. Kibana behaves
        // identically.
        assert_eq!(
            parse(r#"ts:<"2026-01-01T00:00:00Z""#),
            KqlAst::FieldRange {
                field: "ts".into(),
                op: RangeOp::Lt,
                value: "2026-01-01T00:00:00Z".into(),
            }
        );
    }

    #[test]
    fn test_parse_exists() {
        assert_eq!(
            parse("level:*"),
            KqlAst::FieldExists {
                field: "level".into()
            }
        );
    }

    #[test]
    fn test_parse_wildcard_value_is_literal() {
        // `level:err*` is a wildcard *value*, not an exists check.
        assert_eq!(parse("level:err*"), fv("level", "err*"));
    }

    #[test]
    fn test_parse_phrase_value() {
        assert_eq!(
            parse(r#"msg:"hello world""#),
            KqlAst::FieldValue {
                field: "msg".into(),
                value: KqlValue::Phrase("hello world".into()),
            }
        );
    }

    #[test]
    fn test_parse_bare_default_value() {
        assert_eq!(
            parse("hello"),
            KqlAst::DefaultValue(KqlValue::Literal("hello".into()))
        );
        assert_eq!(
            parse(r#""hello world""#),
            KqlAst::DefaultValue(KqlValue::Phrase("hello world".into()))
        );
    }

    #[test]
    fn test_parse_dotted_field() {
        assert_eq!(parse("nested.field:value"), fv("nested.field", "value"));
    }

    #[test]
    fn test_parse_not_inside_value_group() {
        // `field:(a or not b)` → `field:a or not field:b`
        assert_eq!(
            parse("level:(error or not warn)"),
            KqlAst::Or(vec![
                fv("level", "error"),
                KqlAst::Not(Box::new(fv("level", "warn"))),
            ])
        );
    }

    #[test]
    fn test_parse_flattens_associative_ops() {
        // Successive `or`s should produce one flat OR, not a nested tree.
        assert_eq!(
            parse("a:1 or b:2 or c:3"),
            KqlAst::Or(vec![fv("a", "1"), fv("b", "2"), fv("c", "3")])
        );
    }

    #[test]
    fn test_parse_errors_on_dangling_colon() {
        assert!(parse_kql("level:").is_err());
    }

    #[test]
    fn test_parse_errors_on_unbalanced_paren() {
        assert!(parse_kql("(level:error").is_err());
    }

    #[test]
    fn test_parse_errors_on_empty() {
        assert!(parse_kql("").is_err());
        assert!(parse_kql("   ").is_err());
    }

    #[test]
    fn test_parse_errors_on_empty_parens() {
        // `()` has no clause inside — the parser bails when it tries to read a
        // clause and finds `)`.
        assert!(parse_kql("()").is_err());
    }

    #[test]
    fn test_parse_errors_on_dangling_operator() {
        assert!(parse_kql("a:1 and").is_err());
        assert!(parse_kql("a:1 or").is_err());
        assert!(parse_kql("not").is_err());
    }

    #[test]
    fn test_parse_errors_on_leading_binary_operator() {
        // `and`/`or` cannot start a clause; the parser surfaces it as an
        // unexpected token rather than silently accepting it.
        assert!(parse_kql("and a:1").is_err());
        assert!(parse_kql("or a:1").is_err());
    }

    #[test]
    fn test_parse_errors_on_double_colon() {
        // `a::b` — the second `:` is not a valid value.
        assert!(parse_kql("a::b").is_err());
    }

    #[test]
    fn test_parse_errors_on_leading_colon() {
        // `:value` has no field name.
        assert!(parse_kql(":value").is_err());
    }

    #[test]
    fn test_parse_keyword_as_field_name_requires_quoting() {
        // Unquoted `and:value` lexes the `and` as KwAnd, so the parser sees
        // a binary operator at the start and rejects it. To search a field
        // literally named `and`, the user must escape: `\and:value`.
        assert!(parse_kql("and:value").is_err());
        assert_eq!(
            parse(r"\and:value"),
            KqlAst::FieldValue {
                field: "and".into(),
                value: KqlValue::Literal("value".into()),
            }
        );
    }

    #[test]
    fn test_parse_double_negation() {
        // `not not x` reduces to `not(not x)` — equivalent to `x` semantically
        // but the parser preserves the structure (no constant folding).
        assert_eq!(
            parse("not not a:1"),
            KqlAst::Not(Box::new(KqlAst::Not(Box::new(fv("a", "1")))))
        );
    }

    #[test]
    fn test_parse_nested_value_group() {
        // `field:((a or b) and c)` — distributes the field across both inner
        // clauses while preserving the boolean structure.
        assert_eq!(
            parse("level:((error or warn) and *)"),
            KqlAst::And(vec![
                KqlAst::Or(vec![fv("level", "error"), fv("level", "warn")]),
                KqlAst::FieldExists {
                    field: "level".into()
                },
            ])
        );
    }

    #[test]
    fn test_parse_not_precedence_with_or() {
        // NOT binds tighter than OR: `not a:1 or b:2` parses as
        // `(not a:1) or b:2`, not `not (a:1 or b:2)`.
        assert_eq!(
            parse("not a:1 or b:2"),
            KqlAst::Or(vec![KqlAst::Not(Box::new(fv("a", "1"))), fv("b", "2")])
        );
    }

    #[test]
    fn test_parse_escaped_colon_in_field_name() {
        // `\:` inside a bare token escapes the colon, so the colon doesn't
        // start a field qualifier. Lets users include literal `:` in field
        // names like `metric\:count`.
        assert_eq!(
            parse(r"foo\:bar:value"),
            KqlAst::FieldValue {
                field: "foo:bar".into(),
                value: KqlValue::Literal("value".into()),
            }
        );
    }

    #[test]
    fn test_parse_bare_star_against_default_fields() {
        // A standalone `*` is treated as a default-field literal containing a
        // wildcard. Lowering routes it to a WildcardQuery; lowering against
        // an empty default-field list errors out (see lower::tests).
        assert_eq!(
            parse("*"),
            KqlAst::DefaultValue(KqlValue::Literal("*".into()))
        );
    }

    #[test]
    fn test_parse_empty_quoted_phrase_value() {
        // `field:""` produces a phrase value with empty text. The lowering
        // routes this through full-text phrase mode; with the
        // `zero_terms_query=MatchNone` default this matches nothing.
        assert_eq!(
            parse(r#"field:"""#),
            KqlAst::FieldValue {
                field: "field".into(),
                value: KqlValue::Phrase(String::new()),
            }
        );
    }

    #[test]
    fn test_parse_rejects_deeply_nested_parens() {
        // 256 deep is well above the 64-limit; the parser should refuse the
        // input with a depth error rather than stack-overflow.
        let input = "(".repeat(256) + "a:1" + &")".repeat(256);
        let err = parse_kql(&input).expect_err("deep nesting must be rejected");
        assert!(
            err.to_string().contains("maximum depth"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_parse_rejects_deep_not_chain() {
        // Long `not not not ...` chains recurse through parse_not.
        let input = "not ".repeat(256) + "a:1";
        let err = parse_kql(&input).expect_err("deep not chain must be rejected");
        assert!(
            err.to_string().contains("maximum depth"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_parse_rejects_nested_field_qualifier_in_value_group() {
        // `field:(other:value)` is ambiguous (does `other:` rebind the outer
        // qualifier?). Reject it explicitly rather than silently picking one.
        let err =
            parse_kql("level:(severity:high)").expect_err("nested qualifier must be rejected");
        assert!(
            err.to_string().contains("nested field qualifier"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_parse_rejects_nested_field_range_in_value_group() {
        // The FieldRange arm of `distribute_field`'s rejection.
        let err = parse_kql("level:(severity:>5)")
            .expect_err("nested range qualifier must be rejected");
        assert!(err.to_string().contains("nested field qualifier"));
    }

    #[test]
    fn test_parse_rejects_nested_field_exists_in_value_group() {
        // The FieldExists arm of `distribute_field`'s rejection.
        let err = parse_kql("level:(severity:*)")
            .expect_err("nested field-exists qualifier must be rejected");
        assert!(err.to_string().contains("nested field qualifier"));
    }

    #[test]
    fn test_parse_range_rejects_non_value_token_after_operator() {
        // `size:>=(10)` — after `>=`, the parser expects a value token
        // (Bare or Phrase). A `(` here surfaces the "expected a value
        // after range operator" path; otherwise that error arm is dead.
        let err = parse_kql("size:>=(10)").expect_err("paren after range op must be rejected");
        assert!(
            err.to_string().contains("after range operator"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_parse_range_lte() {
        // Direct parser-level test of the `<=` operator. The integration
        // scenarios cover this end-to-end but the unit-test surface needs
        // its own pin.
        assert_eq!(
            parse("size:<=10"),
            KqlAst::FieldRange {
                field: "size".into(),
                op: RangeOp::Lte,
                value: "10".into(),
            }
        );
    }

    #[test]
    fn test_parse_flatten_or_descends_into_paren_produced_or() {
        // `(a or b) or c` — the paren produces an `Or` node which then gets
        // OR'd again at the outer level. The flatten pass must descend into
        // the inner `Or` so the final tree is a single 3-way Or, not a
        // 2-way Or containing an Or.
        assert_eq!(
            parse("(a:1 or b:2) or c:3"),
            KqlAst::Or(vec![fv("a", "1"), fv("b", "2"), fv("c", "3")])
        );
    }

    #[test]
    fn test_parse_flatten_and_descends_into_paren_produced_and() {
        // Same property for the `and` flattener, hit via parens.
        assert_eq!(
            parse("(a:1 and b:2) and c:3"),
            KqlAst::And(vec![fv("a", "1"), fv("b", "2"), fv("c", "3")])
        );
    }

    // Property-based fuzz tests — `parse_kql` is the only KQL entry point that
    // takes untrusted bytes from the network, so it must never panic for any
    // input string within the size cap. The lexer's size guards plus the
    // parser's depth guard together close the obvious DoS paths; these
    // properties pin the invariant in CI.
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        /// `parse_kql` must never panic for arbitrary short ASCII input,
        /// regardless of how malformed.
        #[test]
        fn proptest_parse_never_panics_on_ascii(
            input in proptest::string::string_regex(r"[ -~\\t\\n]{0,128}").unwrap()
        ) {
            let _ = parse_kql(&input);
        }

        /// Same invariant under unrestricted Unicode — catches lexer paths
        /// that assume ASCII boundaries.
        #[test]
        fn proptest_parse_never_panics_on_unicode(
            input in ".{0,64}"
        ) {
            let _ = parse_kql(&input);
        }

        /// Well-formed `field:value` clauses always succeed.
        ///
        /// The field-name strategy excludes the bare keyword forms
        /// (`and`/`or`/`not`, case-insensitive). Unquoted keywords lex as
        /// `KwAnd`/`KwOr`/`KwNot`, not as bare identifiers — reaching them
        /// as a field name requires a backslash escape (covered separately
        /// in `test_parse_keyword_as_field_name_requires_quoting`).
        #[test]
        fn proptest_field_value_succeeds(
            field in "[a-zA-Z_][a-zA-Z0-9_]{0,16}"
                .prop_filter("field name must not collide with a KQL keyword", |s| {
                    let lower = s.to_ascii_lowercase();
                    lower != "and" && lower != "or" && lower != "not"
                }),
            value in "[a-zA-Z0-9_]{1,16}"
                .prop_filter("value must not be a wildcard / exists marker", |s| s != "*"),
        ) {
            let input = format!("{field}:{value}");
            let ast = parse_kql(&input).expect("well-formed clause must parse");
            prop_assert_eq!(
                ast,
                KqlAst::FieldValue {
                    field,
                    value: KqlValue::Literal(value),
                }
            );
        }
    }

    #[test]
    fn test_parse_value_group_with_nested_not() {
        // `field:(not a or b)` distributes negation through the group.
        assert_eq!(
            parse("level:(not error or warn)"),
            KqlAst::Or(vec![
                KqlAst::Not(Box::new(fv("level", "error"))),
                fv("level", "warn"),
            ])
        );
    }
}
