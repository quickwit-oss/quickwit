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

use std::str::FromStr;

use crate::filter_expr::{FilterExpr, StringPattern};

/// Macro for parsing tests. Use `=> "INVALID"` to test that parsing should fail.
///
/// Examples:
/// ```ignore
/// parse_tests! {
///     valid_case: "a:1" => "a:1",
///     invalid_case: "a(" => "INVALID",
/// }
/// ```
macro_rules! parse_tests {
    ($($name:ident: $input:expr => $expected:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                if $expected == "INVALID" {
                    assert!(
                        FilterExpr::from_str($input).is_err(),
                        "expected parse error for {:?}",
                        $input
                    );
                } else {
                    assert_eq!(
                        FilterExpr::from_str($input).unwrap().to_string(),
                        $expected,
                        "input: {:?}",
                        $input
                    );
                }
            }
        )*
    };
}

parse_tests! {
    // Basic key:value
    simple_key_value: "a:1" => "a:1",
    strips_whitespace: "  a:1  " => "a:1",
    star_matches_all: "*" => "*",
    empty_matches_never: "" => "",

    // Wildcards
    wildcard_prefix: "a:hello*" => "a:hello*",
    wildcard_suffix: "a:*world" => "a:*world",
    wildcard_contains: "a:*hello*" => "a:*hello*",
    wildcard_any: "a:*" => "a:*",

    // Parentheses
    parentheses_single: "(a:1)" => "a:1",
    parentheses_nested: "((a:1))" => "a:1",

    // NOT operators (-, not, !)
    not_lowercase: "not a:1" => "NOT (a:1)",
    not_no_space: "not(a:1)" => "NOT (a:1)",
    not_with_dash: "-a:1" => "NOT (a:1)",
    not_with_bang: "!a:1" => "NOT (a:1)",

    // AND operator
    and_two_terms: "a:1 and b:2" => "(a:1 AND b:2)",
    and_three_terms: "a:1 and b:2 and b:3" => "(a:1 AND b:2 AND b:3)",
    and_three_terms_paren: "(a:1 and b:2) and b:3" => "(a:1 AND b:2 AND b:3)",
    and_implicit: "a:1 b:2" => "(a:1 AND b:2)",
    and_no_space: "(a:1)and(b:2)" => "(a:1 AND b:2)",
    and_double_ampersand: "a:1 && b:2" => "(a:1 AND b:2)",

    // OR operator
    or_two_terms: "a:1 or b:2" => "(a:1 OR b:2)",
    or_no_space: "(a:1)or(b:2)" => "(a:1 OR b:2)",
    or_double_pipe: "a:1 || b:2" => "(a:1 OR b:2)",

    // Precedence
    and_binds_tighter_than_or: "a:1 or b:2 and c:3" => "(a:1 OR (b:2 AND c:3))",
    parentheses_override_precedence: "(a:1 or b:2) and c:3" => "((a:1 OR b:2) AND c:3)",
    not_before_and: "not a:1 and b:2" => "(NOT (a:1) AND b:2)",
    mixed_and_or_no_space: "(a:1)and(b:2)or(c:3)" => "((a:1 AND b:2) OR c:3)",

    // Escaped characters in key
    escaped_colon_in_key: r"foo\:bar:value" => "foo:bar:value",
    escaped_star_in_key: r"foo\*bar:value" => "foo*bar:value",
    escaped_backslash_in_key: r"foo\\bar:value" => r"foo\bar:value",
    escaped_paren_in_key: r"foo\(bar:value" => "foo(bar:value",
    escaped_space_in_key: r"foo\ bar:value" => "foo bar:value",

    // Escaped characters in value
    escaped_star_in_pattern: r"key:hello\*world" => "key:hello*world",
    escaped_backslash_in_pattern: r"key:hello\\world" => r"key:hello\world",
    escaped_space_in_pattern: r"key:hello\ world" => "key:hello world",
    colon_in_pattern: "key:hello:world" => "key:hello:world",

    // Dash in keys and values
    dash_in_value: "service:my-service" => "service:my-service",
    dash_prefixing_value: "service:-my-service" => r"service:-my-service",
    dash_in_key: "my-key:value" => "my-key:value",
    escaped_dash_start_key: r"\-key:value" => "-key:value",
    dash_after_not: "-service:foo-bar" => "NOT (service:foo-bar)",

    // special char in keys
    at_prefix_simple: "@ddaudit:web" => "@ddaudit:web",
    at_prefix_with_dot: "@http.status_code:200" => "@http.status_code:200",
    at_prefix_nested_path: "@foo.bar.baz:value" => "@foo.bar.baz:value",
    at_prefix_escaped_dot: r"@foo\.bar.baz:value" => r"@foo.bar.baz:value",
    at_prefix_multiple_escaped_dots: r"@foo\.bar\.baz:value" => r"@foo.bar.baz:value",
    at_prefix_mixed_escaped_and_unescaped: r"@foo.bar\.baz.qux:value" => r"@foo.bar.baz.qux:value",
    slash_in_key: r"app.kubernetes.io/name:test" => "app.kubernetes.io/name:test",
    escaped_char_in_key: r"\-\:\(\)\*\-:test" => r"-:()*-:test",
    needless_escape_key: r"app\.kub\erne\@tes\.io\/name:test" => "app.kuberne@tes.io/name:test",

    // Edge cases: partial operator gluing
    and_glued_left_space_right: "a:1and a:2" => "(a:1and AND a:2)",
    or_glued_left_space_right: "a:1or a:2" => "(a:1or AND a:2)",
    and_glued_both: "a:1anda:2" => "a:1anda:2",

    // Quoted strings
    quoted_simple: r#"key:"hello world""# => "key:hello world",
    quoted_with_star: r#"key:"hello*world""# => "key:hello*world",
    quoted_with_escaped_quote: r#"key:"hello\"world""# => r#"key:hello"world"#,
    quoted_curly_both: "key:\u{201C}hello world\u{201D}" => "key:hello world",
    quoted_curly_left: "key:\u{201C}hello world\u{201C}" => "key:hello world",
    quoted_curly_right: "key:\u{201D}hello world\u{201D}" => "key:hello world",
    quoted_curly_mixed_curly_left_straight_right: "key:\u{201C}hello world\"" => "key:hello world",
    quoted_curly_mixed_straight_left_curly_right: "key:\"hello world\u{201D}" => "key:hello world",

    // Pattern OR
    pattern_or: "key:(a or b)" => "key:(a OR b)",
    pattern_or_with_wildcards: "key:(foo* or *bar)" => "key:(foo* OR *bar)",
    pattern_or_nested: "key:((a or b) or c)" => "key:(a OR b OR c)",
    pattern_or_quoted: r#"key:("hello world" or other)"# => "key:(hello world OR other)",

    // Pattern AND
    pattern_and: "key:(a and b)" => "key:(a AND b)",
    pattern_and_with_wildcards: "key:(foo* and *bar)" => "key:(foo* AND *bar)",

    // Pattern NOT
    pattern_not: "key:(not a)" => "key:NOT (a)",
    pattern_not_dash: "key:(-a)" => "key:NOT (a)",
    pattern_not_with_wildcard: "key:(not foo*)" => "key:NOT (foo*)",

    // Pattern mixed operators
    pattern_and_or_precedence: "key:(a or b and c)" => "key:(a OR (b AND c))",
    pattern_complex: "key:(a and not b or c)" => "key:((a AND NOT (b)) OR c)",

    // Invalid: unescaped star in key
    invalid_unescaped_star_in_key: "foo*bar:value" => "INVALID",
    // Invalid: unfinished operators
    invalid_unfinished_or: "a:1 or" => "INVALID",
    invalid_unfinished_and: "a:1 and" => "INVALID",
    invalid_unfinished_not: "not" => "INVALID",
    // Invalid: parentheses errors
    invalid_unclosed_paren: "(a:1" => "INVALID",
    invalid_extra_close_paren: "a:1)" => "INVALID",
    invalid_empty_parens: "()" => "INVALID",
    // Invalid: missing key
    invalid_missing_key: ":value" => "INVALID",
    // Invalid: structure
    invalid_leading_or: "or a:1" => "INVALID",
    invalid_double_or: "a:1 or or b:2" => "INVALID",
    // Invalid: missing whitespace without parentheses
    invalid_or_glued_left: "(a:1)ora:2" => "INVALID",
    invalid_glued_implicit_and: "(a:1)(a:2)" => "INVALID",
    // Invalid: pattern syntax
    invalid_pattern_unclosed_paren: "key:(a or b" => "INVALID",
    invalid_pattern_empty_parens: "key:()" => "INVALID",
    invalid_pattern_trailing_or: "key:(a or)" => "INVALID",
    invalid_pattern_or_no_outer_paren: "key:(a)or(b)" => "INVALID",
    invalid_pattern_complex_without_paren: "key:not(a)" => "INVALID",

    // Unicode
    unicode_key: "服务:web" => "服务:web",
    unicode_value: "service:数据库" => "service:数据库",
    unicode_wildcard: "name:café*" => "name:café*",
    unicode_operators: "环境:生产 and 地区:亚洲" => "(环境:生产 AND 地区:亚洲)",
    unicode_pattern_or: "状态:(成功 or 失败)" => "状态:(成功 OR 失败)",
    unicode_rtl: "اسم:قيمة" => "اسم:قيمة",
    unicode_emoji: "status:✅" => "status:✅",
}

/// Ensure PrefixAndSuffix is not matched when prefix or suffix would be empty.
/// These should parse as Prefix or Suffix respectively, not PrefixAndSuffix.
#[test]
fn test_prefix_suffix_not_empty() {
    fn get_pattern(input: &str) -> StringPattern {
        match FilterExpr::from_str(input).unwrap() {
            FilterExpr::Match { pattern, .. } => pattern,
            other => panic!("expected Match, got {:?}", other),
        }
    }

    // "a:prefix*" should be Prefix, not PrefixAndSuffix with empty suffix
    assert!(
        matches!(get_pattern("a:prefix*"), StringPattern::Prefix(_)),
        "expected Prefix for 'a:prefix*'"
    );

    // "a:*suffix" should be Suffix, not PrefixAndSuffix with empty prefix
    assert!(
        matches!(get_pattern("a:*suffix"), StringPattern::Suffix(_)),
        "expected Suffix for 'a:*suffix'"
    );

    // "a:prefix*suffix" should be PrefixAndSuffix
    assert!(
        matches!(
            get_pattern("a:prefix*suffix"),
            StringPattern::PrefixAndSuffix { .. }
        ),
        "expected PrefixAndSuffix for 'a:prefix*suffix'"
    );
}

/// Test parsing of real-world production routing filters.
/// This test reads from a JSON file containing actual Datadog routing rules (from
/// /logs/pipelines/indexes) (filtered to exclude unsupported full-text search patterns).
///
/// All filters in the file MUST parse successfully.
#[test]
fn test_parse_real_world_filters() {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct IndexRule {
        index_id: String,
        filter: String,
    }

    let json_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/large-index-routing-table.json"
    );

    let content = std::fs::read_to_string(json_path)
        .expect("tests/fixtures/large-index-routing-table.json not found");

    let rules: Vec<IndexRule> = serde_json::from_str(&content).expect("invalid JSON");

    let mut failed: Vec<(String, String, String)> = Vec::new();

    for rule in &rules {
        if let Err(e) = FilterExpr::from_str(&rule.filter) {
            failed.push((rule.index_id.clone(), rule.filter.clone(), e.to_string()));
        }
    }

    if !failed.is_empty() {
        let mut msg = format!("\n{} filters failed to parse:\n\n", failed.len());
        for (name, filter, err) in &failed {
            msg.push_str(&format!(
                "--- {} ---\nFilter: {}\nError:  {}\n\n",
                name, filter, err
            ));
        }
        panic!("{}", msg);
    }
}
