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

//! Merges ORed [`WildcardQuery`] clauses targeting the same field, JSON path, and
//! case-sensitivity into a single [`RegexQuery`] when the combined FST automaton
//! compiles cleanly.
//!
//! Leading `*` patterns (such as `*something*`) are excluded because they multiply
//! DFA states across branches. Each candidate group is compiled once via
//! [`get_or_compile_cached_fst_regex`]; if compilation fails (for example due to
//! `tantivy_fst`'s 1,000-state limit), the original wildcard queries are kept.

use std::collections::HashMap;

use tantivy::schema::Schema as TantivySchema;

use super::cache_node::CacheState;
use super::{BoolQuery, QueryAst, RegexQuery, WildcardQuery, get_or_compile_cached_fst_regex};
use crate::tokenizers::TokenizerManager;

pub(crate) fn merge_or_wildcards(
    query_ast: QueryAst,
    schema: &TantivySchema,
    tokenizer_manager: &TokenizerManager,
) -> QueryAst {
    match query_ast {
        QueryAst::Bool(mut bool_query) => {
            // `minimum_should_match` defaults to 1 when `must` and `filter` are empty, and 0
            // otherwise. Only `== 1` acts as a plain OR where merging wildcards preserves
            // matching semantics (`0` is optional scoring, `>= 2` counts distinct clauses).
            let min_should_match = bool_query.minimum_should_match.unwrap_or_else(|| {
                usize::from(bool_query.must.is_empty() && bool_query.filter.is_empty())
            });
            // Flatten nested pure-OR blocks before recursing so chained or parenthesized
            // ORs like `(a* OR b*) OR c*` merge into a single RegexQuery instead of
            // producing separate inner RegexQueries that cannot be merged at the parent.
            if min_should_match == 1 {
                flatten_wildcard_ors(&mut bool_query.should);
            }
            flatten_wildcard_ors(&mut bool_query.must_not);

            for clauses in [
                &mut bool_query.must,
                &mut bool_query.should,
                &mut bool_query.must_not,
                &mut bool_query.filter,
            ] {
                *clauses = std::mem::take(clauses)
                    .into_iter()
                    .map(|clause| merge_or_wildcards(clause, schema, tokenizer_manager))
                    .collect();
            }

            merge_wildcard_clauses(&mut bool_query.must_not, schema, tokenizer_manager);

            if min_should_match == 1 {
                let had_multiple_clauses = bool_query.should.len() > 1;
                merge_wildcard_clauses(&mut bool_query.should, schema, tokenizer_manager);
                if had_multiple_clauses
                    && is_pure_or_bool(&bool_query)
                    && bool_query.should.len() == 1
                {
                    return bool_query.should.pop().unwrap();
                }
            }

            QueryAst::Bool(bool_query)
        }
        QueryAst::Boost { underlying, boost } => QueryAst::Boost {
            underlying: Box::new(merge_or_wildcards(*underlying, schema, tokenizer_manager)),
            boost,
        },
        QueryAst::Cache(mut cache_node) => {
            if !matches!(cache_node.state, CacheState::CacheHit(_)) {
                cache_node.inner = Box::new(merge_or_wildcards(
                    *cache_node.inner,
                    schema,
                    tokenizer_manager,
                ));
            }
            QueryAst::Cache(cache_node)
        }
        other => other,
    }
}

fn is_pure_or_bool(bool_query: &BoolQuery) -> bool {
    !bool_query.should.is_empty()
        && bool_query.must.is_empty()
        && bool_query.filter.is_empty()
        && bool_query.must_not.is_empty()
        && matches!(bool_query.minimum_should_match, None | Some(1))
}

fn count_wildcards(clauses: &[QueryAst]) -> usize {
    clauses
        .iter()
        .map(|clause| match clause {
            QueryAst::Wildcard(_) => 1,
            QueryAst::Bool(bool_query) if is_pure_or_bool(bool_query) => {
                count_wildcards(&bool_query.should)
            }
            _ => 0,
        })
        .sum()
}

fn flatten_wildcard_ors(clauses: &mut Vec<QueryAst>) {
    fn flatten_into(clauses: Vec<QueryAst>, out: &mut Vec<QueryAst>) {
        for clause in clauses {
            match clause {
                QueryAst::Bool(bool_query)
                    if is_pure_or_bool(&bool_query) && count_wildcards(&bool_query.should) > 0 =>
                {
                    flatten_into(bool_query.should, out);
                }
                other => out.push(other),
            }
        }
    }

    if count_wildcards(clauses) >= 2 {
        let mut flattened = Vec::with_capacity(clauses.len());
        flatten_into(std::mem::take(clauses), &mut flattened);
        *clauses = flattened;
    }
}

fn merge_wildcard_clauses(
    clauses: &mut Vec<QueryAst>,
    schema: &TantivySchema,
    tokenizer_manager: &TokenizerManager,
) {
    if clauses
        .iter()
        .filter(|clause| matches!(clause, QueryAst::Wildcard(_)))
        .count()
        < 2
    {
        return;
    }

    let mut result = Vec::with_capacity(clauses.len());
    let mut group_indices: HashMap<(String, bool), usize> = HashMap::new();
    let mut groups: Vec<Vec<WildcardQuery>> = Vec::new();

    for clause in std::mem::take(clauses) {
        match clause {
            QueryAst::Wildcard(wildcard) if !wildcard.value.starts_with('*') => {
                let key = (wildcard.field.clone(), wildcard.case_insensitive);
                let group_idx = *group_indices.entry(key).or_insert_with(|| {
                    groups.push(Vec::new());
                    groups.len() - 1
                });
                groups[group_idx].push(wildcard);
            }
            other => result.push(other),
        }
    }

    for group in groups {
        if let Some(merged) = try_merge_wildcard_group(&group, schema, tokenizer_manager) {
            result.push(QueryAst::Regex(merged));
        } else {
            result.extend(group.into_iter().map(QueryAst::Wildcard));
        }
    }

    *clauses = result;
}

fn try_merge_wildcard_group(
    group: &[WildcardQuery],
    schema: &TantivySchema,
    tokenizer_manager: &TokenizerManager,
) -> Option<RegexQuery> {
    if group.len() < 2 {
        return None;
    }
    let joined = group
        .iter()
        .map(|wildcard| {
            wildcard
                .to_regex(schema, tokenizer_manager)
                .ok()
                .map(|(_, _, regex)| regex)
        })
        .collect::<Option<Vec<String>>>()?
        .join("|");
    // Wrap case-sensitive merges in `(?-i:...)` so `RegexQuery::to_resolved` keeps
    // them case-sensitive on lowercasing fields without adding Unicode case-folding.
    let regex = if group[0].case_insensitive {
        joined
    } else {
        format!("(?-i:{joined})")
    };
    // Verify the combined automaton compiles (and populate the regex cache for execution).
    // Compilation can still fail if the merged DFA exceeds `tantivy_fst`'s state limit, in
    // which case returning `None` falls back to keeping the original wildcard queries.
    get_or_compile_cached_fst_regex(&regex)?;
    Some(RegexQuery {
        field: group[0].field.clone(),
        regex,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tantivy::collector::DocSetCollector;
    use tantivy::schema::{STRING, Schema as TantivySchema, TEXT, TextFieldIndexing, TextOptions};
    use tantivy::{Index, doc};

    use super::merge_or_wildcards;
    use crate::query_ast::{
        BoolQuery, BuildTantivyAstContext, QueryAst, RegexQuery, WildcardQuery,
        query_ast_from_user_text,
    };
    use crate::tokenizers::create_default_quickwit_tokenizer_manager;

    fn wc(field: &str, value: &str, case_insensitive: bool) -> QueryAst {
        WildcardQuery {
            field: field.to_string(),
            value: value.to_string(),
            lenient: false,
            case_insensitive,
        }
        .into()
    }

    fn regex_ast(field: &str, pattern: &str) -> QueryAst {
        RegexQuery {
            field: field.to_string(),
            regex: pattern.to_string(),
        }
        .into()
    }

    fn or_bool(should: Vec<QueryAst>) -> QueryAst {
        BoolQuery {
            should,
            ..Default::default()
        }
        .into()
    }

    fn parse(query: &str, default_fields: Option<Vec<String>>) -> QueryAst {
        query_ast_from_user_text(query, default_fields)
            .parse_user_query(&[])
            .unwrap()
    }

    #[test]
    fn test_merge_or_wildcards() {
        let mut builder = TantivySchema::builder();
        for name in ["text_default", "other_default"] {
            builder.add_text_field(name, TEXT);
        }
        for name in ["text_raw", "other_raw"] {
            builder.add_text_field(name, STRING);
        }
        builder.add_json_field(
            "json_raw",
            TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer("raw")),
        );
        let schema = builder.build();
        let tm = create_default_quickwit_tokenizer_manager();
        let merge = |ast| merge_or_wildcards(ast, &schema, &tm);

        let index = Index::create_in_ram(schema.clone());
        let mut writer = index.writer_with_num_threads(1, 20_000_000).unwrap();
        let [text_default, other_default, text_raw, json_raw] =
            ["text_default", "other_default", "text_raw", "json_raw"]
                .map(|name| schema.get_field(name).unwrap());
        for (td, od, tr, ja, jb) in [
            ("AlphaOne 1001", "", "FooOne", "svc-1", "x"),
            ("BetaTwo 1015", "", "fooTwo", "svc-2", "y"),
            ("GammaThree", "1599", "BarThree", "api-1", "svc-1"),
            ("DeltaFour 5432", "", "barFour", "web-1", "z"),
            ("999", "98765", "other", "none", "w"),
            ("100000", "1000", "other2", "none", "w"),
        ] {
            writer
                .add_document(doc!(
                    text_default => td,
                    other_default => od,
                    text_raw => tr,
                    json_raw => serde_json::json!({"a": ja, "b": jb}),
                ))
                .unwrap();
        }
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let context = BuildTantivyAstContext::for_test(&schema);
        let search = |ast: &QueryAst| -> BTreeSet<_> {
            searcher
                .search(
                    &ast.build_tantivy_query(&context).unwrap(),
                    &DocSetCollector,
                )
                .unwrap()
                .into_iter()
                .collect()
        };

        let foo_bar = vec![wc("text_raw", "foo*", false), wc("text_raw", "bar*", false)];
        let untouched = |ast: QueryAst| (ast.clone(), ast);

        // Merging rules, exclusions, untouched queries, TooManyStates fallback, and equivalence.
        for (original, expected) in [
            (
                parse(
                    "((text_raw:Foo* OR text_raw:Bar*) OR text_raw:Baz*) OR (text_raw:Qux* OR \
                     text_raw:Quux*)",
                    None,
                ),
                regex_ast("text_raw", "(?-i:Foo.*|Bar.*|Baz.*|Qux.*|Quux.*)"),
            ),
            (
                or_bool(vec![
                    wc("text_raw", "foo*", true),
                    wc("text_raw", "bar*", true),
                ]),
                regex_ast("text_raw", "(?i)foo.*|(?i)bar.*"),
            ),
            (
                parse("text_default:Alpha* OR text_default:Gamma*", None),
                regex_ast("text_default", "(?-i:alpha.*|gamma.*)"),
            ),
            (
                or_bool(vec![
                    wc("text_default", "Alpha*", true),
                    wc("text_default", "Gamma*", true),
                ]),
                regex_ast("text_default", "(?i)alpha.*|(?i)gamma.*"),
            ),
            (
                parse("json_raw.a:svc-* OR json_raw.a:web-*", None),
                regex_ast("json_raw.a", "(?-i:svc\\-.*|web\\-.*)"),
            ),
            (
                parse(
                    "text_raw:*something* OR text_raw:/explicit.*/ OR text_raw:foo* OR \
                     text_raw:bar* OR other_raw:baz* OR json_raw.a:alpha* OR json_raw.b:beta* OR \
                     json_raw.a:gamma*",
                    None,
                ),
                or_bool(vec![
                    wc("text_raw", "*something*", false),
                    regex_ast("text_raw", "explicit.*"),
                    regex_ast("text_raw", "(?-i:foo.*|bar.*)"),
                    wc("other_raw", "baz*", false),
                    regex_ast("json_raw.a", "(?-i:alpha.*|gamma.*)"),
                    wc("json_raw.b", "beta*", false),
                ]),
            ),
            (
                parse("-text_raw:foo* -text_raw:bar*", None),
                BoolQuery {
                    must_not: vec![regex_ast("text_raw", "(?-i:foo.*|bar.*)")],
                    ..Default::default()
                }
                .into(),
            ),
            untouched(or_bool(vec![
                wc("text_raw", "Foo*", false),
                wc("text_raw", "Bar*", true),
            ])),
            untouched(
                BoolQuery {
                    should: foo_bar.clone(),
                    minimum_should_match: Some(2),
                    ..Default::default()
                }
                .into(),
            ),
            untouched(
                BoolQuery {
                    must: vec![parse("other_raw:x", None)],
                    should: foo_bar,
                    ..Default::default()
                }
                .into(),
            ),
            untouched(parse(
                &(0..300)
                    .map(|i| format!("text_raw:a{i:04}x{i:04}*"))
                    .collect::<Vec<_>>()
                    .join(" OR "),
                None,
            )),
        ] {
            let merged = merge(original.clone());
            assert_eq!(merged, expected);
            assert_eq!(search(&original), search(&merged));
        }

        // Nested multi-field numeric range (70 wildcards across 2 fields -> 2 RegexQueries).
        let terms: Vec<String> = std::iter::once("1001".to_string())
            .chain((1..=9).map(|i| format!("10{i}?")))
            .chain((1..=9).map(|i| format!("1{i}??")))
            .chain((2..=9).map(|i| format!("{i}???")))
            .chain((1..=9).map(|i| format!("{i}????")))
            .collect();
        let nested_query = parse(
            &terms.join(" OR "),
            Some(vec!["text_default".into(), "other_default".into()]),
        );
        let merged = merge(nested_query.clone());
        let QueryAst::Bool(disjunction) = &merged else {
            panic!("expected BoolQuery");
        };
        assert_eq!(disjunction.should.len(), 3);
        assert!(
            matches!(&disjunction.should[1], QueryAst::Regex(regex) if regex.field == "text_default")
        );
        assert!(
            matches!(&disjunction.should[2], QueryAst::Regex(regex) if regex.field == "other_default")
        );
        let orig_docs = search(&nested_query);
        assert_eq!(orig_docs.len(), 5);
        assert_eq!(orig_docs, search(&merged));
    }
}
