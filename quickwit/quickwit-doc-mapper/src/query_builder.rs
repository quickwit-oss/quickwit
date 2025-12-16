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

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::ops::Bound;
use std::sync::Arc;

use quickwit_proto::types::SplitId;
use quickwit_query::query_ast::{
    BuildTantivyAstContext, FieldPresenceQuery, FullTextQuery, PhrasePrefixQuery, QueryAst,
    QueryAstTransformer, QueryAstVisitor, RangeQuery, RegexQuery, TermSetQuery, WildcardQuery,
};
use quickwit_query::tokenizers::TokenizerManager;
use quickwit_query::{InvalidQuery, find_field_or_hit_dynamic};
use tantivy::Term;
use tantivy::query::Query;
use tantivy::schema::{Field, Schema};
use tracing::error;

use crate::doc_mapper::FastFieldWarmupInfo;
use crate::{Automaton, QueryParserError, TermRange, WarmupInfo};

#[derive(Default)]
struct RangeQueryFields {
    range_query_field_names: HashSet<String>,
}

impl<'a> QueryAstVisitor<'a> for RangeQueryFields {
    type Err = Infallible;

    fn visit_range(&mut self, range_query: &'a RangeQuery) -> Result<(), Infallible> {
        self.range_query_field_names
            .insert(range_query.field.to_string());
        Ok(())
    }
}

/// Term Queries on fields which are fast but not indexed.
struct TermSearchOnColumnar<'f> {
    fields: &'f mut HashSet<FastFieldWarmupInfo>,
    schema: Schema,
}
impl<'a, 'f> QueryAstVisitor<'a> for TermSearchOnColumnar<'f> {
    type Err = Infallible;

    fn visit_term_set(&mut self, term_set_query: &'a TermSetQuery) -> Result<(), Infallible> {
        for field in term_set_query.terms_per_field.keys() {
            if let Some((_field, field_entry, path)) =
                find_field_or_hit_dynamic(field, &self.schema)
                && field_entry.is_fast()
                && !field_entry.is_indexed()
            {
                self.fields.insert(FastFieldWarmupInfo {
                    name: if path.is_empty() {
                        field_entry.name().to_string()
                    } else {
                        format!("{}.{}", field_entry.name(), path)
                    },
                    with_subfields: false,
                });
            }
        }
        Ok(())
    }

    fn visit_term(
        &mut self,
        term_query: &'a quickwit_query::query_ast::TermQuery,
    ) -> Result<(), Infallible> {
        if let Some((_field, field_entry, path)) =
            find_field_or_hit_dynamic(&term_query.field, &self.schema)
            && field_entry.is_fast()
            && !field_entry.is_indexed()
        {
            self.fields.insert(FastFieldWarmupInfo {
                name: if path.is_empty() {
                    field_entry.name().to_string()
                } else {
                    format!("{}.{}", field_entry.name(), path)
                },
                with_subfields: false,
            });
        }
        Ok(())
    }
    /// We also need to visit full text queries because they can be converted to term queries
    /// on fast fields. We only care about the field being fast and not indexed AND the tokenizer
    /// being `raw` or None.
    fn visit_full_text(&mut self, full_text_query: &'a FullTextQuery) -> Result<(), Infallible> {
        if let Some((_field, field_entry, path)) =
            find_field_or_hit_dynamic(&full_text_query.field, &self.schema)
            && field_entry.is_fast()
            && !field_entry.is_indexed()
            && (full_text_query.params.tokenizer.is_none()
                || full_text_query.params.tokenizer.as_deref() == Some("raw"))
        {
            self.fields.insert(FastFieldWarmupInfo {
                name: if path.is_empty() {
                    field_entry.name().to_string()
                } else {
                    format!("{}.{}", field_entry.name(), path)
                },
                with_subfields: false,
            });
        }
        Ok(())
    }
}

struct ExistsQueryFastFields<'f> {
    fields: &'f mut HashSet<FastFieldWarmupInfo>,
    schema: Schema,
}

impl<'a, 'f> QueryAstVisitor<'a> for ExistsQueryFastFields<'f> {
    type Err = Infallible;

    fn visit_exists(&mut self, exists_query: &'a FieldPresenceQuery) -> Result<(), Infallible> {
        let fields = exists_query.find_field_and_subfields(&self.schema);
        for (_, field_entry, path) in fields {
            if field_entry.is_fast() {
                if field_entry.field_type().is_json() {
                    let full_path = format!("{}.{}", field_entry.name(), path);
                    self.fields.insert(FastFieldWarmupInfo {
                        name: full_path,
                        with_subfields: true,
                    });
                } else if path.is_empty() {
                    self.fields.insert(FastFieldWarmupInfo {
                        name: field_entry.name().to_string(),
                        with_subfields: false,
                    });
                } else {
                    error!(
                        field_entry = field_entry.name(),
                        path, "only JSON type supports subfields"
                    );
                }
            }
        }
        Ok(())
    }
}

/// Build a `Query` with field resolution & forbidding range clauses.
pub(crate) fn build_query(
    query_ast: QueryAst,
    context: &BuildTantivyAstContext,
    cache_context: Option<(Arc<dyn quickwit_query::query_ast::PredicateCache>, SplitId)>,
) -> Result<(Box<dyn Query>, WarmupInfo), QueryParserError> {
    let mut fast_fields: HashSet<FastFieldWarmupInfo> = HashSet::new();

    let query_ast = if let Some((cache, split_id)) = cache_context {
        let Ok(query_ast) = quickwit_query::query_ast::PredicateCacheInjector { cache, split_id }
            .transform(query_ast);
        // this transformer isn't supposed to ever remove a node
        query_ast.unwrap_or(QueryAst::MatchAll)
    } else {
        query_ast
    };

    let mut range_query_fields = RangeQueryFields::default();
    // This cannot fail. The error type is Infallible.
    let Ok(_) = range_query_fields.visit(&query_ast);
    let range_query_fast_fields =
        range_query_fields
            .range_query_field_names
            .into_iter()
            .map(|name| FastFieldWarmupInfo {
                name,
                with_subfields: false,
            });
    fast_fields.extend(range_query_fast_fields);

    let Ok(_) = TermSearchOnColumnar {
        fields: &mut fast_fields,
        schema: context.schema.clone(),
    }
    .visit(&query_ast);

    let Ok(_) = ExistsQueryFastFields {
        fields: &mut fast_fields,
        schema: context.schema.clone(),
    }
    .visit(&query_ast);

    let query = query_ast.build_tantivy_query(context)?;

    let term_set_query_fields = extract_term_set_query_fields(&query_ast, context.schema)?;
    let (term_ranges_grouped_by_field, automatons_grouped_by_field) =
        extract_prefix_term_ranges_and_automaton(
            &query_ast,
            context.schema,
            context.tokenizer_manager,
        )?;

    let mut terms_grouped_by_field: HashMap<Field, HashMap<_, bool>> = Default::default();
    query.query_terms(&mut |term, need_position| {
        let field = term.field();
        if !context.schema.get_field_entry(field).is_indexed() {
            return;
        }
        *terms_grouped_by_field
            .entry(field)
            .or_default()
            .entry(term.clone())
            .or_default() |= need_position;
    });

    let warmup_info = WarmupInfo {
        term_dict_fields: term_set_query_fields,
        terms_grouped_by_field,
        term_ranges_grouped_by_field,
        fast_fields,
        automatons_grouped_by_field,
        ..WarmupInfo::default()
    };

    Ok((query, warmup_info))
}

struct ExtractTermSetFields<'a> {
    term_dict_fields_to_warm_up: HashSet<Field>,
    schema: &'a Schema,
}

impl<'a> ExtractTermSetFields<'a> {
    fn new(schema: &'a Schema) -> Self {
        ExtractTermSetFields {
            term_dict_fields_to_warm_up: HashSet::new(),
            schema,
        }
    }
}

impl<'a> QueryAstVisitor<'a> for ExtractTermSetFields<'_> {
    type Err = anyhow::Error;

    fn visit_term_set(&mut self, term_set_query: &'a TermSetQuery) -> anyhow::Result<()> {
        for field in term_set_query.terms_per_field.keys() {
            if let Some((field, _field_entry, _path)) =
                find_field_or_hit_dynamic(field, self.schema)
            {
                self.term_dict_fields_to_warm_up.insert(field);
            } else {
                anyhow::bail!("field does not exist: {}", field);
            }
        }
        Ok(())
    }
}

fn extract_term_set_query_fields(
    query_ast: &QueryAst,
    schema: &Schema,
) -> anyhow::Result<HashSet<Field>> {
    let mut visitor = ExtractTermSetFields::new(schema);
    visitor.visit(query_ast)?;
    Ok(visitor.term_dict_fields_to_warm_up)
}

/// Converts a `prefix` term into the equivalent term range.
///
/// The resulting range is `[prefix, next_prefix)`, that is:
/// - start bound: `Included(prefix)`
/// - end bound: `Excluded(next lexicographic term after the prefix)`
///
/// "abc"    -> start: "abc", end: "abd" (excluded)
/// "ab\xFF" -> start: "ab\xFF", end: "ac" (excluded)
/// "\xFF\xFF" -> start: "\xFF\xFF", end: Unbounded
fn prefix_term_to_range(prefix: Term) -> (Bound<Term>, Bound<Term>) {
    // Start from the given prefix and try to find the successor
    let mut end_bound = prefix.clone();
    let mut end_bound_value_bytes = prefix.serialized_value_bytes().to_vec();
    while !end_bound_value_bytes.is_empty() {
        let last_byte = end_bound_value_bytes.last_mut().unwrap();
        if *last_byte != u8::MAX {
            *last_byte += 1;
            // The last non-`u8::MAX` byte incremented
            // gives us the exclusive upper bound.
            end_bound.set_bytes(&end_bound_value_bytes);
            return (Bound::Included(prefix), Bound::Excluded(end_bound));
        }
        // pop u8::MAX byte and try next
        end_bound_value_bytes.pop();
    }
    // All bytes were `u8::MAX`: there is no successor, so the upper bound is unbounded.
    (Bound::Included(prefix), Bound::Unbounded)
}

type PositionNeeded = bool;

struct ExtractPrefixTermRanges<'a> {
    schema: &'a Schema,
    tokenizer_manager: &'a TokenizerManager,
    term_ranges_to_warm_up: HashMap<Field, HashMap<TermRange, PositionNeeded>>,
    automatons_to_warm_up: HashMap<Field, HashSet<Automaton>>,
}

impl<'a> ExtractPrefixTermRanges<'a> {
    fn with_schema(schema: &'a Schema, tokenizer_manager: &'a TokenizerManager) -> Self {
        ExtractPrefixTermRanges {
            schema,
            tokenizer_manager,
            term_ranges_to_warm_up: HashMap::new(),
            automatons_to_warm_up: HashMap::new(),
        }
    }

    fn add_prefix_term(
        &mut self,
        term: Term,
        max_expansions: u32,
        position_needed: PositionNeeded,
    ) {
        let field = term.field();
        let (start, end) = prefix_term_to_range(term);
        let term_range = TermRange {
            start,
            end,
            limit: Some(max_expansions as u64),
        };
        *self
            .term_ranges_to_warm_up
            .entry(field)
            .or_default()
            .entry(term_range)
            .or_default() |= position_needed;
    }

    fn add_automaton(&mut self, field: Field, automaton: Automaton) {
        self.automatons_to_warm_up
            .entry(field)
            .or_default()
            .insert(automaton);
    }
}

impl<'a, 'b: 'a> QueryAstVisitor<'a> for ExtractPrefixTermRanges<'b> {
    type Err = InvalidQuery;

    fn visit_full_text(&mut self, full_text_query: &'a FullTextQuery) -> Result<(), Self::Err> {
        if let Some(prefix_term) =
            full_text_query.get_prefix_term(self.schema, self.tokenizer_manager)
        {
            // the max_expansion expansion of a bool prefix query is used for the fuzzy part of the
            // query, not for the expension to a range request.
            // see https://github.com/elastic/elasticsearch/blob/6ad48306d029e6e527c0481e2e9880bd2f06b239/docs/reference/query-dsl/match-bool-prefix-query.asciidoc#parameters
            self.add_prefix_term(prefix_term, u32::MAX, false);
        }
        Ok(())
    }

    fn visit_phrase_prefix(
        &mut self,
        phrase_prefix: &'a PhrasePrefixQuery,
    ) -> Result<(), Self::Err> {
        let terms = match phrase_prefix.get_terms(self.schema, self.tokenizer_manager) {
            Ok((_, terms)) => terms,
            Err(InvalidQuery::SchemaError(_)) | Err(InvalidQuery::FieldDoesNotExist { .. }) => {
                return Ok(());
            } /* the query will be nullified when casting to a tantivy ast */
            Err(e) => return Err(e),
        };
        if let Some((_, term)) = terms.last() {
            self.add_prefix_term(term.clone(), phrase_prefix.max_expansions, terms.len() > 1);
        }
        Ok(())
    }

    fn visit_wildcard(&mut self, wildcard_query: &'a WildcardQuery) -> Result<(), Self::Err> {
        let (field, path, regex) =
            match wildcard_query.to_regex(self.schema, self.tokenizer_manager) {
                Ok(res) => res,
                /* the query will be nullified when casting to a tantivy ast */
                Err(InvalidQuery::FieldDoesNotExist { .. }) => return Ok(()),
                Err(e) => return Err(e),
            };

        self.add_automaton(field, Automaton::Regex(path, regex));
        Ok(())
    }

    fn visit_regex(&mut self, regex_query: &'a RegexQuery) -> Result<(), Self::Err> {
        let (field, path, regex) = match regex_query.to_field_and_regex(self.schema) {
            Ok(res) => res,
            /* the query will be nullified when casting to a tantivy ast */
            Err(InvalidQuery::FieldDoesNotExist { .. }) => return Ok(()),
            Err(e) => return Err(e),
        };
        self.add_automaton(field, Automaton::Regex(path, regex));
        Ok(())
    }
}

type TermRangeWarmupInfo = HashMap<Field, HashMap<TermRange, PositionNeeded>>;
type AutomatonWarmupInfo = HashMap<Field, HashSet<Automaton>>;

fn extract_prefix_term_ranges_and_automaton(
    query_ast: &QueryAst,
    schema: &Schema,
    tokenizer_manager: &TokenizerManager,
) -> anyhow::Result<(TermRangeWarmupInfo, AutomatonWarmupInfo)> {
    let mut visitor = ExtractPrefixTermRanges::with_schema(schema, tokenizer_manager);
    visitor.visit(query_ast)?;
    Ok((
        visitor.term_ranges_to_warm_up,
        visitor.automatons_to_warm_up,
    ))
}

#[cfg(test)]
mod test {
    use std::ops::Bound;

    use quickwit_common::shared_consts::FIELD_PRESENCE_FIELD_NAME;
    use quickwit_query::query_ast::{
        BuildTantivyAstContext, FullTextMode, FullTextParams, PhrasePrefixQuery, QueryAstVisitor,
        UserInputQuery, query_ast_from_user_text,
    };
    use quickwit_query::{
        BooleanOperand, MatchAllOrNone, create_default_quickwit_tokenizer_manager,
    };
    use tantivy::Term;
    use tantivy::schema::{DateOptions, DateTimePrecision, FAST, INDEXED, STORED, Schema, TEXT};

    use super::{ExtractPrefixTermRanges, build_query};
    use crate::{DYNAMIC_FIELD_NAME, SOURCE_FIELD_NAME, TermRange};

    enum TestExpectation<'a> {
        Err(&'a str),
        Ok(&'a str),
    }

    fn make_schema(dynamic_mode: bool) -> Schema {
        let mut schema_builder = Schema::builder();
        schema_builder.add_i64_field(FIELD_PRESENCE_FIELD_NAME, INDEXED);
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("desc", TEXT | STORED);
        schema_builder.add_text_field("server.name", TEXT | STORED);
        schema_builder.add_text_field("server.mem", TEXT);
        schema_builder.add_bool_field("server.running", FAST | STORED | INDEXED);
        schema_builder.add_text_field(SOURCE_FIELD_NAME, TEXT);
        schema_builder.add_ip_addr_field("ip", FAST | STORED);
        schema_builder.add_ip_addr_field("ips", FAST);
        schema_builder.add_ip_addr_field("ip_notff", STORED);
        let date_options = DateOptions::default()
            .set_fast()
            .set_precision(DateTimePrecision::Milliseconds);
        schema_builder.add_date_field("dt", date_options);
        schema_builder.add_u64_field("u64_fast", FAST | STORED);
        schema_builder.add_i64_field("i64_fast", FAST | STORED);
        schema_builder.add_f64_field("f64_fast", FAST | STORED);
        schema_builder.add_json_field("json_fast", FAST);
        schema_builder.add_json_field("json_text", TEXT);
        if dynamic_mode {
            schema_builder.add_json_field(DYNAMIC_FIELD_NAME, TEXT);
        }
        schema_builder.build()
    }

    #[track_caller]
    fn check_build_query_dynamic_mode(
        user_query: &str,
        search_fields: Vec<String>,
        expected: TestExpectation,
    ) {
        check_build_query(user_query, search_fields, expected, true, false);
    }

    #[track_caller]
    fn check_build_query_static_mode(
        user_query: &str,
        search_fields: Vec<String>,
        expected: TestExpectation,
    ) {
        check_build_query(user_query, search_fields, expected, false, false);
    }

    #[track_caller]
    fn check_build_query_static_lenient_mode(
        user_query: &str,
        search_fields: Vec<String>,
        expected: TestExpectation,
    ) {
        check_build_query(user_query, search_fields, expected, false, true);
    }

    fn test_build_query(
        user_query: &str,
        search_fields: Vec<String>,
        dynamic_mode: bool,
        lenient: bool,
    ) -> Result<String, String> {
        let user_input_query = UserInputQuery {
            user_text: user_query.to_string(),
            default_fields: Some(search_fields),
            default_operator: BooleanOperand::And,
            lenient,
        };
        let query_ast = user_input_query
            .parse_user_query(&[])
            .map_err(|err| err.to_string())?;
        let schema = make_schema(dynamic_mode);
        let query_result = build_query(query_ast, &BuildTantivyAstContext::for_test(&schema), None);
        query_result
            .map(|query| format!("{query:?}"))
            .map_err(|err| err.to_string())
    }

    #[track_caller]
    fn check_build_query(
        user_query: &str,
        search_fields: Vec<String>,
        expected: TestExpectation,
        dynamic_mode: bool,
        lenient: bool,
    ) {
        let query_result = test_build_query(user_query, search_fields, dynamic_mode, lenient);
        match (query_result, expected) {
            (Err(query_err_msg), TestExpectation::Err(sub_str)) => {
                assert!(
                    query_err_msg.contains(sub_str),
                    "query error received is {query_err_msg}. it should contain {sub_str}"
                );
            }
            (Ok(query_str), TestExpectation::Ok(sub_str)) => {
                assert!(
                    query_str.contains(sub_str),
                    "error query parsing {query_str} should contain {sub_str}"
                );
            }
            (Err(error_msg), TestExpectation::Ok(expectation)) => {
                panic!("Expected `{expectation}` but got an error `{error_msg}`.");
            }
            (Ok(query_str), TestExpectation::Err(expected_error)) => {
                panic!("Expected the error `{expected_error}`, but got a success `{query_str}`");
            }
        }
    }

    #[test]
    fn test_build_query_dynamic_field() {
        check_build_query_dynamic_mode("*", Vec::new(), TestExpectation::Ok("All"));
        check_build_query_dynamic_mode(
            "foo:bar",
            Vec::new(),
            TestExpectation::Ok(
                r#"TermQuery(Term(field=16, type=Json, path=foo, type=Str, "bar"))"#,
            ),
        );
        check_build_query_dynamic_mode(
            "server.type:hpc server.mem:4GB",
            Vec::new(),
            TestExpectation::Ok("server.type"),
        );
        check_build_query_dynamic_mode(
            "title:[a TO b]",
            Vec::new(),
            TestExpectation::Err(
                "range queries are only supported for fast fields. (`title` is not a fast field)",
            ),
        );
        check_build_query_dynamic_mode(
            "title:{a TO b} desc:foo",
            Vec::new(),
            TestExpectation::Err(
                "range queries are only supported for fast fields. (`title` is not a fast field)",
            ),
        );
    }

    #[test]
    fn test_build_query_not_dynamic_mode() {
        check_build_query_static_mode("*", Vec::new(), TestExpectation::Ok("All"));
        check_build_query_static_mode(
            "foo:bar",
            Vec::new(),
            TestExpectation::Err("invalid query: field does not exist: `foo`"),
        );
        check_build_query_static_lenient_mode(
            "foo:bar",
            Vec::new(),
            TestExpectation::Ok("EmptyQuery"),
        );
        check_build_query_static_mode(
            "title:bar",
            Vec::new(),
            TestExpectation::Ok(r#"TermQuery(Term(field=1, type=Str, "bar"))"#),
        );
        check_build_query_static_mode(
            "bar",
            vec!["fieldnotinschema".to_string()],
            TestExpectation::Err("invalid query: field does not exist: `fieldnotinschema`"),
        );
        check_build_query_static_lenient_mode(
            "bar",
            vec!["fieldnotinschema".to_string()],
            TestExpectation::Ok("EmptyQuery"),
        );
        check_build_query_static_mode(
            "title:[a TO b]",
            Vec::new(),
            TestExpectation::Err(
                "range queries are only supported for fast fields. (`title` is not a fast field)",
            ),
        );
        check_build_query_static_mode(
            "title:{a TO b} desc:foo",
            Vec::new(),
            TestExpectation::Err(
                "range queries are only supported for fast fields. (`title` is not a fast field)",
            ),
        );
        check_build_query_static_mode(
            "title:>foo",
            Vec::new(),
            TestExpectation::Err(
                "range queries are only supported for fast fields. (`title` is not a fast field)",
            ),
        );
        check_build_query_static_mode(
            "title:foo desc:bar _source:baz",
            Vec::new(),
            TestExpectation::Ok("TermQuery"),
        );
        check_build_query_static_mode(
            "server.name:\".bar:\" server.mem:4GB",
            vec!["server.name".to_string()],
            TestExpectation::Ok("TermQuery"),
        );
        check_build_query_static_mode(
            "server.name:\"for.bar:b\" server.mem:4GB",
            Vec::new(),
            TestExpectation::Ok("TermQuery"),
        );
        check_build_query_static_mode(
            "foo",
            Vec::new(),
            TestExpectation::Err("query requires a default search field and none was supplied"),
        );
        check_build_query_static_mode(
            "bar",
            Vec::new(),
            TestExpectation::Err("query requires a default search field and none was supplied"),
        );
        check_build_query_static_mode(
            "title:hello AND (Jane OR desc:world)",
            Vec::new(),
            TestExpectation::Err("query requires a default search field and none was supplied"),
        );
        check_build_query_static_mode(
            "server.running:true",
            Vec::new(),
            TestExpectation::Ok("TermQuery"),
        );
        check_build_query_static_mode(
            "title: IN [hello]",
            Vec::new(),
            TestExpectation::Ok("TermSetQuery"),
        );
        check_build_query_static_mode(
            "IN [hello]",
            Vec::new(),
            TestExpectation::Err("set query need to target a specific field"),
        );
    }

    #[test]
    fn test_wildcard_query() {
        check_build_query_static_mode("title:hello*", Vec::new(), TestExpectation::Ok("Regex"));
        check_build_query_static_mode(
            "title:\"hello world\"*",
            Vec::new(),
            TestExpectation::Ok("PhrasePrefixQuery"),
        );
        // the tokenizer removes '*' chars, making it a simple PhraseQuery (not RegexPhraseQuery)
        check_build_query_static_mode(
            "title:\"hello* world*\"",
            Vec::new(),
            TestExpectation::Ok("PhraseQuery"),
        );
        check_build_query_static_mode(
            "foo:bar*",
            Vec::new(),
            TestExpectation::Err("invalid query: field does not exist: `foo`"),
        );
        check_build_query_static_mode("title:hello*yo", Vec::new(), TestExpectation::Ok("Regex"));
    }

    #[test]
    fn test_existence_query() {
        check_build_query_static_mode(
            "title:*",
            Vec::new(),
            TestExpectation::Ok("TermQuery(Term(field=0, type=U64"),
        );

        check_build_query_static_mode(
            "ip:*",
            Vec::new(),
            TestExpectation::Ok("ExistsQuery { field_name: \"ip\", json_subpaths: true }"),
        );
        check_build_query_static_mode(
            "json_text:*",
            Vec::new(),
            TestExpectation::Ok("TermSetQuery"),
        );
        check_build_query_static_mode(
            "json_fast:*",
            Vec::new(),
            TestExpectation::Ok("ExistsQuery { field_name: \"json_fast\", json_subpaths: true }"),
        );
        check_build_query_static_mode(
            "foo:*",
            Vec::new(),
            TestExpectation::Err("invalid query: field does not exist: `foo`"),
        );
        check_build_query_static_mode(
            "server:*",
            Vec::new(),
            TestExpectation::Ok("BooleanQuery { subqueries: [(Should, TermQuery(Term"),
        );
    }

    #[test]
    fn test_datetime_range_query() {
        {
            // Check range on datetime in millisecond, precision has no impact as it is in
            // milliseconds.
            let start_date_time_str = "2023-01-10T08:38:51.150Z";
            let end_date_time_str = "2023-01-10T08:38:51.160Z";
            check_build_query_static_mode(
                &format!("dt:[{start_date_time_str} TO {end_date_time_str}]"),
                Vec::new(),
                TestExpectation::Ok("2023-01-10T08:38:51.15Z"),
            );
            check_build_query_static_mode(
                &format!("dt:[{start_date_time_str} TO {end_date_time_str}]"),
                Vec::new(),
                TestExpectation::Ok("RangeQuery"),
            );
            check_build_query_static_mode(
                &format!("dt:<{end_date_time_str}"),
                Vec::new(),
                TestExpectation::Ok("lower_bound: Unbounded"),
            );
            check_build_query_static_mode(
                &format!("dt:<{end_date_time_str}"),
                Vec::new(),
                TestExpectation::Ok("upper_bound: Excluded"),
            );
            check_build_query_static_mode(
                &format!("dt:<{end_date_time_str}"),
                Vec::new(),
                TestExpectation::Ok("2023-01-10T08:38:51.16Z"),
            );
        }

        // Check range on datetime in microseconds and truncation to milliseconds.
        {
            let start_date_time_str = "2023-01-10T08:38:51.000150Z";
            let end_date_time_str = "2023-01-10T08:38:51.000151Z";
            check_build_query_static_mode(
                &format!("dt:[{start_date_time_str} TO {end_date_time_str}]"),
                Vec::new(),
                TestExpectation::Ok("2023-01-10T08:38:51Z"),
            );
        }
    }

    #[test]
    fn test_ip_range_query() {
        check_build_query_static_mode(
            "ip:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            TestExpectation::Ok(
                "RangeQuery { bounds: BoundsRange { lower_bound: Included(Term(field=7, \
                 type=IpAddr, ::ffff:127.0.0.1)), upper_bound: Included(Term(field=7, \
                 type=IpAddr, ::ffff:127.1.1.1)) } }",
            ),
        );
        check_build_query_static_mode(
            "ip:>127.0.0.1",
            Vec::new(),
            TestExpectation::Ok(
                "RangeQuery { bounds: BoundsRange { lower_bound: Excluded(Term(field=7, \
                 type=IpAddr, ::ffff:127.0.0.1)), upper_bound: Unbounded } }",
            ),
        );
    }

    #[test]
    fn test_f64_range_query() {
        check_build_query_static_mode(
            "f64_fast:[7.7 TO 77.7]",
            Vec::new(),
            TestExpectation::Ok(
                r#"RangeQuery { bounds: BoundsRange { lower_bound: Included(Term(field=13, type=F64, 7.7)), upper_bound: Included(Term(field=13, type=F64, 77.7)) } }"#,
            ),
        );
        check_build_query_static_mode(
            "f64_fast:>7",
            Vec::new(),
            TestExpectation::Ok(
                r#"RangeQuery { bounds: BoundsRange { lower_bound: Excluded(Term(field=13, type=F64, 7.0)), upper_bound: Unbounded } }"#,
            ),
        );
    }

    #[test]
    fn test_i64_range_query() {
        check_build_query_static_mode(
            "i64_fast:[-7 TO 77]",
            Vec::new(),
            TestExpectation::Ok(r#"field=12"#),
        );
        check_build_query_static_mode(
            "i64_fast:>7",
            Vec::new(),
            TestExpectation::Ok(r#"field=12"#),
        );
    }

    #[test]
    fn test_u64_range_query() {
        check_build_query_static_mode(
            "u64_fast:[7 TO 77]",
            Vec::new(),
            TestExpectation::Ok(r#"field=11,"#),
        );
        check_build_query_static_mode(
            "u64_fast:>7",
            Vec::new(),
            TestExpectation::Ok(r#"field=11,"#),
        );
    }

    #[test]
    fn test_range_query_ip_fields_multivalued() {
        check_build_query_static_mode(
            "ips:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            TestExpectation::Ok(
                "RangeQuery { bounds: BoundsRange { lower_bound: Included(Term(field=8, \
                 type=IpAddr, ::ffff:127.0.0.1)), upper_bound: Included(Term(field=8, \
                 type=IpAddr, ::ffff:127.1.1.1)) } }",
            ),
        );
    }

    #[test]
    fn test_range_query_no_fast_field() {
        check_build_query_static_mode(
            "ip_notff:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            TestExpectation::Err("`ip_notff` is not a fast field"),
        );
    }

    #[test]
    fn test_build_query_not_bool_should_fail() {
        check_build_query_static_mode(
            "server.running:notabool",
            Vec::new(),
            TestExpectation::Err("expected a `bool` search value for field `server.running`"),
        );
    }

    #[test]
    fn test_build_query_warmup_info() {
        let query_with_set = query_ast_from_user_text("desc: IN [hello]", None)
            .parse_user_query(&[])
            .unwrap();
        let query_without_set = query_ast_from_user_text("desc:hello", None)
            .parse_user_query(&[])
            .unwrap();

        let schema = make_schema(true);
        let context = BuildTantivyAstContext::for_test(&schema);

        let (_, warmup_info) = build_query(query_with_set, &context, None).unwrap();
        assert_eq!(warmup_info.term_dict_fields.len(), 1);
        assert!(
            warmup_info
                .term_dict_fields
                .contains(&tantivy::schema::Field::from_field_id(2))
        );

        let (_, warmup_info) = build_query(query_without_set, &context, None).unwrap();
        assert!(warmup_info.term_dict_fields.is_empty());
    }

    #[test]
    fn test_extract_phrase_prefix_position_required() {
        let schema = make_schema(false);
        let tokenizer_manager = create_default_quickwit_tokenizer_manager();

        let params = FullTextParams {
            tokenizer: None,
            mode: FullTextMode::Phrase { slop: 0 },
            zero_terms_query: MatchAllOrNone::MatchNone,
        };
        let short = PhrasePrefixQuery {
            field: "title".to_string(),
            phrase: "short".to_string(),
            max_expansions: 50,
            params: params.clone(),
            lenient: false,
        };
        let long = PhrasePrefixQuery {
            field: "title".to_string(),
            phrase: "not so short".to_string(),
            max_expansions: 50,
            params: params.clone(),
            lenient: false,
        };
        let mut extractor1 = ExtractPrefixTermRanges::with_schema(&schema, &tokenizer_manager);
        extractor1.visit_phrase_prefix(&short).unwrap();
        extractor1.visit_phrase_prefix(&long).unwrap();

        let mut extractor2 = ExtractPrefixTermRanges::with_schema(&schema, &tokenizer_manager);
        extractor2.visit_phrase_prefix(&long).unwrap();
        extractor2.visit_phrase_prefix(&short).unwrap();

        assert_eq!(
            extractor1.term_ranges_to_warm_up,
            extractor2.term_ranges_to_warm_up
        );

        let field = tantivy::schema::Field::from_field_id(1);
        let mut expected_inner = std::collections::HashMap::new();
        expected_inner.insert(
            TermRange {
                start: Bound::Included(Term::from_field_text(field, "short")),
                end: Bound::Excluded(Term::from_field_text(field, "shoru")),
                limit: Some(50),
            },
            true,
        );
        let mut expected = std::collections::HashMap::new();
        expected.insert(field, expected_inner);
        assert_eq!(extractor1.term_ranges_to_warm_up, expected);
    }
}
