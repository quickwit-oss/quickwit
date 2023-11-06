// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::ops::Bound;

use quickwit_query::query_ast::{
    FieldPresenceQuery, FullTextQuery, PhrasePrefixQuery, QueryAst, QueryAstVisitor, RangeQuery,
    TermSetQuery, WildcardQuery,
};
use quickwit_query::tokenizers::TokenizerManager;
use quickwit_query::{find_field_or_hit_dynamic, InvalidQuery};
use tantivy::query::Query;
use tantivy::schema::{Field, Schema};
use tantivy::Term;

use crate::{QueryParserError, TermRange, WarmupInfo};

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

#[derive(Default)]
struct ExistsQueryFields {
    exists_query_field_names: HashSet<String>,
}

impl<'a> QueryAstVisitor<'a> for ExistsQueryFields {
    type Err = Infallible;

    fn visit_exists(&mut self, exists_query: &'a FieldPresenceQuery) -> Result<(), Infallible> {
        // If the field is a fast field, we will rely on the `ColumnIndex`.
        // If the field is not a fast, we will rely on the field presence field.
        //
        // After all field names are collected they are checked against schema and
        // non-fast fields are removed from warmup operation.
        self.exists_query_field_names
            .insert(exists_query.field.to_string());
        Ok(())
    }
}

/// Build a `Query` with field resolution & forbidding range clauses.
pub(crate) fn build_query(
    query_ast: &QueryAst,
    schema: Schema,
    tokenizer_manager: &TokenizerManager,
    search_fields: &[String],
    with_validation: bool,
) -> Result<(Box<dyn Query>, WarmupInfo), QueryParserError> {
    let mut range_query_fields = RangeQueryFields::default();
    // This cannot fail. The error type is Infallible.
    let _: Result<(), Infallible> = range_query_fields.visit(query_ast);

    let mut exists_query_fields = ExistsQueryFields::default();
    // This cannot fail. The error type is Infallible.
    let _: Result<(), Infallible> = exists_query_fields.visit(query_ast);

    let mut fast_field_names = HashSet::new();
    fast_field_names.extend(range_query_fields.range_query_field_names);
    fast_field_names.extend(
        exists_query_fields
            .exists_query_field_names
            .into_iter()
            .filter(|field| is_fast_field(&schema, field)),
    );

    let query = query_ast.build_tantivy_query(
        &schema,
        tokenizer_manager,
        search_fields,
        with_validation,
    )?;

    let term_set_query_fields = extract_term_set_query_fields(query_ast);
    let term_ranges_grouped_by_field =
        extract_prefix_term_ranges(query_ast, &schema, tokenizer_manager)?;

    let mut terms_grouped_by_field: HashMap<Field, HashMap<_, bool>> = Default::default();
    query.query_terms(&mut |term, need_position| {
        let field = term.field();
        *terms_grouped_by_field
            .entry(field)
            .or_default()
            .entry(term.clone())
            .or_default() |= need_position;
    });

    let warmup_info = WarmupInfo {
        term_dict_field_names: term_set_query_fields.clone(),
        posting_field_names: term_set_query_fields,
        terms_grouped_by_field,
        term_ranges_grouped_by_field,
        fast_field_names,
        ..WarmupInfo::default()
    };

    Ok((query, warmup_info))
}

fn is_fast_field(schema: &Schema, field_name: &str) -> bool {
    if let Ok((_field, field_entry, _path)) = find_field_or_hit_dynamic(field_name, schema) {
        return field_entry.is_fast();
    }
    false
}

#[derive(Default)]
struct ExtractTermSetFields {
    term_dict_fields_to_warm_up: HashSet<String>,
}

impl<'a> QueryAstVisitor<'a> for ExtractTermSetFields {
    type Err = anyhow::Error;

    fn visit_term_set(&mut self, term_set_query: &'a TermSetQuery) -> anyhow::Result<()> {
        for field in term_set_query.terms_per_field.keys() {
            self.term_dict_fields_to_warm_up.insert(field.to_string());
        }
        Ok(())
    }
}

fn extract_term_set_query_fields(query_ast: &QueryAst) -> HashSet<String> {
    let mut visitor = ExtractTermSetFields::default();
    visitor
        .visit(query_ast)
        .expect("Extracting term set queries's field should never return an error.");
    visitor.term_dict_fields_to_warm_up
}

fn prefix_term_to_range(prefix: Term) -> (Bound<Term>, Bound<Term>) {
    let mut end_bound = prefix.serialized_term().to_vec();
    while !end_bound.is_empty() {
        let last_byte = end_bound.last_mut().unwrap();
        if *last_byte != u8::MAX {
            *last_byte += 1;
            return (
                Bound::Included(prefix),
                Bound::Excluded(Term::wrap(end_bound)),
            );
        }
        end_bound.pop();
    }
    // prefix is something like [255, 255, ..]
    (Bound::Included(prefix), Bound::Unbounded)
}

type PositionNeeded = bool;

struct ExtractPrefixTermRanges<'a> {
    schema: &'a Schema,
    tokenizer_manager: &'a TokenizerManager,
    term_ranges_to_warm_up: HashMap<Field, HashMap<TermRange, PositionNeeded>>,
}

impl<'a> ExtractPrefixTermRanges<'a> {
    fn with_schema(schema: &'a Schema, tokenizer_manager: &'a TokenizerManager) -> Self {
        ExtractPrefixTermRanges {
            schema,
            tokenizer_manager,
            term_ranges_to_warm_up: HashMap::new(),
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
        self.term_ranges_to_warm_up
            .entry(field)
            .or_default()
            .insert(term_range, position_needed);
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
        let (_, terms) = phrase_prefix.get_terms(self.schema, self.tokenizer_manager)?;
        if let Some((_, term)) = terms.last() {
            self.add_prefix_term(term.clone(), phrase_prefix.max_expansions, terms.len() > 1);
        }
        Ok(())
    }

    fn visit_wildcard(&mut self, wildcard_query: &'a WildcardQuery) -> Result<(), Self::Err> {
        let (_, term) = wildcard_query.extract_prefix_term(self.schema, self.tokenizer_manager)?;
        self.add_prefix_term(term, u32::MAX, false);
        Ok(())
    }
}

fn extract_prefix_term_ranges(
    query_ast: &QueryAst,
    schema: &Schema,
    tokenizer_manager: &TokenizerManager,
) -> anyhow::Result<HashMap<Field, HashMap<TermRange, PositionNeeded>>> {
    let mut visitor = ExtractPrefixTermRanges::with_schema(schema, tokenizer_manager);
    visitor.visit(query_ast)?;
    Ok(visitor.term_ranges_to_warm_up)
}

#[cfg(test)]
mod test {
    use quickwit_query::create_default_quickwit_tokenizer_manager;
    use quickwit_query::query_ast::query_ast_from_user_text;
    use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};

    use super::build_query;
    use crate::{DYNAMIC_FIELD_NAME, SOURCE_FIELD_NAME};

    enum TestExpectation {
        Err(&'static str),
        Ok(&'static str),
    }

    fn make_schema(dynamic_mode: bool) -> Schema {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("desc", TEXT | STORED);
        schema_builder.add_text_field("server.name", TEXT | STORED);
        schema_builder.add_text_field("server.mem", TEXT);
        schema_builder.add_bool_field("server.running", FAST | STORED | INDEXED);
        schema_builder.add_text_field(SOURCE_FIELD_NAME, TEXT);
        schema_builder.add_ip_addr_field("ip", FAST | STORED);
        schema_builder.add_ip_addr_field("ips", FAST);
        schema_builder.add_ip_addr_field("ip_notff", STORED);
        schema_builder.add_date_field("dt", FAST);
        schema_builder.add_u64_field("u64_fast", FAST | STORED);
        schema_builder.add_i64_field("i64_fast", FAST | STORED);
        schema_builder.add_f64_field("f64_fast", FAST | STORED);
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
        check_build_query(user_query, search_fields, expected, true);
    }

    #[track_caller]
    fn check_build_query_static_mode(
        user_query: &str,
        search_fields: Vec<String>,
        expected: TestExpectation,
    ) {
        check_build_query(user_query, search_fields, expected, false);
    }

    fn test_build_query(
        user_query: &str,
        search_fields: Vec<String>,
        dynamic_mode: bool,
    ) -> Result<String, String> {
        let query_ast = query_ast_from_user_text(user_query, Some(search_fields))
            .parse_user_query(&[])
            .map_err(|err| err.to_string())?;
        let schema = make_schema(dynamic_mode);
        let query_result = build_query(
            &query_ast,
            schema,
            &create_default_quickwit_tokenizer_manager(),
            &[],
            true,
        );
        query_result
            .map(|query| format!("{:?}", query))
            .map_err(|err| err.to_string())
    }

    #[track_caller]
    fn check_build_query(
        user_query: &str,
        search_fields: Vec<String>,
        expected: TestExpectation,
        dynamic_mode: bool,
    ) {
        let query_result = test_build_query(user_query, search_fields, dynamic_mode);
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
                r#"TermQuery(Term(field=13, type=Json, path=foo, type=Str, "bar"))"#,
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
        check_build_query_static_mode(
            "title:bar",
            Vec::new(),
            TestExpectation::Ok(r#"TermQuery(Term(field=0, type=Str, "bar"))"#),
        );
        check_build_query_static_mode(
            "bar",
            vec!["fieldnotinschema".to_string()],
            TestExpectation::Err("invalid query: field does not exist: `fieldnotinschema`"),
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
    fn test_datetime_range_query() {
        check_build_query_static_mode(
            "dt:[2023-01-10T15:13:35Z TO 2023-01-10T15:13:40Z]",
            Vec::new(),
            TestExpectation::Ok("RangeQuery { field: \"dt\", value_type: Date"),
        );
        check_build_query_static_mode(
            "dt:<2023-01-10T15:13:35Z",
            Vec::new(),
            TestExpectation::Ok("RangeQuery { field: \"dt\", value_type: Date"),
        );
    }

    #[test]
    fn test_ip_range_query() {
        check_build_query_static_mode(
            "ip:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            TestExpectation::Ok(
                "RangeQuery { field: \"ip\", value_type: IpAddr, lower_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1]), upper_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 1, 1, 1])",
            ),
        );
        check_build_query_static_mode(
            "ip:>127.0.0.1",
            Vec::new(),
            TestExpectation::Ok(
                "RangeQuery { field: \"ip\", value_type: IpAddr, lower_bound: Excluded([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1]), upper_bound: Unbounded",
            ),
        );
    }

    #[test]
    fn test_f64_range_query() {
        check_build_query_static_mode(
            "f64_fast:[7.7 TO 77.7]",
            Vec::new(),
            TestExpectation::Ok(
                r#"FastFieldRangeWeight { field: "f64_fast", lower_bound: Included(13843727484564851917), upper_bound: Included(13858540105214250189), column_type_opt: Some(F64) }"#,
            ),
        );
        check_build_query_static_mode(
            "f64_fast:>7",
            Vec::new(),
            TestExpectation::Ok(
                r#"FastFieldRangeWeight { field: "f64_fast", lower_bound: Excluded(13842939354630062080), upper_bound: Unbounded, column_type_opt: Some(F64)"#,
            ),
        );
    }

    #[test]
    fn test_i64_range_query() {
        check_build_query_static_mode(
            "i64_fast:[-7 TO 77]",
            Vec::new(),
            TestExpectation::Ok(r#"FastFieldRangeWeight { field: "i64_fast","#),
        );
        check_build_query_static_mode(
            "i64_fast:>7",
            Vec::new(),
            TestExpectation::Ok(r#"FastFieldRangeWeight { field: "i64_fast","#),
        );
    }

    #[test]
    fn test_u64_range_query() {
        check_build_query_static_mode(
            "u64_fast:[7 TO 77]",
            Vec::new(),
            TestExpectation::Ok(r#"FastFieldRangeWeight { field: "u64_fast","#),
        );
        check_build_query_static_mode(
            "u64_fast:>7",
            Vec::new(),
            TestExpectation::Ok(r#"FastFieldRangeWeight { field: "u64_fast","#),
        );
    }

    #[test]
    fn test_range_query_ip_fields_multivalued() {
        check_build_query_static_mode(
            "ips:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            TestExpectation::Ok(
                "RangeQuery { field: \"ips\", value_type: IpAddr, lower_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1]), upper_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 1, 1, 1])",
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
        let query_with_set = query_ast_from_user_text("title: IN [hello]", None)
            .parse_user_query(&[])
            .unwrap();
        let query_without_set = query_ast_from_user_text("title:hello", None)
            .parse_user_query(&[])
            .unwrap();

        let (_, warmup_info) = build_query(
            &query_with_set,
            make_schema(true),
            &create_default_quickwit_tokenizer_manager(),
            &[],
            true,
        )
        .unwrap();
        assert_eq!(warmup_info.term_dict_field_names.len(), 1);
        assert_eq!(warmup_info.posting_field_names.len(), 1);
        assert!(warmup_info.term_dict_field_names.contains("title"));
        assert!(warmup_info.posting_field_names.contains("title"));

        let (_, warmup_info) = build_query(
            &query_without_set,
            make_schema(true),
            &create_default_quickwit_tokenizer_manager(),
            &[],
            true,
        )
        .unwrap();
        assert!(warmup_info.term_dict_field_names.is_empty());
        assert!(warmup_info.posting_field_names.is_empty());
    }
}
