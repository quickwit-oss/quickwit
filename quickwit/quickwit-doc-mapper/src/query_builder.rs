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

use quickwit_proto::SearchRequest;
use quickwit_query::{SearchInputAst, build_query_from_search_input_ast};
use tantivy::query::Query;
use tantivy::schema::{Field, Schema};

use crate::{QueryParserError, WarmupInfo, QUICKWIT_TOKENIZER_MANAGER};

/// Build a `Query` with field resolution & forbidding range clauses.
pub(crate) fn build_query(
    schema: Schema,
    search_request: &SearchRequest,
    _default_field_names: &[String],
) -> Result<(Box<dyn Query>, WarmupInfo), QueryParserError> {
    let search_input_ast: SearchInputAst =
        serde_json::from_str(&search_request.query)
        .expect("Could not deserialize SearchInputAst");

    let search_fields: Vec<Field> = search_request
        .resolved_search_fields
        .iter()
        .map(|field_id| Field::from_field_id(*field_id)).collect();
    let query = build_query_from_search_input_ast(schema, search_fields, search_input_ast, QUICKWIT_TOKENIZER_MANAGER.clone())?;

    let mut terms_grouped_by_field: HashMap<Field, HashMap<_, bool>> = Default::default();
    query.query_terms(&mut |term, need_position| {
        let field = term.field();
        *terms_grouped_by_field
            .entry(field)
            .or_default()
            .entry(term.clone())
            .or_default() |= need_position;
    });

    let fast_field_names: HashSet<String> =
        search_request.fast_field_names.iter().cloned().collect();
    let term_set_query_fields: HashSet<String> =
        search_request.term_set_query_fields.iter().cloned().collect();
    let warmup_info = WarmupInfo {
        term_dict_field_names: term_set_query_fields.clone(),
        posting_field_names: term_set_query_fields,
        terms_grouped_by_field,
        fast_field_names,
        ..WarmupInfo::default()
    };

    Ok((query, warmup_info))
}

#[cfg(test)]
mod test {
    use quickwit_proto::SearchRequest;
    use quickwit_query::{validate_requested_snippet_fields, SearchInputAst};
    use tantivy::schema::{
        Cardinality, DateOptions, IpAddrOptions, Schema, FAST, INDEXED, STORED, TEXT,
    };

    use super::build_query;
    use crate::{DYNAMIC_FIELD_NAME, SOURCE_FIELD_NAME};

    enum TestExpectation {
        Err(&'static str),
        Ok(&'static str),
    }

    fn make_schema() -> Schema {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("desc", TEXT | STORED);
        schema_builder.add_text_field("server.name", TEXT | STORED);
        schema_builder.add_text_field("server.mem", TEXT);
        schema_builder.add_bool_field("server.running", FAST | STORED | INDEXED);
        schema_builder.add_text_field(SOURCE_FIELD_NAME, TEXT);
        schema_builder.add_json_field(DYNAMIC_FIELD_NAME, TEXT);
        schema_builder.add_ip_addr_field("ip", FAST | STORED);
        schema_builder.add_ip_addr_field(
            "ips",
            IpAddrOptions::default().set_fast(Cardinality::MultiValues),
        );
        schema_builder.add_ip_addr_field("ip_notff", STORED);
        schema_builder.add_date_field(
            "dt",
            DateOptions::default().set_fast(Cardinality::SingleValue),
        );
        schema_builder.add_u64_field("u64_fast", FAST | STORED);
        schema_builder.add_i64_field("i64_fast", FAST | STORED);
        schema_builder.add_f64_field("f64_fast", FAST | STORED);
        schema_builder.build()
    }

    #[track_caller]
    fn check_build_query(
        query_str: &str,
        search_fields: Vec<String>,
        default_search_fields: Option<Vec<String>>,
        expected: TestExpectation,
    ) -> anyhow::Result<()> {
        let request = SearchRequest {
            aggregation_request: None,
            index_id: "test_index".to_string(),
            query: query_str.to_string(),
            search_fields,
            snippet_fields: vec![],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 20,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            ..Default::default()
        };

        let default_field_names =
            default_search_fields.unwrap_or_else(|| vec!["title".to_string(), "desc".to_string()]);

        let query_result = build_query(make_schema(), &request, &default_field_names);
        match expected {
            TestExpectation::Err(sub_str) => {
                assert!(
                    query_result.is_err(),
                    "Expected error {sub_str}, but got a success on query parsing {query_str}"
                );
                let query_err = query_result.err().unwrap();
                assert!(
                    format!("{query_err:?}").contains(sub_str),
                    "Query error received is {query_err:?}. It should contain {sub_str}"
                );
            }
            TestExpectation::Ok(sub_str) => {
                assert!(
                    query_result.is_ok(),
                    "Expected a success when parsing {sub_str}, but got an error: {:?}",
                    query_result.err()
                );
                let (query, _) = query_result.unwrap();
                assert!(
                    format!("{query:?}").contains(sub_str),
                    "Error query parsing {query:?} should contain {sub_str}"
                );
            }
        }

        Ok(())
    }

    #[test]
    fn test_build_query() {
        check_build_query("*", vec![], None, TestExpectation::Ok("All")).unwrap();
        check_build_query(
            "foo:bar",
            vec![],
            None,
            TestExpectation::Err("Field does not exist: 'foo'"),
        )
        .unwrap();
        check_build_query(
            "server.type:hpc server.mem:4GB",
            vec![],
            None,
            TestExpectation::Err("Field does not exist: 'server.type'"),
        )
        .unwrap();
        check_build_query(
            "title:[a TO b]",
            vec![],
            None,
            TestExpectation::Err(
                "Field `title` is of type `Str`. Range queries are only supported on boolean, \
                 datetime, IP, and numeric fields",
            ),
        )
        .unwrap();
        check_build_query(
            "title:{a TO b} desc:foo",
            vec![],
            None,
            TestExpectation::Err(
                "Field `title` is of type `Str`. Range queries are only supported on boolean, \
                 datetime, IP, and numeric fields",
            ),
        )
        .unwrap();
        check_build_query(
            "title:>foo",
            vec![],
            None,
            TestExpectation::Err(
                "Field `title` is of type `Str`. Range queries are only supported on boolean, \
                 datetime, IP, and numeric fields",
            ),
        )
        .unwrap();
        check_build_query(
            "title:foo desc:bar _source:baz",
            vec![],
            None,
            TestExpectation::Ok("TermQuery"),
        )
        .unwrap();
        check_build_query(
            "title:foo desc:bar",
            vec!["url".to_string()],
            None,
            TestExpectation::Err("field does not exist: 'url'"),
        )
        .unwrap();
        check_build_query(
            "server.name:\".bar:\" server.mem:4GB",
            vec!["server.name".to_string()],
            None,
            TestExpectation::Ok("TermQuery"),
        )
        .unwrap();
        check_build_query(
            "server.name:\"for.bar:b\" server.mem:4GB",
            vec![],
            None,
            TestExpectation::Ok("TermQuery"),
        )
        .unwrap();
        check_build_query(
            "foo",
            vec![],
            Some(vec![]),
            TestExpectation::Err("No default field declared and no field specified in query."),
        )
        .unwrap();
        check_build_query(
            "bar",
            vec![],
            Some(vec![DYNAMIC_FIELD_NAME.to_string()]),
            TestExpectation::Err("No default field declared and no field specified in query."),
        )
        .unwrap();
        check_build_query(
            "title:hello AND (Jane OR desc:world)",
            vec![],
            Some(vec![DYNAMIC_FIELD_NAME.to_string()]),
            TestExpectation::Err("No default field declared and no field specified in query."),
        )
        .unwrap();
        check_build_query(
            "server.running:true",
            vec![],
            None,
            TestExpectation::Ok("TermQuery"),
        )
        .unwrap();
        check_build_query(
            "title: IN [hello]",
            vec![],
            None,
            TestExpectation::Ok("TermSetQuery"),
        )
        .unwrap();
        check_build_query(
            "IN [hello]",
            vec![],
            None,
            TestExpectation::Err("Unsupported query: Set query need to target a specific field."),
        )
        .unwrap();
    }

    #[test]
    fn test_datetime_range_query() {
        check_build_query(
            "dt:[2023-01-10T15:13:35Z TO 2023-01-10T15:13:40Z]",
            Vec::new(),
            None,
            TestExpectation::Ok("RangeQuery { field: \"dt\", value_type: Date"),
        )
        .unwrap();
        check_build_query(
            "dt:<2023-01-10T15:13:35Z",
            Vec::new(),
            None,
            TestExpectation::Ok("RangeQuery { field: \"dt\", value_type: Date"),
        )
        .unwrap();
    }

    #[test]
    fn test_ip_range_query() {
        check_build_query(
            "ip:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            None,
            TestExpectation::Ok(
                "RangeQuery { field: \"ip\", value_type: IpAddr, left_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1]), right_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 1, 1, 1]) }",
            ),
        )
        .unwrap();
        check_build_query(
            "ip:>127.0.0.1",
            Vec::new(),
            None,
            TestExpectation::Ok(
                "RangeQuery { field: \"ip\", value_type: IpAddr, left_bound: Excluded([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1]), right_bound: Unbounded }",
            ),
        )
        .unwrap();
    }

    #[test]
    fn test_f64_range_query() {
        check_build_query(
            "f64_fast:[7.7 TO 77.7]",
            Vec::new(),
            None,
            TestExpectation::Ok("RangeQuery { field: \"f64_fast\", value_type: F64"),
        )
        .unwrap();
        check_build_query(
            "f64_fast:>7",
            Vec::new(),
            None,
            TestExpectation::Ok("RangeQuery { field: \"f64_fast\", value_type: F64"),
        )
        .unwrap();
    }

    #[test]
    fn test_i64_range_query() {
        check_build_query(
            "i64_fast:[-7 TO 77]",
            Vec::new(),
            None,
            TestExpectation::Ok("RangeQuery { field: \"i64_fast\", value_type: I64"),
        )
        .unwrap();
        check_build_query(
            "i64_fast:>7",
            Vec::new(),
            None,
            TestExpectation::Ok("RangeQuery { field: \"i64_fast\", value_type: I64"),
        )
        .unwrap();
    }

    #[test]
    fn test_u64_range_query() {
        check_build_query(
            "u64_fast:[7 TO 77]",
            Vec::new(),
            None,
            TestExpectation::Ok("RangeQuery { field: \"u64_fast\", value_type: U64"),
        )
        .unwrap();
        check_build_query(
            "u64_fast:>7",
            Vec::new(),
            None,
            TestExpectation::Ok("RangeQuery { field: \"u64_fast\", value_type: U64"),
        )
        .unwrap();
    }

    #[test]
    fn test_range_query_ip_fields_multivalued() {
        check_build_query(
            "ips:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            None,
            TestExpectation::Ok(
                "RangeQuery { field: \"ips\", value_type: IpAddr, left_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1]), right_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 1, 1, 1]) }",
            ),
        )
        .unwrap();
    }

    #[test]
    fn test_range_query_no_fast_field() {
        check_build_query(
            "ip_notff:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            None,
            TestExpectation::Err("field `ip_notff` is not declared as a fast field"),
        )
        .unwrap();
    }

    #[track_caller]
    fn check_snippet_fields_validation(
        query_str: &str,
        search_fields: Vec<String>,
        snippet_fields: Vec<String>,
        default_search_fields: Option<Vec<String>>,
    ) -> anyhow::Result<()> {
        let schema = make_schema();
        let request = SearchRequest {
            aggregation_request: None,
            index_id: "test_index".to_string(),
            query: query_str.to_string(),
            search_fields,
            snippet_fields,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 20,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            ..Default::default()
        };
        let search_input_ast = SearchInputAst::empty_query();
        let default_field_names =
            default_search_fields.unwrap_or_else(|| vec!["title".to_string(), "desc".to_string()]);

        validate_requested_snippet_fields(
            &schema,
            &request,
            &search_input_ast,
            &default_field_names,
        )
    }

    #[test]
    #[should_panic(expected = "provided string was not `true` or `false`")]
    fn test_build_query_not_bool_should_fail() {
        check_build_query(
            "server.running:not a bool",
            vec![],
            None,
            TestExpectation::Err("Expected a success when parsing TermQuery, but got error"),
        )
        .unwrap();
    }

    #[test]
    fn test_validate_requested_snippet_fields() {
        let validation_result =
            check_snippet_fields_validation("foo", vec![], vec!["desc".to_string()], None);
        assert!(validation_result.is_ok());
        let validation_result = check_snippet_fields_validation(
            "foo",
            vec![],
            vec!["desc".to_string()],
            Some(vec!["desc".to_string()]),
        );
        assert!(validation_result.is_ok());
        let validation_result = check_snippet_fields_validation(
            "desc:foo",
            vec![],
            vec!["desc".to_string()],
            Some(vec![]),
        );
        assert!(validation_result.is_ok());
        let validation_result = check_snippet_fields_validation(
            "foo",
            vec!["desc".to_string()],
            vec!["desc".to_string()],
            Some(vec![]),
        );
        assert!(validation_result.is_ok());

        // Non existing field
        let validation_result = check_snippet_fields_validation(
            "foo",
            vec!["summary".to_string()],
            vec!["summary".to_string()],
            None,
        );
        assert_eq!(
            validation_result.unwrap_err().to_string(),
            "The field does not exist: 'summary'"
        );
        // Unknown searched field
        let validation_result =
            check_snippet_fields_validation("foo", vec![], vec!["server.name".to_string()], None);
        assert_eq!(
            validation_result.unwrap_err().to_string(),
            "The snippet field `server.name` should be a default search field or appear in the \
             query."
        );
        // Search field in query
        let validation_result = check_snippet_fields_validation(
            "server.name:foo",
            vec![],
            vec!["server.name".to_string()],
            None,
        );
        assert!(validation_result.is_ok());
        // Not stored field
        let validation_result =
            check_snippet_fields_validation("foo", vec![], vec!["title".to_string()], None);
        assert_eq!(
            validation_result.unwrap_err().to_string(),
            "The snippet field `title` must be stored."
        );
        // Non text field
        let validation_result = check_snippet_fields_validation(
            "foo",
            vec!["server.running".to_string()],
            vec!["server.running".to_string()],
            None,
        );
        assert_eq!(
            validation_result.unwrap_err().to_string(),
            "The snippet field `server.running` must be of type `Str`, got `Bool`."
        );
    }

    #[test]
    fn test_build_query_warmup_info() -> anyhow::Result<()> {
        let request_with_set = SearchRequest {
            aggregation_request: None,
            index_id: "test_index".to_string(),
            query: "title: IN [hello]".to_string(),
            search_fields: vec![],
            snippet_fields: vec![],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 20,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            ..Default::default()
        };
        let request_without_set = SearchRequest {
            aggregation_request: None,
            index_id: "test_index".to_string(),
            query: "title:hello".to_string(),
            search_fields: vec![],
            snippet_fields: vec![],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 20,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            ..Default::default()
        };

        let default_field_names = vec!["title".to_string(), "desc".to_string()];

        let (_, warmup_info) = build_query(make_schema(), &request_with_set, &default_field_names)?;
        assert_eq!(warmup_info.term_dict_field_names.len(), 1);
        assert_eq!(warmup_info.posting_field_names.len(), 1);
        assert!(warmup_info.term_dict_field_names.contains("title"));
        assert!(warmup_info.posting_field_names.contains("title"));

        let (_, warmup_info) =
            build_query(make_schema(), &request_without_set, &default_field_names)?;
        assert!(warmup_info.term_dict_field_names.is_empty());
        assert!(warmup_info.posting_field_names.is_empty());

        Ok(())
    }
}
