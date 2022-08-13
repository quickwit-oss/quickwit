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

use quickwit_proto::SearchRequest;
use tantivy::query::{Query, QueryParser, QueryParserError as TantivyQueryParserError};
use tantivy::schema::{Field, Schema};
use tantivy_query_grammar::{UserInputAst, UserInputLeaf, UserInputLiteral};

use crate::sort_by::validate_sort_by_field_name;
use crate::{QueryParserError, DYNAMIC_FIELD_NAME, QUICKWIT_TOKENIZER_MANAGER};

/// Build a `Query` with field resolution & forbidding range clauses.
pub(crate) fn build_query(
    schema: Schema,
    request: &SearchRequest,
    default_field_names: &[String],
) -> Result<Box<dyn Query>, QueryParserError> {
    let user_input_ast = tantivy_query_grammar::parse_query(&request.query)
        .map_err(|_| TantivyQueryParserError::SyntaxError(request.query.to_string()))?;

    if has_range_clause(&user_input_ast) {
        return Err(anyhow::anyhow!("Range queries are not currently allowed.").into());
    }

    if needs_default_search_field(&user_input_ast)
        && request.search_fields.is_empty()
        && (default_field_names.is_empty() || default_field_names == [DYNAMIC_FIELD_NAME])
    {
        return Err(
            anyhow::anyhow!("No default field declared and no field specified in query.").into(),
        );
    }

    let search_fields = if request.search_fields.is_empty() {
        resolve_fields(&schema, default_field_names)?
    } else {
        resolve_fields(&schema, &request.search_fields)?
    };

    if let Some(sort_by_field) = request.sort_by_field.as_ref() {
        validate_sort_by_field_name(sort_by_field, &schema, Some(&search_fields))?;
    }

    let mut query_parser =
        QueryParser::new(schema, search_fields, QUICKWIT_TOKENIZER_MANAGER.clone());
    query_parser.set_conjunction_by_default();
    let query = query_parser.parse_query(&request.query)?;
    Ok(query)
}

fn has_range_clause(user_input_ast: &UserInputAst) -> bool {
    match user_input_ast {
        UserInputAst::Clause(sub_queries) => {
            for (_, sub_ast) in sub_queries {
                if has_range_clause(sub_ast) {
                    return true;
                }
            }
            false
        }
        UserInputAst::Boost(ast, _) => has_range_clause(ast),
        UserInputAst::Leaf(leaf) => matches!(**leaf, UserInputLeaf::Range { .. }),
    }
}

/// Tells if the query has a Term or Range node which does not
/// specify a search field.
fn needs_default_search_field(user_input_ast: &UserInputAst) -> bool {
    match user_input_ast {
        UserInputAst::Clause(sub_queries) => {
            for (_, sub_ast) in sub_queries {
                if needs_default_search_field(sub_ast) {
                    return true;
                }
            }
            false
        }
        UserInputAst::Boost(ast, _) => needs_default_search_field(ast),
        UserInputAst::Leaf(leaf) => match &**leaf {
            UserInputLeaf::Literal(UserInputLiteral { field_name, .. }) => field_name.is_none(),
            UserInputLeaf::Range { field, .. } => field.is_none(),
            _ => false,
        },
    }
}

fn resolve_fields(schema: &Schema, field_names: &[String]) -> anyhow::Result<Vec<Field>> {
    let mut fields = vec![];
    for field_name in field_names {
        let field = schema
            .get_field(field_name)
            .ok_or_else(|| TantivyQueryParserError::FieldDoesNotExist(field_name.clone()))?;
        fields.push(field);
    }
    Ok(fields)
}

#[cfg(test)]
mod test {
    use quickwit_proto::SearchRequest;
    use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};

    use super::build_query;
    use crate::{DYNAMIC_FIELD_NAME, SOURCE_FIELD_NAME};

    enum TestExpectation {
        Err(&'static str),
        Ok(&'static str),
    }

    fn make_schema() -> Schema {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("desc", TEXT);
        schema_builder.add_text_field("server.name", TEXT);
        schema_builder.add_text_field("server.mem", TEXT);
        schema_builder.add_bool_field("server.running", FAST | STORED | INDEXED);
        schema_builder.add_text_field(SOURCE_FIELD_NAME, TEXT);
        schema_builder.add_json_field(DYNAMIC_FIELD_NAME, TEXT);
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
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 20,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
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
                    "Query error received is {:?}. It should contain {}",
                    query_err,
                    sub_str
                );
            }
            TestExpectation::Ok(sub_str) => {
                assert!(
                    query_result.is_ok(),
                    "Expected a success when parsing {sub_str}, but got error"
                );
                let query = query_result.unwrap();
                assert!(
                    format!("{query:?}").contains(sub_str),
                    "Error query parsing {:?} should contain {}",
                    query,
                    sub_str
                );
            }
        }

        Ok(())
    }

    #[test]
    fn test_build_query() {
        check_build_query(
            "foo:bar",
            vec![],
            None,
            TestExpectation::Err("Field does not exists: 'foo'"),
        )
        .unwrap();
        check_build_query(
            "server.type:hpc server.mem:4GB",
            vec![],
            None,
            TestExpectation::Err("Field does not exists: 'server.type'"),
        )
        .unwrap();
        check_build_query(
            "title:[a TO b]",
            vec![],
            None,
            TestExpectation::Err("Range queries are not currently allowed."),
        )
        .unwrap();
        check_build_query(
            "title:{a TO b} desc:foo",
            vec![],
            None,
            TestExpectation::Err("Range queries are not currently allowed."),
        )
        .unwrap();
        check_build_query(
            "title:>foo",
            vec![],
            None,
            TestExpectation::Err("Range queries are not currently allowed."),
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
            TestExpectation::Err("Field does not exists: 'url'"),
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
}
