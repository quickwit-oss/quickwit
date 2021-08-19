/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use quickwit_proto::SearchRequest;
use tantivy::query::{Query, QueryParser, QueryParserError as TantivyQueryParserError};
use tantivy::schema::{Field, Schema};
use tantivy::tokenizer::TokenizerManager;
use tantivy_query_grammar::{UserInputAst, UserInputLeaf};

use crate::QueryParserError;

/// Build a `Query` with field resolution & forbidding range clauses.
pub(crate) fn build_query(
    schema: Schema,
    request: &SearchRequest,
    default_field_names: &[String],
) -> Result<Box<dyn Query>, QueryParserError> {
    let user_input_ast = tantivy_query_grammar::parse_query(&request.query)
        .map_err(|_| TantivyQueryParserError::SyntaxError)?;

    if has_range_clause(user_input_ast) {
        return Err(anyhow::anyhow!("Range queries are not currently allowed.").into());
    }

    let search_fields = if request.search_fields.is_empty() {
        resolve_fields(&schema, default_field_names)?
    } else {
        resolve_fields(&schema, &request.search_fields)?
    };

    let query_parser = QueryParser::new(schema, search_fields, TokenizerManager::default());
    let query = query_parser.parse_query(&request.query)?;
    Ok(query)
}

fn has_range_clause(user_input_ast: UserInputAst) -> bool {
    match user_input_ast {
        UserInputAst::Clause(sub_queries) => {
            for (_, sub_ast) in sub_queries {
                if has_range_clause(sub_ast) {
                    return true;
                }
            }
            false
        }
        UserInputAst::Boost(ast, _) => has_range_clause(*ast),
        UserInputAst::Leaf(leaf) => matches!(*leaf, UserInputLeaf::Range { .. }),
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
    use super::build_query;

    use quickwit_proto::SearchRequest;
    use tantivy::schema::{Schema, TEXT};

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
        schema_builder.add_text_field("_source", TEXT);
        schema_builder.build()
    }

    fn check_build_query(
        query_str: &str,
        search_fields: Vec<String>,
        expected: TestExpectation,
    ) -> anyhow::Result<()> {
        let request = SearchRequest {
            index_id: "test_index".to_string(),
            query: query_str.to_string(),
            search_fields,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 20,
            start_offset: 0,
            tags: vec![],
        };

        let default_field_names = vec!["title".to_string(), "desc".to_string()];

        let query_result = build_query(make_schema(), &request, &default_field_names);
        match expected {
            TestExpectation::Err(sub_str) => {
                assert_eq!(format!("{:?}", query_result).contains(sub_str), true);
            }
            TestExpectation::Ok(sub_str) => {
                let query = query_result?;
                assert_eq!(format!("{:?}", query).contains(sub_str), true);
            }
        }

        Ok(())
    }

    #[test]
    fn test_build_query() -> anyhow::Result<()> {
        check_build_query(
            "foo:bar",
            vec![],
            TestExpectation::Err("Field does not exists: '\"foo\"'"),
        )?;
        check_build_query(
            "title:[a TO b]",
            vec![],
            TestExpectation::Err("Range queries are not currently allowed."),
        )?;
        check_build_query(
            "title:{a TO b} desc:foo",
            vec![],
            TestExpectation::Err("Range queries are not currently allowed."),
        )?;
        check_build_query(
            "title:>foo",
            vec![],
            TestExpectation::Err("Range queries are not currently allowed."),
        )?;
        check_build_query(
            "title:foo desc:bar _source:baz",
            vec![],
            TestExpectation::Ok("TermQuery"),
        )?;

        check_build_query(
            "server.type:hpc server.mem:4GB",
            vec![],
            TestExpectation::Err("Field does not exists: '\"server.type\"'"),
        )?;

        check_build_query(
            "title:foo desc:bar",
            vec!["url".to_string()],
            TestExpectation::Err("Field does not exists: '\"url\"'"),
        )?;

        check_build_query(
            "server.name:\".bar:\" server.mem:4GB",
            vec!["server.name".to_string()],
            TestExpectation::Ok("TermQuery"),
        )?;

        check_build_query(
            "server.name:\"for.bar:b\" server.mem:4GB",
            vec![],
            TestExpectation::Ok("TermQuery"),
        )?;

        Ok(())
    }
}
