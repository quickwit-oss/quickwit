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

use std::collections::HashMap;

use anyhow::bail;
use quickwit_proto::SearchRequest;
use tantivy::query::{Query, QueryParser, QueryParserError as TantivyQueryParserError};
use tantivy::schema::{Field, Schema};
use tantivy::tokenizer::TokenizerManager;
use tantivy_query_grammar::{UserInputAst, UserInputLeaf};

use crate::mapper::{is_valid_field_mapping_name, TANTIVY_DOT_SYMBOL};
use crate::QueryParserError;
use once_cell::sync::Lazy;
use regex::Regex;

/// Build a `Query` with field resolution & forbidding range clauses.
pub(crate) fn build_query(
    schema: Schema,
    request: &SearchRequest,
    default_field_names: &[String],
) -> Result<Box<dyn Query>, QueryParserError> {
    let (query_string, field_names_mapping) = normalize_query_string(&request.query)?;
    let get_field_name_from_mapping = move |field_name: String| {
        let field_name = field_names_mapping.get(&field_name).unwrap_or(&field_name);
        (*field_name).clone()
    };

    let user_input_ast = tantivy_query_grammar::parse_query(&query_string)
        .map_err(|_| TantivyQueryParserError::SyntaxError)?;

    if has_range_clause(user_input_ast) {
        return Err(anyhow::anyhow!("Range queries are not currently allowed.").into());
    }

    let default_fields = resolve_fields(&schema, default_field_names)?;
    let query_parser = QueryParser::new(schema, default_fields, TokenizerManager::default());
    let query = query_parser
        .parse_query(&query_string)
        .map_err(|error| match error {
            TantivyQueryParserError::FieldDoesNotExist(field_name) => {
                TantivyQueryParserError::FieldDoesNotExist(get_field_name_from_mapping(field_name))
            }
            TantivyQueryParserError::FieldNotIndexed(field_name) => {
                TantivyQueryParserError::FieldNotIndexed(get_field_name_from_mapping(field_name))
            }
            TantivyQueryParserError::FieldDoesNotHavePositionsIndexed(field_name) => {
                TantivyQueryParserError::FieldDoesNotHavePositionsIndexed(
                    get_field_name_from_mapping(field_name),
                )
            }
            _ => error,
        })?;

    Ok(query)
}

/// Finds field names form the user query and transforms those
/// that contain [`TANTIVY_DOT_SYMBOL`] into tantivy compatible field names.
///
/// It transforms the user query by replacing the `.` symbol with [`TANTIVY_DOT_SYMBOL`]
/// while returning a map of the repaced field names in order to display more accurate error messages.
///
pub(crate) fn normalize_query_string(
    query_string: &str,
) -> anyhow::Result<(String, HashMap<String, String>)> {
    static POTENTIAL_FIELD_NAME_PATTERN: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?P<field_name>[a-zA-Z0-9_\.\-]+):").unwrap());
    let mut result_query_string = String::from(query_string);
    let mut field_names_mapping = HashMap::new();
    let captures = POTENTIAL_FIELD_NAME_PATTERN.captures_iter(query_string);
    for capture in captures {
        let field_name = capture
            .name("field_name")
            .map_or("".to_string(), |match_val| match_val.as_str().to_string());
        if !is_valid_field_mapping_name(&field_name) {
            bail!("Invalid field name: `{}`.", field_name);
        }

        let compatible_field_name = field_name.replace(".", TANTIVY_DOT_SYMBOL);
        result_query_string = result_query_string.replace(&field_name, &compatible_field_name);
        field_names_mapping.insert(compatible_field_name, field_name);
    }

    Ok((result_query_string, field_names_mapping))
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
            .get_field(&field_name)
            .ok_or_else(|| TantivyQueryParserError::FieldDoesNotExist(field_name.clone()))?;
        fields.push(field);
    }
    Ok(fields)
}

#[cfg(test)]
mod test {
    use crate::mapper::TANTIVY_DOT_SYMBOL;

    use super::{build_query, normalize_query_string};

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
        schema_builder.add_text_field("server__dot__name", TEXT);
        schema_builder.add_text_field("server__dot__mem", TEXT);
        schema_builder.add_text_field("_source", TEXT);
        schema_builder.build()
    }

    fn check_build_query(query_str: &str, expected: TestExpectation) -> anyhow::Result<()> {
        let request = SearchRequest {
            index_id: "test_index".to_string(),
            query: query_str.to_string(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 20,
            start_offset: 0,
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
            TestExpectation::Err("Field does not exists: '\"foo\"'"),
        )?;
        check_build_query(
            "title:[a TO b]",
            TestExpectation::Err("Range queries are not currently allowed."),
        )?;
        check_build_query(
            "title:{a TO b} desc:foo",
            TestExpectation::Err("Range queries are not currently allowed."),
        )?;
        check_build_query(
            "title:>foo",
            TestExpectation::Err("Range queries are not currently allowed."),
        )?;
        check_build_query(
            "title:foo desc:bar _source:baz",
            TestExpectation::Ok("TermQuery"),
        )?;

        check_build_query(
            "server.type:hpc server.mem:4GB",
            TestExpectation::Err("Field does not exists: '\"server.type\"'"),
        )?;

        check_build_query(
            "server.type:hpc server.mem:4GB",
            TestExpectation::Err("Field does not exists: '\"server.type\"'"),
        )?;

        check_build_query(
            ".server.name:foo _source:4GB",
            TestExpectation::Err("Invalid field name: `.server.name`."),
        )?;

        check_build_query(
            "server__dot__name:foo _source:4GB",
            TestExpectation::Err("Invalid field name: `server__dot__name`."),
        )?;

        check_build_query(
            "server.name:foo server.mem:4GB",
            TestExpectation::Ok("TermQuery"),
        )?;

        Ok(())
    }

    #[test]
    fn test_normalize_query_string() {
        let result = normalize_query_string(".aws.server:foo mem:>3gb");
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid field name: `.aws.server`."
        );

        let result = normalize_query_string("test.server:foo .mem:>3gb");
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid field name: `.mem`."
        );

        let result = normalize_query_string(".aws__dot__server:foo mem:>3gb");
        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid field name: `.aws{}server`.", TANTIVY_DOT_SYMBOL)
        );

        let (query_string, field_names_mapping) =
            normalize_query_string("server.name:foo server.mem:3GB title:+foo _source:>foo")
                .unwrap();
        assert_eq!(
            query_string,
            format!(
                "server{0}name:foo server{0}mem:3GB title:+foo _source:>foo",
                TANTIVY_DOT_SYMBOL
            )
        );
        assert_eq!(field_names_mapping.len(), 4);
        assert_eq!(
            field_names_mapping
                .get(format!("server{}name", TANTIVY_DOT_SYMBOL).as_str())
                .unwrap(),
            "server.name"
        );
        assert_eq!(
            field_names_mapping
                .get(format!("server{}mem", TANTIVY_DOT_SYMBOL).as_str())
                .unwrap(),
            "server.mem"
        );
        assert_eq!(field_names_mapping.get("title").unwrap(), "title");
        assert_eq!(field_names_mapping.get("_source").unwrap(), "_source");
    }
}
