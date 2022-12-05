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

use std::collections::{HashMap, HashSet};

use anyhow::{bail, Context};
use quickwit_proto::SearchRequest;
use tantivy::query::{Query, QueryParser, QueryParserError as TantivyQueryParserError};
use tantivy::schema::{Field, FieldEntry, FieldType, Schema};
use tantivy_query_grammar::{UserInputAst, UserInputLeaf, UserInputLiteral};

use crate::{QueryParserError, WarmupInfo, DYNAMIC_FIELD_NAME, QUICKWIT_TOKENIZER_MANAGER};

/// Build a `Query` with field resolution & forbidding range clauses.
pub(crate) fn build_query(
    schema: Schema,
    request: &SearchRequest,
    default_field_names: &[String],
) -> Result<(Box<dyn Query>, WarmupInfo), QueryParserError> {
    let user_input_ast = tantivy_query_grammar::parse_query(&request.query)
        .map_err(|_| TantivyQueryParserError::SyntaxError(request.query.to_string()))?;

    let fast_field_names: HashSet<String> = extract_field_with_ranges(&schema, &user_input_ast)?;

    if needs_default_search_field(&user_input_ast)
        && request.search_fields.is_empty()
        && (default_field_names.is_empty() || default_field_names == [DYNAMIC_FIELD_NAME])
    {
        return Err(
            anyhow::anyhow!("No default field declared and no field specified in query.").into(),
        );
    }

    validate_requested_snippet_fields(&schema, request, &user_input_ast, default_field_names)?;

    let search_fields = if request.search_fields.is_empty() {
        resolve_fields(&schema, default_field_names)?
    } else {
        resolve_fields(&schema, &request.search_fields)?
    };

    if let Some(sort_by_field) = &request.sort_by_field {
        validate_sort_by_field(sort_by_field, &schema, Some(&search_fields))?;
    }

    let mut query_parser =
        QueryParser::new(schema, search_fields, QUICKWIT_TOKENIZER_MANAGER.clone());
    query_parser.set_conjunction_by_default();
    let query = query_parser.parse_query(&request.query)?;

    let mut term_set_query_fields = HashSet::new();
    extract_term_set_query_fields(&user_input_ast, &mut term_set_query_fields);

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
        fast_field_names,
        ..WarmupInfo::default()
    };

    Ok((query, warmup_info))
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

// Extract leaves from query ast.
fn collect_leaves(user_input_ast: &UserInputAst) -> Vec<&UserInputLeaf> {
    match user_input_ast {
        UserInputAst::Clause(sub_queries) => {
            let mut leaves = vec![];
            for (_, sub_ast) in sub_queries {
                leaves.extend(collect_leaves(sub_ast));
            }
            leaves
        }
        UserInputAst::Boost(ast, _) => collect_leaves(ast),
        UserInputAst::Leaf(leaf) => vec![leaf],
    }
}

fn extract_field_name(leaf: &UserInputLeaf) -> Option<&str> {
    match leaf {
        UserInputLeaf::Literal(UserInputLiteral { field_name, .. }) => field_name.as_deref(),
        UserInputLeaf::Range { field, .. } => field.as_deref(),
        UserInputLeaf::Set { field, .. } => field.as_deref(),
        UserInputLeaf::All => None,
    }
}

fn is_valid_field_for_range(field_entry: &FieldEntry) -> anyhow::Result<()> {
    match field_entry.field_type() {
        FieldType::IpAddr(ip_addr_options) => {
            if !ip_addr_options.is_fast() {
                bail!(
                    "Range queries require having a fast field (Field `{}` is not declared as a \
                     fast field.)",
                    field_entry.name()
                );
            }
        }
        other_type => {
            anyhow::bail!(
                "`{}` is of type `{:?}`. Range queries are only supported for IP Field at the \
                 moment.",
                field_entry.name(),
                other_type.value_type()
            );
        }
    }
    Ok(())
}

/// Extracts field names with ranges.
fn extract_field_with_ranges(
    schema: &Schema,
    user_input_ast: &UserInputAst,
) -> anyhow::Result<HashSet<String>> {
    let mut field_with_ranges: HashSet<String> = Default::default();
    for leaf in collect_leaves(user_input_ast) {
        if let UserInputLeaf::Range { field, .. } = leaf {
            // TODO check the field supports ranges.
            if let Some(field_name) = field {
                let field: Field = schema
                    .get_field(field_name)
                    .with_context(|| format!("Unknown field `{field_name}`"))?;
                let field_entry = schema.get_field_entry(field);
                is_valid_field_for_range(field_entry)?;
                field_with_ranges.insert(field_name.clone());
            } else {
                anyhow::bail!("Range query without targeting a specific field is forbidden.");
            }
        }
    }
    Ok(field_with_ranges)
}

fn extract_term_set_query_fields(user_input_ast: &UserInputAst, set: &mut HashSet<String>) {
    set.extend(
        collect_leaves(user_input_ast)
            .into_iter()
            .filter_map(|leaf| {
                if let UserInputLeaf::Set { field, .. } = leaf {
                    field.clone()
                } else {
                    None
                }
            }),
    )
}

/// Tells if the query has a Term or Range node which does not
/// specify a search field.
fn needs_default_search_field(user_input_ast: &UserInputAst) -> bool {
    // `All` query apply to all fields, therefore doesn't need default fields.
    if matches!(user_input_ast, UserInputAst::Leaf(leaf) if **leaf == UserInputLeaf::All) {
        return false;
    }

    collect_leaves(user_input_ast)
        .into_iter()
        .any(|leaf| extract_field_name(leaf).is_none())
}

/// Collects all the fields names on the query ast nodes.
fn field_names(user_input_ast: &UserInputAst) -> HashSet<&str> {
    collect_leaves(user_input_ast)
        .into_iter()
        .filter_map(extract_field_name)
        .collect()
}

fn validate_requested_snippet_fields(
    schema: &Schema,
    request: &SearchRequest,
    user_input_ast: &UserInputAst,
    default_field_names: &[String],
) -> anyhow::Result<()> {
    let query_fields = field_names(user_input_ast);
    for field_name in &request.snippet_fields {
        if !default_field_names.contains(field_name)
            && !request.search_fields.contains(field_name)
            && !query_fields.contains(field_name.as_str())
        {
            return Err(anyhow::anyhow!(
                "The snippet field `{}` should be a default search field or appear in the query.",
                field_name
            ));
        }

        let field_entry = schema
            .get_field(field_name)
            .map(|field| schema.get_field_entry(field))
            .ok_or_else(|| anyhow::anyhow!("The snippet field `{}` does not exist.", field_name))?;
        match field_entry.field_type() {
            FieldType::Str(text_options) => {
                if !text_options.is_stored() {
                    return Err(anyhow::anyhow!(
                        "The snippet field `{}` must be stored.",
                        field_name
                    ));
                }
                continue;
            }
            other => {
                return Err(anyhow::anyhow!(
                    "The snippet field `{}` must be of type `Str`, got `{}`.",
                    field_name,
                    other.value_type().name()
                ))
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_sort_by_field(
    field_name: &str,
    schema: &Schema,
    search_fields_opt: Option<&Vec<Field>>,
) -> anyhow::Result<()> {
    if field_name == "_score" {
        return validate_sort_by_score(schema, search_fields_opt);
    }
    let sort_by_field = schema
        .get_field(field_name)
        .with_context(|| format!("Unknown sort by field: `{}`", field_name))?;
    let sort_by_field_entry = schema.get_field_entry(sort_by_field);

    if matches!(sort_by_field_entry.field_type(), FieldType::Str(_)) {
        bail!(
            "Sort by field on type text is currently not supported `{}`.",
            field_name
        )
    }
    if !sort_by_field_entry.is_fast() {
        bail!(
            "Sort by field must be a fast field, please add the fast property to your field `{}`.",
            field_name
        )
    }

    Ok(())
}

fn validate_sort_by_score(
    schema: &Schema,
    search_fields_opt: Option<&Vec<Field>>,
) -> anyhow::Result<()> {
    if let Some(fields) = search_fields_opt {
        for field in fields {
            if !schema.get_field_entry(*field).has_fieldnorms() {
                bail!(
                    "Fieldnorms for field `{}` is missing. Fieldnorms must be stored for the \
                     field to compute the BM25 score of the documents.",
                    schema.get_field_name(*field)
                )
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use quickwit_proto::SearchRequest;
    use tantivy::query::QueryParserError;
    use tantivy::schema::{
        Cardinality, DateOptions, IpAddrOptions, Schema, FAST, INDEXED, STORED, TEXT,
    };

    use super::{build_query, validate_requested_snippet_fields};
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
        schema_builder.add_u64_field("int_fast", FAST | STORED);
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
                let (query, _) = query_result.unwrap();
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
        check_build_query("*", vec![], None, TestExpectation::Ok("All")).unwrap();
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
            TestExpectation::Err(r#"`title` is of type `Str`. Range queries are only supported for IP Field at the moment."#),
        )
        .unwrap();
        check_build_query(
            "title:{a TO b} desc:foo",
            vec![],
            None,
            TestExpectation::Err(
                "`title` is of type `Str`. Range queries are only supported for IP Field at the \
                 moment.",
            ),
        )
        .unwrap();
        check_build_query(
            "title:>foo",
            vec![],
            None,
            TestExpectation::Err(
                "`title` is of type `Str`. Range queries are only supported for IP Field",
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
    fn test_range_query() {
        check_build_query(
            "ip:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            None,
            TestExpectation::Ok(
                "RangeQuery { field: Field(7), value_type: IpAddr, left_bound: Included([0, 0, 0, \
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
                "RangeQuery { field: Field(7), value_type: IpAddr, left_bound: Excluded([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1]), right_bound: Unbounded }",
            ),
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
                "RangeQuery { field: Field(8), value_type: IpAddr, left_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1]), right_bound: Included([0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 1, 1, 1]) }",
            ),
        )
        .unwrap();
    }

    #[test]
    fn test_range_query_ip_field_no_fast_field() {
        check_build_query(
            "ip_notff:[127.0.0.1 TO 127.1.1.1]",
            Vec::new(),
            None,
            TestExpectation::Err("Field `ip_notff` is not declared as a fast field"),
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
        };
        let user_input_ast = tantivy_query_grammar::parse_query(&request.query)
            .map_err(|_| QueryParserError::SyntaxError(request.query.clone()))
            .unwrap();
        let default_field_names =
            default_search_fields.unwrap_or_else(|| vec!["title".to_string(), "desc".to_string()]);

        validate_requested_snippet_fields(&schema, &request, &user_input_ast, &default_field_names)
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
            "The snippet field `summary` does not exist."
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
