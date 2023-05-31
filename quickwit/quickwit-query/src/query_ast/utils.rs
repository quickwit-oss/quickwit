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

use tantivy::json_utils::{convert_to_fast_value_and_get_term, JsonTermWriter};
use tantivy::query::TermQuery as TantivyTermQuery;
use tantivy::schema::{
    Field, FieldEntry, FieldType, IndexRecordOption, JsonObjectOptions, Schema as TantivySchema,
    Type,
};
use tantivy::Term;

use crate::json_literal::InterpretUserInput;
use crate::query_ast::full_text_query::FullTextParams;
use crate::query_ast::tantivy_query_ast::{TantivyBoolQuery, TantivyQueryAst};
use crate::InvalidQuery;

const DYNAMIC_FIELD_NAME: &str = "_dynamic";

fn make_term_query(term: Term) -> TantivyQueryAst {
    TantivyTermQuery::new(term, IndexRecordOption::WithFreqs).into()
}

pub fn find_field_or_hit_dynamic<'a>(
    full_path: &'a str,
    schema: &'a TantivySchema,
) -> Result<(Field, &'a FieldEntry, &'a str), InvalidQuery> {
    let (field, path) = if let Some((field, path)) = schema.find_field(full_path) {
        (field, path)
    } else {
        let dynamic_field =
            schema
                .get_field(DYNAMIC_FIELD_NAME)
                .map_err(|_| InvalidQuery::FieldDoesNotExist {
                    full_path: full_path.to_string(),
                })?;
        (dynamic_field, full_path)
    };
    let field_entry = schema.get_field_entry(field);
    let typ = field_entry.field_type().value_type();
    if path.is_empty() {
        if typ == Type::Json {
            return Err(InvalidQuery::JsonFieldRootNotSearchable {
                full_path: full_path.to_string(),
            });
        }
    } else if typ != Type::Json {
        return Err(InvalidQuery::FieldDoesNotExist {
            full_path: full_path.to_string(),
        });
    }
    Ok((field, field_entry, path))
}

/// Creates a full text query.
///
/// If tokenize is set to true, the text will be tokenized.
pub(crate) fn full_text_query(
    full_path: &str,
    text_query: &str,
    full_text_params: &FullTextParams,
    schema: &TantivySchema,
) -> Result<TantivyQueryAst, InvalidQuery> {
    let (field, field_entry, path) = find_field_or_hit_dynamic(full_path, schema)?;
    compute_query_with_field(field, field_entry, path, text_query, full_text_params)
}

fn parse_value_from_user_text<'a, T: InterpretUserInput<'a>>(
    text: &'a str,
    field_name: &str,
) -> Result<T, InvalidQuery> {
    if let Some(parsed_value) = T::interpret_str(text) {
        return Ok(parsed_value);
    }
    Err(InvalidQuery::InvalidSearchTerm {
        expected_value_type: T::name(),
        field_name: field_name.to_string(),
        value: text.to_string(),
    })
}

fn compute_query_with_field(
    field: Field,
    field_entry: &FieldEntry,
    json_path: &str,
    value: &str,
    full_text_params: &FullTextParams,
) -> Result<TantivyQueryAst, InvalidQuery> {
    let field_type = field_entry.field_type();
    match field_type {
        FieldType::U64(_) => {
            let val = parse_value_from_user_text::<u64>(value, field_entry.name())?;
            let term = Term::from_field_u64(field, val);
            Ok(make_term_query(term))
        }
        FieldType::I64(_) => {
            let val = parse_value_from_user_text::<i64>(value, field_entry.name())?;
            let term = Term::from_field_i64(field, val);
            Ok(make_term_query(term))
        }
        FieldType::F64(_) => {
            let val = parse_value_from_user_text::<f64>(value, field_entry.name())?;
            let term = Term::from_field_f64(field, val);
            Ok(make_term_query(term))
        }
        FieldType::Bool(_) => {
            let bool_val = parse_value_from_user_text(value, field_entry.name())?;
            let term = Term::from_field_bool(field, bool_val);
            Ok(make_term_query(term))
        }
        FieldType::Date(_) => {
            let dt = parse_value_from_user_text(value, field_entry.name())?;
            let term = Term::from_field_date(field, dt);
            Ok(make_term_query(term))
        }
        FieldType::Str(text_options) => {
            let text_field_indexing = text_options.get_indexing_options().ok_or_else(|| {
                InvalidQuery::SchemaError(format!(
                    "Field {} is not full-text searchable",
                    field_entry.name()
                ))
            })?;
            let terms =
                full_text_params.tokenize_text_into_terms(field, value, text_field_indexing)?;
            full_text_params.make_query(terms, text_field_indexing.index_option())
        }
        FieldType::IpAddr(_) => {
            let ip_v6 = parse_value_from_user_text(value, field_entry.name())?;
            let term = Term::from_field_ip_addr(field, ip_v6);
            Ok(make_term_query(term))
        }
        FieldType::JsonObject(ref json_options) => compute_tantivy_ast_query_for_json(
            field,
            json_path,
            value,
            full_text_params,
            json_options,
        ),
        FieldType::Facet(_) => Err(InvalidQuery::SchemaError(
            "Facets are not supported in Quickwit.".to_string(),
        )),
        FieldType::Bytes(_) => {
            let buffer: Vec<u8> = parse_value_from_user_text(value, field_entry.name())?;
            let term = Term::from_field_bytes(field, &buffer[..]);
            Ok(make_term_query(term))
        }
    }
}

fn compute_tantivy_ast_query_for_json(
    field: Field,
    json_path: &str,
    text: &str,
    full_text_params: &FullTextParams,
    json_options: &JsonObjectOptions,
) -> Result<TantivyQueryAst, InvalidQuery> {
    let mut bool_query = TantivyBoolQuery::default();
    let mut term = Term::with_capacity(100);
    let mut json_term_writer = JsonTermWriter::from_field_and_json_path(
        field,
        json_path,
        json_options.is_expand_dots_enabled(),
        &mut term,
    );
    if let Some(term) = convert_to_fast_value_and_get_term(&mut json_term_writer, text) {
        bool_query
            .should
            .push(TantivyTermQuery::new(term, IndexRecordOption::Basic).into());
    }
    let position_terms: Vec<(usize, Term)> =
        full_text_params.tokenize_text_into_terms_json(field, json_path, text, json_options)?;
    let index_record_option = json_options
        .get_text_indexing_options()
        .map(|text_indexing_options| text_indexing_options.index_option())
        .unwrap_or(IndexRecordOption::Basic);
    bool_query
        .should
        .push(full_text_params.make_query(position_terms, index_record_option)?);
    Ok(bool_query.into())
}
