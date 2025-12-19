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

use tantivy::Term;
use tantivy::json_utils::convert_to_fast_value_and_append_to_json_term;
use tantivy::query::TermQuery as TantivyTermQuery;
use tantivy::schema::{
    Field, FieldEntry, FieldType, IndexRecordOption, JsonObjectOptions, Schema as TantivySchema,
    TextFieldIndexing, Type,
};

use crate::InvalidQuery;
use crate::MatchAllOrNone::MatchNone as TantivyEmptyQuery;
use crate::json_literal::InterpretUserInput;
use crate::query_ast::full_text_query::FullTextParams;
use crate::query_ast::tantivy_query_ast::{TantivyBoolQuery, TantivyQueryAst};
use crate::tokenizers::{RAW_TOKENIZER_NAME, TokenizerManager};

pub(crate) const DYNAMIC_FIELD_NAME: &str = "_dynamic";

fn make_term_query(term: Term) -> TantivyQueryAst {
    TantivyTermQuery::new(term, IndexRecordOption::WithFreqs).into()
}

/// Find the field or fallback to the dynamic field if it exists
pub fn find_field_or_hit_dynamic<'a>(
    full_path: &'a str,
    schema: &'a TantivySchema,
) -> Option<(Field, &'a FieldEntry, &'a str)> {
    let (field, path) = if let Some((field, path)) = schema.find_field(full_path) {
        (field, path)
    } else {
        let dynamic_field = schema.get_field(DYNAMIC_FIELD_NAME).ok()?;
        (dynamic_field, full_path)
    };
    let field_entry = schema.get_field_entry(field);
    let typ = field_entry.field_type().value_type();
    if !path.is_empty() && typ != Type::Json {
        return None;
    }
    Some((field, field_entry, path))
}

/// Find all the fields that are below the given path.
///
/// This will return a list of fields only when the path is that of a composite
/// type in the doc mapping.
pub fn find_subfields<'a>(
    path: &'a str,
    schema: &'a TantivySchema,
) -> Vec<(Field, &'a FieldEntry)> {
    let prefix = format!("{path}.");
    schema
        .fields()
        .filter(|(_, field_entry)| field_entry.name().starts_with(&prefix))
        .collect()
}

/// Creates a full text query.
///
/// If tokenize is set to true, the text will be tokenized.
pub(crate) fn full_text_query(
    full_path: &str,
    text_query: &str,
    full_text_params: &FullTextParams,
    schema: &TantivySchema,
    tokenizer_manager: &TokenizerManager,
    lenient: bool,
) -> Result<TantivyQueryAst, InvalidQuery> {
    let Some((field, field_entry, path)) = find_field_or_hit_dynamic(full_path, schema) else {
        if lenient {
            return Ok(TantivyEmptyQuery.into());
        } else {
            return Err(InvalidQuery::FieldDoesNotExist {
                full_path: full_path.to_string(),
            });
        }
    };
    compute_query_with_field(
        field,
        field_entry,
        path,
        text_query,
        full_text_params,
        tokenizer_manager,
    )
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
    tokenizer_manager: &TokenizerManager,
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
        FieldType::Date(date_options) => {
            let dt = parse_value_from_user_text(value, field_entry.name())?;
            let term = if date_options.is_indexed() {
                Term::from_field_date_for_search(field, dt)
            } else {
                Term::from_field_date(field, dt.truncate(date_options.get_precision()))
            };
            Ok(make_term_query(term))
        }
        FieldType::Str(text_options) => {
            let columnar_opt = TextFieldIndexing::default()
                .set_fieldnorms(false)
                .set_tokenizer(RAW_TOKENIZER_NAME);
            let text_field_indexing = text_options
                .get_indexing_options()
                .or_else(|| text_options.is_fast().then_some(&columnar_opt))
                .ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "field {} is not full-text searchable",
                        field_entry.name()
                    ))
                })?;
            let terms = full_text_params.tokenize_text_into_terms(
                field,
                value,
                text_field_indexing,
                tokenizer_manager,
            )?;
            full_text_params.make_query(terms, text_field_indexing.index_option())
        }
        FieldType::IpAddr(_) => {
            let ip_v6 = parse_value_from_user_text(value, field_entry.name())?;
            let term = Term::from_field_ip_addr(field, ip_v6);
            Ok(make_term_query(term))
        }
        FieldType::JsonObject(json_options) => compute_tantivy_ast_query_for_json(
            field,
            json_path,
            value,
            full_text_params,
            json_options,
            tokenizer_manager,
        ),
        FieldType::Facet(_) => Err(InvalidQuery::SchemaError(
            "facets are not supported in Quickwit".to_string(),
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
    tokenizer_manager: &TokenizerManager,
) -> Result<TantivyQueryAst, InvalidQuery> {
    let mut bool_query = TantivyBoolQuery::default();
    let term = Term::from_field_json_path(field, json_path, json_options.is_expand_dots_enabled());
    if let Some(term) = convert_to_fast_value_and_append_to_json_term(&term, text, true) {
        bool_query
            .should
            .push(TantivyTermQuery::new(term, IndexRecordOption::Basic).into());
    }
    let position_terms: Vec<(usize, Term)> = full_text_params.tokenize_text_into_terms_json(
        field,
        json_path,
        text,
        json_options,
        tokenizer_manager,
    )?;
    let index_record_option = json_options
        .get_text_indexing_options()
        .map(|text_indexing_options| text_indexing_options.index_option())
        .unwrap_or(IndexRecordOption::Basic);
    bool_query
        .should
        .push(full_text_params.make_query(position_terms, index_record_option)?);
    Ok(bool_query.into())
}
