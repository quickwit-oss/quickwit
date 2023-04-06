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

use std::net::IpAddr;
use std::str::FromStr;

use tantivy::json_utils::{convert_to_fast_value_and_get_term, JsonTermWriter};
use tantivy::query::{PhraseQuery, TermQuery};
use tantivy::schema::{
    Field, FieldEntry, FieldType, IndexRecordOption, IntoIpv6Addr, JsonObjectOptions, Schema,
    TextOptions, Type,
};
use tantivy::time::format_description::well_known::Rfc3339;
use tantivy::time::OffsetDateTime;
use tantivy::tokenizer::TextAnalyzer;
use tantivy::{DateTime, Term};

use crate::quickwit_query_ast::tantivy_query_ast::{TantivyBoolQuery, TantivyQueryAst};

const DYNAMIC_FIELD_NAME: &str = "_dynamic";

fn get_tokenizer(text_options: &TextOptions) -> Option<TextAnalyzer> {
    let text_field_indexing = text_options.get_indexing_options()?;
    let tokenizer_name = text_field_indexing.tokenizer();
    crate::tokenizers::get_quickwit_tokenizer_manager().get(tokenizer_name)
}

fn get_tokenizer_from_json_option(json_options: &JsonObjectOptions) -> Option<TextAnalyzer> {
    let text_field_indexing = json_options.get_text_indexing_options()?;
    let tokenizer_name = text_field_indexing.tokenizer();
    crate::tokenizers::get_quickwit_tokenizer_manager().get(tokenizer_name)
}

fn make_term_query(term: Term) -> TantivyQueryAst {
    TermQuery::new(term, IndexRecordOption::Basic).into()
}

pub(crate) fn find_field_or_hit_dynamic<'a>(
    full_path: &'a str,
    schema: &'a Schema,
) -> Option<(Field, &'a FieldEntry, &'a str)> {
    let (field, path) = if let Some((field, path)) = schema.find_field(&full_path) {
        (field, path)
    } else {
        let dynamic_field = schema.get_field(DYNAMIC_FIELD_NAME).ok()?;
        (dynamic_field, full_path)
    };
    let field_entry = schema.get_field_entry(field);
    let typ = field_entry.field_type().value_type();
    if path.is_empty() != (typ != Type::Json) {
        return None;
    }
    Some((field, field_entry, path))
}

pub(crate) fn compute_query(
    full_path: &str,
    value: &str,
    tokenize: bool,
    schema: &Schema,
) -> TantivyQueryAst {
    if let Some((field, field_entry, path)) = find_field_or_hit_dynamic(full_path, schema) {
        compute_query_with_field(field, field_entry, path, &value, tokenize)
    } else {
        TantivyQueryAst::match_none()
    }
}

fn compute_query_with_field(
    field: Field,
    field_entry: &FieldEntry,
    json_path: &str,
    value: &str,
    tokenize: bool,
) -> TantivyQueryAst {
    let field_type = field_entry.field_type();
    match field_type {
        FieldType::U64(_) => {
            let Ok(val) = u64::from_str(value) else { return TantivyQueryAst::match_none(); };
            let term = Term::from_field_u64(field, val);
            make_term_query(term)
        }
        FieldType::I64(_) => {
            let Ok(val) = i64::from_str(value) else { return TantivyQueryAst::match_none(); };
            let term = Term::from_field_i64(field, val);
            make_term_query(term)
        }
        FieldType::F64(_) => {
            let Ok(val) = f64::from_str(value) else { return TantivyQueryAst::match_none(); };
            let term = Term::from_field_f64(field, val);
            make_term_query(term)
        }
        FieldType::Bool(_) => {
            let Ok(val) = bool::from_str(value) else { return TantivyQueryAst::match_none(); };
            let term = Term::from_field_bool(field, val);
            make_term_query(term)
        }
        FieldType::Date(_) => {
            // TODO handle input format.
            let Ok(dt) = OffsetDateTime::parse(value, &Rfc3339) else { return TantivyQueryAst::match_none(); };
            let term = Term::from_field_date(field, DateTime::from_utc(dt));
            make_term_query(term)
        }
        FieldType::Str(text_options) => {
            let text_analyzer_opt: Option<tantivy::tokenizer::TextAnalyzer> = if tokenize {
                get_tokenizer(text_options)
            } else {
                None
            };
            if let Some(text_analyzer) = text_analyzer_opt {
                let mut terms: Vec<(usize, Term)> = Vec::new();
                let mut token_stream = text_analyzer.token_stream(value);
                token_stream.process(&mut |token| {
                    terms.push((token.position, Term::from_field_text(field, &token.text)));
                });
                if terms.is_empty() {
                    return TantivyQueryAst::match_none();
                } else if terms.len() == 1 {
                    let term = terms.pop().unwrap().1;
                    make_term_query(term)
                } else {
                    PhraseQuery::new_with_offset(terms).into()
                }
            } else {
                let term = Term::from_field_text(field, value);
                make_term_query(term)
            }
        }
        FieldType::IpAddr(_) => {
            let Ok(ip_addr) = IpAddr::from_str(value) else { return TantivyQueryAst::match_none(); };
            let ip_v6 = ip_addr.into_ipv6_addr();
            let term = Term::from_field_ip_addr(field, ip_v6);
            make_term_query(term)
        }
        FieldType::JsonObject(ref json_options) => {
            compute_tantivy_ast_query_for_json(field, json_path, value, tokenize, json_options)
        }
        FieldType::Facet(_) => {
            todo!();
        }
        FieldType::Bytes(_) => {
            todo!()
        }
    }
}

fn compute_tantivy_ast_query_for_json(
    field: Field,
    json_path: &str,
    text: &str,
    tokenize: bool,
    json_options: &JsonObjectOptions,
) -> TantivyQueryAst {
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
            .push(TermQuery::new(term, IndexRecordOption::Basic).into());
    }
    let text_analyzer_opt: Option<tantivy::tokenizer::TextAnalyzer> = if tokenize {
        get_tokenizer_from_json_option(json_options)
    } else {
        None
    };
    if let Some(text_analyzer) = text_analyzer_opt {
        let mut terms: Vec<(usize, Term)> = Vec::new();
        let mut token_stream = text_analyzer.token_stream(text);
        token_stream.process(&mut |token| {
            json_term_writer.set_str(&token.text);
            terms.push((token.position, json_term_writer.term().clone()));
        });
        if terms.is_empty() {
            return TantivyQueryAst::match_none();
        } else if terms.len() == 1 {
            let term = terms.pop().unwrap().1;
            bool_query.should.push(make_term_query(term));
        } else {
            bool_query
                .should
                .push(PhraseQuery::new_with_offset(terms).into());
        }
    } else {
        json_term_writer.set_str(text);
        let term = json_term_writer.term().clone();
        bool_query
            .should
            .push(TermQuery::new(term, IndexRecordOption::Basic).into());
    }
    TantivyQueryAst::Bool(bool_query)
}
