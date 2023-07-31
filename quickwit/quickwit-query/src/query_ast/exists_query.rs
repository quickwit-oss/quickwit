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

use serde::{Deserialize, Serialize};
use tantivy::schema::{Field, IndexRecordOption, Schema as TantivySchema};
use tantivy::tokenizer::TokenizerManager;
use tantivy::Term;

use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::query_ast::{BuildTantivyAst, QueryAst};
use crate::InvalidQuery;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExistsQuery {
    pub(crate) field: String,
}

fn build_field_presence_term(field: &str) -> Term {
    let path = replace_unescaped_dot_by_json_path_sep(field);
    Term::from_field_text(FIELD_PRESENCE_FIELD, &path)
}

impl From<ExistsQuery> for QueryAst {
    fn from(exists_query: ExistsQuery) -> Self {
        QueryAst::Exists(exists_query)
    }
}

// TODO shared consts
const FIELD_PRESENCE_FIELD: Field = Field::from_field_id(0u32);

fn replace_unescaped_dot_by_json_path_sep(field_name: &str) -> String {
    let mut escaped = false;
    let mut result = String::with_capacity(field_name.len());
    for c in field_name.chars() {
        if escaped {
            escaped = false;
            result.push(c);
        } else {
            match c {
                '\\' => {
                    escaped = true;
                    result.push(c);
                }
                '.' => {
                    result.push(1u8 as char);
                }
                _ => {
                    result.push(c);
                }
            }
        }
    }
    result
}

impl BuildTantivyAst for ExistsQuery {
    fn build_tantivy_ast_impl(
        &self,
        _schema: &TantivySchema,
        _tokenizer_manager: &TokenizerManager,
        _search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let field_exists_term: Term = build_field_presence_term(&self.field);
        let field_exists_term_query =
            tantivy::query::TermQuery::new(field_exists_term, IndexRecordOption::Basic);
        Ok(TantivyQueryAst::from(field_exists_term_query))
    }
}

#[cfg(test)]
mod tests {

    use tantivy::schema::Type;

    use super::*;

    #[test]
    fn test_exists_query() {
        let field_presence_term: Term = build_field_presence_term("attributes.color");
        assert_eq!(field_presence_term.field(), super::FIELD_PRESENCE_FIELD);
        assert_eq!(field_presence_term.typ(), Type::Str);
        let field_presence_text = field_presence_term
            .value()
            .as_str()
            .as_ref()
            .unwrap()
            .to_string();
        assert_eq!(field_presence_text, "attributes\u{1}color");
    }
}
