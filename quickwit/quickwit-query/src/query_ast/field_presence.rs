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

use quickwit_common::shared_consts::FIELD_PRESENCE_FIELD_NAME;
use quickwit_common::PathHasher;
use serde::{Deserialize, Serialize};
use tantivy::schema::{Field, IndexRecordOption, Schema as TantivySchema};
use tantivy::tokenizer::TokenizerManager;
use tantivy::Term;

use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::query_ast::{BuildTantivyAst, QueryAst};
use crate::{find_field_or_hit_dynamic, InvalidQuery};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FieldPresenceQuery {
    pub field: String,
}

impl From<FieldPresenceQuery> for QueryAst {
    fn from(field_presence_query: FieldPresenceQuery) -> Self {
        QueryAst::FieldPresence(field_presence_query)
    }
}

fn compute_field_presence_hash(field: Field, field_path: &str) -> u64 {
    let mut path_hasher: PathHasher = PathHasher::default();
    path_hasher.append(&field.field_id().to_le_bytes()[..]);
    let mut escaped = false;
    let mut current_segment = String::new();
    for c in field_path.chars() {
        if escaped {
            escaped = false;
            current_segment.push(c);
            continue;
        }
        match c {
            '\\' => {
                escaped = true;
            }
            '.' => {
                path_hasher.append(current_segment.as_bytes());
                current_segment.clear();
            }
            _ => {
                current_segment.push(c);
            }
        }
    }
    if !current_segment.is_empty() {
        path_hasher.append(current_segment.as_bytes());
    }
    path_hasher.finish()
}

impl BuildTantivyAst for FieldPresenceQuery {
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        _tokenizer_manager: &TokenizerManager,
        _search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let field_presence_field = schema.get_field(FIELD_PRESENCE_FIELD_NAME).map_err(|_| {
            InvalidQuery::SchemaError("field presence is not available for this split".to_string())
        })?;
        let (field, field_entry, path) = find_field_or_hit_dynamic(&self.field, schema)?;
        if field_entry.is_fast() {
            let full_path = if path.is_empty() {
                field_entry.name().to_string()
            } else {
                format!("{}.{}", field_entry.name(), path)
            };
            let exists_query = tantivy::query::ExistsQuery::new_exists_query(full_path);
            Ok(TantivyQueryAst::from(exists_query))
        } else {
            // fallback to the presence field
            let field_presence_hash = compute_field_presence_hash(field, path);
            let field_presence_term: Term =
                Term::from_field_u64(field_presence_field, field_presence_hash);
            let field_presence_term_query =
                tantivy::query::TermQuery::new(field_presence_term, IndexRecordOption::Basic);
            Ok(TantivyQueryAst::from(field_presence_term_query))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_field_presence_single() {
        let field_presence_term: u64 =
            compute_field_presence_hash(Field::from_field_id(17u32), "attributes");
        assert_eq!(
            field_presence_term,
            PathHasher::hash_path(&[&17u32.to_le_bytes()[..], b"attributes"])
        );
    }

    #[test]
    fn test_field_presence_hash_simple() {
        let field_presence_term: u64 =
            compute_field_presence_hash(Field::from_field_id(17u32), "attributes.color");
        assert_eq!(
            field_presence_term,
            PathHasher::hash_path(&[&17u32.to_le_bytes()[..], b"attributes", b"color"])
        );
    }

    #[test]
    fn test_field_presence_hash_escaped_dot() {
        let field_presence_term: u64 =
            compute_field_presence_hash(Field::from_field_id(17u32), r"attributes\.color.hello");
        assert_eq!(
            field_presence_term,
            PathHasher::hash_path(&[&17u32.to_le_bytes()[..], b"attributes.color", b"hello"])
        );
    }
}
