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

use quickwit_common::PathHasher;
use quickwit_common::shared_consts::FIELD_PRESENCE_FIELD_NAME;
use serde::{Deserialize, Serialize};
use tantivy::Term;
use tantivy::schema::{Field, FieldEntry, IndexRecordOption, Schema as TantivySchema};

use super::tantivy_query_ast::TantivyBoolQuery;
use super::utils::{DYNAMIC_FIELD_NAME, find_subfields};
use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::query_ast::{BuildTantivyAst, BuildTantivyAstContext, QueryAst};
use crate::{BooleanOperand, InvalidQuery, find_field_or_hit_dynamic};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FieldPresenceQuery {
    pub field: String,
}

impl From<FieldPresenceQuery> for QueryAst {
    fn from(field_presence_query: FieldPresenceQuery) -> Self {
        QueryAst::FieldPresence(field_presence_query)
    }
}

fn compute_field_presence_hash(field: Field, field_path: &str) -> PathHasher {
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
    path_hasher
}

fn build_existence_query(
    field_presence_field: Field,
    field: Field,
    field_entry: &FieldEntry,
    path: &str,
) -> TantivyQueryAst {
    if field_entry.is_fast() {
        let full_path = if path.is_empty() {
            field_entry.name().to_string()
        } else {
            format!("{}.{}", field_entry.name(), path)
        };
        let exists_query = tantivy::query::ExistsQuery::new(full_path, true);
        TantivyQueryAst::from(exists_query)
    } else {
        // fallback to the presence field
        let presence_hasher = compute_field_presence_hash(field, path);
        let leaf_term = Term::from_field_u64(field_presence_field, presence_hasher.finish_leaf());
        if field_entry.field_type().is_json() {
            let intermediate_term =
                Term::from_field_u64(field_presence_field, presence_hasher.finish_intermediate());
            let query = tantivy::query::TermSetQuery::new([leaf_term, intermediate_term]);
            TantivyQueryAst::from(query)
        } else {
            let query = tantivy::query::TermQuery::new(leaf_term, IndexRecordOption::Basic);
            TantivyQueryAst::from(query)
        }
    }
}

impl FieldPresenceQuery {
    /// Identify the field and potential subfields that are required for this query.
    ///
    /// This is only based on the schema and cannot now about dynamic fields.
    pub fn find_field_and_subfields<'a>(
        &'a self,
        schema: &'a TantivySchema,
    ) -> Vec<(Field, &'a FieldEntry, &'a str)> {
        let mut fields = Vec::new();
        if let Some((field, entry, path)) = find_field_or_hit_dynamic(&self.field, schema) {
            fields.push((field, entry, path));
        };
        // if `self.field` was not found, it might still be an `object` field
        if fields.is_empty() || fields[0].1.name() == DYNAMIC_FIELD_NAME {
            for (field, entry) in find_subfields(&self.field, schema) {
                fields.push((field, entry, ""));
            }
        }
        fields
    }
}

impl BuildTantivyAst for FieldPresenceQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let field_presence_field = context
            .schema
            .get_field(FIELD_PRESENCE_FIELD_NAME)
            .map_err(|_| {
                InvalidQuery::SchemaError(
                    "field presence is not available for this split".to_string(),
                )
            })?;
        let fields = self.find_field_and_subfields(context.schema);
        if fields.is_empty() {
            // the schema is not dynamic and no subfields are defined
            return Err(InvalidQuery::FieldDoesNotExist {
                full_path: self.field.clone(),
            });
        }
        let queries = fields
            .into_iter()
            .map(|(field, entry, path)| {
                build_existence_query(field_presence_field, field, entry, path)
            })
            .collect();
        Ok(TantivyQueryAst::Bool(TantivyBoolQuery::build_clause(
            BooleanOperand::Or,
            queries,
        )))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_field_presence_single() {
        let field_presence_term: u64 =
            compute_field_presence_hash(Field::from_field_id(17u32), "attributes").finish_leaf();
        assert_eq!(
            field_presence_term,
            PathHasher::hash_path(&[&17u32.to_le_bytes()[..], b"attributes"])
        );
    }

    #[test]
    fn test_field_presence_hash_simple() {
        let field_presence_term: u64 =
            compute_field_presence_hash(Field::from_field_id(17u32), "attributes.color")
                .finish_leaf();
        assert_eq!(
            field_presence_term,
            PathHasher::hash_path(&[&17u32.to_le_bytes()[..], b"attributes", b"color"])
        );
    }

    #[test]
    fn test_field_presence_hash_escaped_dot() {
        let field_presence_term: u64 =
            compute_field_presence_hash(Field::from_field_id(17u32), r"attributes\.color.hello")
                .finish_leaf();
        assert_eq!(
            field_presence_term,
            PathHasher::hash_path(&[&17u32.to_le_bytes()[..], b"attributes.color", b"hello"])
        );
    }
}
