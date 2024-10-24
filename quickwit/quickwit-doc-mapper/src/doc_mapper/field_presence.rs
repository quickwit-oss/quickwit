// Copyright (C) 2024 Quickwit, Inc.
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

use fnv::FnvHashSet;
use quickwit_common::PathHasher;
use tantivy::schema::document::{ReferenceValue, ReferenceValueLeaf};
use tantivy::schema::{FieldType, Schema, Value};
use tantivy::Document;

/// Populates the field presence for a document.
///
/// The field presence is a set of hashes that represent the fields that are present in the
/// document. Each hash is computed from the field path.
///
/// It is only added if the field is indexed and not fast.
pub(crate) fn populate_field_presence<D: Document>(
    document: &D,
    schema: &Schema,
) -> FnvHashSet<u64> {
    let mut field_presence_hashes: FnvHashSet<u64> =
        FnvHashSet::with_capacity_and_hasher(schema.num_fields(), Default::default());
    for (field, value) in document.iter_fields_and_values() {
        let field_entry = schema.get_field_entry(field);
        if !field_entry.is_indexed() || field_entry.is_fast() {
            // We are using an tantivy's ExistsQuery for fast fields.
            continue;
        }
        let mut path_hasher: PathHasher = PathHasher::default();
        path_hasher.append(&field.field_id().to_le_bytes()[..]);
        if let Some(json_obj) = value.as_object() {
            let is_expand_dots_enabled: bool =
                if let FieldType::JsonObject(json_options) = field_entry.field_type() {
                    json_options.is_expand_dots_enabled()
                } else {
                    false
                };
            populate_field_presence_for_json_obj(
                json_obj,
                path_hasher,
                is_expand_dots_enabled,
                &mut field_presence_hashes,
            );
        } else {
            field_presence_hashes.insert(path_hasher.finish());
        }
    }
    field_presence_hashes
}

#[inline]
fn populate_field_presence_for_json_value<'a>(
    json_value: impl Value<'a>,
    path_hasher: &PathHasher,
    is_expand_dots_enabled: bool,
    output: &mut FnvHashSet<u64>,
) {
    match json_value.as_value() {
        ReferenceValue::Leaf(ReferenceValueLeaf::Null) => {}
        ReferenceValue::Leaf(_) => {
            output.insert(path_hasher.finish());
        }
        ReferenceValue::Array(items) => {
            for item in items {
                populate_field_presence_for_json_value(
                    item,
                    path_hasher,
                    is_expand_dots_enabled,
                    output,
                );
            }
        }
        ReferenceValue::Object(json_obj) => {
            populate_field_presence_for_json_obj(
                json_obj,
                path_hasher.clone(),
                is_expand_dots_enabled,
                output,
            );
        }
    }
}

fn populate_field_presence_for_json_obj<'a, Iter: Iterator<Item = (&'a str, impl Value<'a>)>>(
    json_obj: Iter,
    path_hasher: PathHasher,
    is_expand_dots_enabled: bool,
    output: &mut FnvHashSet<u64>,
) {
    for (field_key, field_value) in json_obj {
        let mut child_path_hasher = path_hasher.clone();
        if is_expand_dots_enabled {
            for segment in field_key.split('.') {
                child_path_hasher.append(segment.as_bytes());
            }
        } else {
            child_path_hasher.append(field_key.as_bytes());
        };
        populate_field_presence_for_json_value(
            field_value,
            &child_path_hasher,
            is_expand_dots_enabled,
            output,
        );
    }
}
