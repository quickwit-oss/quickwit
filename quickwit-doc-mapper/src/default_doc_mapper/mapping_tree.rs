// Copyright (C) 2021 Quickwit, Inc.
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

use std::any::type_name;
use std::collections::BTreeMap;

use anyhow::bail;
use serde_json::Value as JsonValue;
use tantivy::schema::{
    BytesOptions, Cardinality, Field, JsonObjectOptions, NumericOptions, SchemaBuilder,
    TextOptions, Value,
};
use tantivy::time::format_description::well_known::Rfc3339;
use tantivy::time::OffsetDateTime;
use tantivy::{DateTime, Document};

use crate::default_doc_mapper::field_mapping_entry::{
    QuickwitNumericOptions, QuickwitObjectOptions, QuickwitTextOptions,
};
use crate::default_doc_mapper::{FieldMappingType, QuickwitJsonOptions};
use crate::{DocParsingError, FieldMappingEntry, ModeType};

#[derive(Copy, Clone, PartialEq, Eq)]
enum JsonType {
    Null,
    Bool,
    Array,
    Number,
    String,
    Object,
}

#[derive(Clone)]
pub enum LeafType {
    Text(QuickwitTextOptions),
    I64(QuickwitNumericOptions),
    U64(QuickwitNumericOptions),
    F64(QuickwitNumericOptions),
    Date(QuickwitNumericOptions),
    Bytes(QuickwitNumericOptions),
    Json(QuickwitJsonOptions),
}

impl LeafType {
    fn json_type(&self) -> JsonType {
        match self {
            LeafType::Text(_) => JsonType::String,
            LeafType::I64(_) | LeafType::U64(_) | LeafType::F64(_) => JsonType::Number,
            LeafType::Date(_) => JsonType::String,
            LeafType::Bytes(_) => JsonType::String,
            LeafType::Json(_) => JsonType::Object,
        }
    }

    pub fn is_fast_field(&self) -> bool {
        match self {
            LeafType::Text(_opt) => false, // TODO fixme once we have text fast field
            LeafType::I64(opt)
            | LeafType::U64(opt)
            | LeafType::F64(opt)
            | LeafType::Date(opt)
            | LeafType::Bytes(opt) => opt.fast,
            LeafType::Json(_) => false,
        }
    }

    fn value_from_json(&self, json_val: serde_json::Value) -> Result<Value, String> {
        match self {
            LeafType::Text(_) => {
                if let JsonValue::String(text) = json_val {
                    Ok(Value::Str(text))
                } else {
                    Err(format!("Expected JSON string, got '{}'.", json_val))
                }
            }
            LeafType::I64(_) => i64::from_json(json_val),
            LeafType::U64(_) => u64::from_json(json_val),
            LeafType::F64(_) => f64::from_json(json_val),
            LeafType::Date(_) => {
                let date_rfc3339_str = if let JsonValue::String(text) = json_val {
                    text
                } else {
                    return Err(format!(
                        "Expected rfc3339 datetime string, got '{}'.",
                        json_val
                    ));
                };
                let offset_date_time =
                    OffsetDateTime::parse(&date_rfc3339_str, &Rfc3339).map_err(|_err| {
                        format!("Expected RFC 3339 date, got '{}'.", date_rfc3339_str)
                    })?;
                let date_time_utc = DateTime::from_utc(offset_date_time);
                Ok(Value::Date(date_time_utc))
            }
            LeafType::Bytes(_) => {
                let base64_str = if let JsonValue::String(base64_str) = json_val {
                    base64_str
                } else {
                    return Err(format!("Expected base64 string, got '{}'.", json_val));
                };
                let payload = base64::decode(&base64_str).map_err(|base64_decode_err| {
                    format!("Expected Base64 string, got '{base64_str}': {base64_decode_err}")
                })?;
                Ok(Value::Bytes(payload))
            }
            LeafType::Json(_) => {
                if let JsonValue::Object(json_obj) = json_val {
                    Ok(Value::JsonObject(json_obj))
                } else {
                    Err(format!("Expected JSON object  got '{}'.", json_val))
                }
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct MappingLeaf {
    field: Field,
    typ: LeafType,
    cardinality: Cardinality,
}

impl MappingLeaf {
    pub fn doc_from_json(
        &self,
        json_val: serde_json::Value,
        document: &mut Document,
        path: &mut Vec<String>,
    ) -> Result<(), DocParsingError> {
        if json_val.is_null() {
            // We just ignore `null`.
            return Ok(());
        }
        if let JsonValue::Array(els) = json_val {
            if self.cardinality == Cardinality::SingleValue {
                return Err(DocParsingError::MultiValuesNotSupported(path.join(".")));
            }
            for el_json_val in els {
                if el_json_val.is_null() {
                    // We just ignore `null`.
                    continue;
                }
                let value = self
                    .typ
                    .value_from_json(el_json_val)
                    .map_err(|err_msg| DocParsingError::ValueError(path.join("."), err_msg))?;
                document.add_field_value(self.field, value);
            }
            return Ok(());
        }
        let value = self
            .typ
            .value_from_json(json_val)
            .map_err(|err_msg| DocParsingError::ValueError(path.join("."), err_msg))?;
        document.add_field_value(self.field, value);
        Ok(())
    }

    fn populate_json<'a>(
        &'a self,
        named_doc: &mut BTreeMap<String, Vec<JsonValue>>,
        field_path: &[&'a str],
        doc_json: &mut serde_json::Map<String, serde_json::Value>,
    ) {
        let json_type = self.typ.json_type();
        if let Some(json_val) = extract_json_val(json_type, named_doc, field_path, self.cardinality)
        {
            insert_json_val(field_path, json_val, doc_json);
        }
    }

    pub fn field(&self) -> Field {
        self.field
    }

    pub fn get_type(&self) -> &LeafType {
        &self.typ
    }
}

fn json_type_from_json_value(json_value: &JsonValue) -> JsonType {
    match json_value {
        JsonValue::Null => JsonType::Null,
        JsonValue::Bool(_) => JsonType::Bool,
        JsonValue::Array(_) => JsonType::Array,
        JsonValue::Number(_) => JsonType::Number,
        JsonValue::String(_) => JsonType::String,
        JsonValue::Object(_) => JsonType::Object,
    }
}

fn extract_json_val(
    json_type: JsonType,
    named_doc: &mut BTreeMap<String, Vec<JsonValue>>,
    field_path: &[&str],
    cardinality: Cardinality,
) -> Option<JsonValue> {
    let full_path = field_path.join(".");
    let vals = named_doc.remove(&full_path)?;
    let mut vals_with_correct_type_it = vals
        .into_iter()
        .filter(|json_val| json_type_from_json_value(json_val) == json_type);
    match cardinality {
        Cardinality::SingleValue => vals_with_correct_type_it.next(),
        Cardinality::MultiValues => Some(JsonValue::Array(vals_with_correct_type_it.collect())),
    }
}

fn insert_json_val(
    field_path: &[&str], //< may not be empty
    json_val: JsonValue,
    mut doc_json: &mut serde_json::Map<String, serde_json::Value>,
) {
    let (last_field_name, up_to_last) = field_path.split_last().expect("Empty path is forbidden");
    for &field_name in up_to_last {
        let entry = doc_json
            .entry(field_name.to_string())
            .or_insert_with(|| JsonValue::Object(Default::default()));
        if let JsonValue::Object(child_json_obj) = entry {
            doc_json = child_json_obj;
        } else {
            return;
        }
    }
    doc_json.insert(last_field_name.to_string(), json_val);
}

trait NumVal: Sized + Into<Value> {
    fn from_json_number(num: &serde_json::Number) -> Option<Self>;

    fn from_json(json_val: JsonValue) -> Result<Value, String> {
        if let JsonValue::Number(num_val) = json_val {
            Ok(Self::from_json_number(&num_val)
                .ok_or_else(|| {
                    format!(
                        "Expected {}, got inconvertible JSON number '{}'.",
                        type_name::<Self>(),
                        num_val
                    )
                })?
                .into())
        } else {
            Err(format!("Expected JSON number, got '{:?}'.", json_val))
        }
    }
}

impl NumVal for u64 {
    fn from_json_number(num: &serde_json::Number) -> Option<Self> {
        num.as_u64()
    }
}

impl NumVal for i64 {
    fn from_json_number(num: &serde_json::Number) -> Option<Self> {
        num.as_i64()
    }
}
impl NumVal for f64 {
    fn from_json_number(num: &serde_json::Number) -> Option<Self> {
        num.as_f64()
    }
}

#[derive(Clone, Default)]
pub(crate) struct MappingNode {
    pub branches: fnv::FnvHashMap<String, MappingTree>,
    branches_order: Vec<String>,
}

fn get_or_insert_path<'a>(
    path: &[String],
    mut dynamic_json_obj: &'a mut serde_json::Map<String, JsonValue>,
) -> &'a mut serde_json::Map<String, JsonValue> {
    for field_name in path {
        let child_json_val = dynamic_json_obj
            .entry(field_name.clone())
            .or_insert_with(|| JsonValue::Object(Default::default()));
        dynamic_json_obj = if let JsonValue::Object(child_map) = child_json_val {
            child_map
        } else {
            panic!("Expected Json object.");
        };
    }
    dynamic_json_obj
}

impl MappingNode {
    #[cfg(test)]
    pub fn num_fields(&self) -> usize {
        self.branches.len()
    }

    /// Returns an iterator over the tree child.
    ///
    /// The returned child are not ordered.
    pub fn children(&self) -> impl Iterator<Item = &MappingTree> {
        self.branches.values()
    }

    pub fn insert(&mut self, path: &str, node: MappingTree) -> anyhow::Result<()> {
        if self.branches.contains_key(path) {
            bail!("Redundant field definition `{}`", path);
        }
        self.branches_order.push(path.to_string());
        self.branches.insert(path.to_string(), node);
        Ok(())
    }

    pub fn ordered_field_mapping_entries(&self) -> Vec<FieldMappingEntry> {
        assert_eq!(self.branches.len(), self.branches_order.len());
        let mut field_mapping_entries = Vec::new();
        for field_name in &self.branches_order {
            let child_tree = self.branches.get(field_name).expect("Missing field");
            let field_mapping_entry = FieldMappingEntry {
                name: field_name.clone(),
                mapping_type: child_tree.clone().into(),
            };
            field_mapping_entries.push(field_mapping_entry);
        }
        field_mapping_entries
    }

    pub fn doc_from_json(
        &self,
        json_obj: serde_json::Map<String, JsonValue>,
        mode: ModeType,
        document: &mut Document,
        path: &mut Vec<String>,
        dynamic_json_obj: &mut serde_json::Map<String, JsonValue>,
    ) -> Result<(), DocParsingError> {
        for (field_name, val) in json_obj {
            if let Some(child_tree) = self.branches.get(&field_name) {
                path.push(field_name);
                child_tree.doc_from_json(val, mode, document, path, dynamic_json_obj)?;
                path.pop();
            } else {
                match mode {
                    ModeType::Lenient => {
                        // In lenient mode we simply ignore these unmapped fields.
                    }
                    ModeType::Dynamic => {
                        let dynamic_json_obj_after_path =
                            get_or_insert_path(path, dynamic_json_obj);
                        dynamic_json_obj_after_path.insert(field_name, val);
                    }
                    ModeType::Strict => {
                        path.push(field_name);
                        let field_path = path.join(".");
                        return Err(DocParsingError::NoSuchFieldInSchema(field_path));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn populate_json<'a>(
        &'a self,
        named_doc: &mut BTreeMap<String, Vec<JsonValue>>,
        field_path: &mut Vec<&'a str>,
        doc_json: &mut serde_json::Map<String, serde_json::Value>,
    ) {
        for (field_name, field_mapping) in &self.branches {
            field_path.push(field_name);
            field_mapping.populate_json(named_doc, field_path, doc_json);
            field_path.pop();
        }
    }
}

impl From<MappingTree> for FieldMappingType {
    fn from(mapping_tree: MappingTree) -> Self {
        match mapping_tree {
            MappingTree::Leaf(leaf) => leaf.into(),
            MappingTree::Node(node) => FieldMappingType::Object(QuickwitObjectOptions {
                field_mappings: node.into(),
            }),
        }
    }
}

impl From<MappingLeaf> for FieldMappingType {
    fn from(leaf: MappingLeaf) -> Self {
        match leaf.typ {
            LeafType::Text(opt) => FieldMappingType::Text(opt, leaf.cardinality),
            LeafType::I64(opt) => FieldMappingType::I64(opt, leaf.cardinality),
            LeafType::U64(opt) => FieldMappingType::U64(opt, leaf.cardinality),
            LeafType::F64(opt) => FieldMappingType::F64(opt, leaf.cardinality),
            LeafType::Date(opt) => FieldMappingType::Date(opt, leaf.cardinality),
            LeafType::Bytes(opt) => FieldMappingType::Bytes(opt, leaf.cardinality),
            LeafType::Json(opt) => FieldMappingType::Json(opt, leaf.cardinality),
        }
    }
}

impl From<MappingNode> for Vec<FieldMappingEntry> {
    fn from(node: MappingNode) -> Self {
        node.ordered_field_mapping_entries()
    }
}

#[derive(Clone)]
pub(crate) enum MappingTree {
    Leaf(MappingLeaf),
    Node(MappingNode),
}

impl MappingTree {
    fn doc_from_json(
        &self,
        json_value: serde_json::Value,
        mode: ModeType,
        document: &mut Document,
        path: &mut Vec<String>,
        dynamic_json_obj: &mut serde_json::Map<String, JsonValue>,
    ) -> Result<(), DocParsingError> {
        match self {
            MappingTree::Leaf(mapping_leaf) => {
                mapping_leaf.doc_from_json(json_value, document, path)
            }
            MappingTree::Node(mapping_node) => {
                if let JsonValue::Object(json_obj) = json_value {
                    mapping_node.doc_from_json(json_obj, mode, document, path, dynamic_json_obj)
                } else {
                    Err(DocParsingError::ValueError(
                        path.join("."),
                        format!("Expected an JSON Object, got {}", json_value),
                    ))
                }
            }
        }
    }

    fn populate_json<'a>(
        &'a self,
        named_doc: &mut BTreeMap<String, Vec<JsonValue>>,
        field_path: &mut Vec<&'a str>,
        doc_json: &mut serde_json::Map<String, serde_json::Value>,
    ) {
        match self {
            MappingTree::Leaf(mapping_leaf) => {
                mapping_leaf.populate_json(named_doc, field_path, doc_json)
            }
            MappingTree::Node(mapping_node) => {
                mapping_node.populate_json(named_doc, field_path, doc_json);
            }
        }
    }
}

pub(crate) fn build_mapping_tree(
    entries: &[FieldMappingEntry],
    schema: &mut SchemaBuilder,
) -> anyhow::Result<MappingNode> {
    let mut field_path = Vec::new();
    build_mapping_tree_from_entries(entries, &mut field_path, schema)
}

fn build_mapping_tree_from_entries<'a>(
    entries: &'a [FieldMappingEntry],
    field_path: &mut Vec<&'a str>,
    schema: &mut SchemaBuilder,
) -> anyhow::Result<MappingNode> {
    let mut mapping_node = MappingNode::default();
    for entry in entries {
        field_path.push(&entry.name);
        let child_tree = build_mapping_from_field_type(&entry.mapping_type, field_path, schema)?;
        field_path.pop();
        mapping_node.insert(&entry.name, child_tree)?;
    }
    Ok(mapping_node)
}

fn get_numeric_options(
    quickwit_numeric_options: &QuickwitNumericOptions,
    cardinality: Cardinality,
) -> NumericOptions {
    let mut numeric_options = NumericOptions::default();
    if quickwit_numeric_options.stored {
        numeric_options = numeric_options.set_stored();
    }
    if quickwit_numeric_options.indexed {
        numeric_options = numeric_options.set_indexed();
    }
    if quickwit_numeric_options.fast {
        numeric_options = numeric_options.set_fast(cardinality);
    }
    numeric_options
}

fn get_bytes_options(quickwit_numeric_options: &QuickwitNumericOptions) -> BytesOptions {
    let mut bytes_options = BytesOptions::default();
    if quickwit_numeric_options.indexed {
        bytes_options = bytes_options.set_indexed();
    }
    if quickwit_numeric_options.fast {
        bytes_options = bytes_options.set_fast();
    }
    if quickwit_numeric_options.stored {
        bytes_options = bytes_options.set_stored();
    }
    bytes_options
}

fn build_mapping_from_field_type<'a>(
    field_mapping_type: &'a FieldMappingType,
    field_path: &mut Vec<&'a str>,
    schema_builder: &mut SchemaBuilder,
) -> anyhow::Result<MappingTree> {
    let field_name = field_path.join(".");
    match field_mapping_type {
        FieldMappingType::Text(options, cardinality) => {
            let text_options: TextOptions = options.clone().into();
            let field = schema_builder.add_text_field(&field_name, text_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::Text(options.clone()),
                cardinality: *cardinality,
            };
            Ok(MappingTree::Leaf(mapping_leaf))
        }
        FieldMappingType::I64(options, cardinality) => {
            let numeric_options = get_numeric_options(options, *cardinality);
            let field = schema_builder.add_i64_field(&field_name, numeric_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::I64(options.clone()),
                cardinality: *cardinality,
            };
            Ok(MappingTree::Leaf(mapping_leaf))
        }
        FieldMappingType::U64(options, cardinality) => {
            let numeric_options = get_numeric_options(options, *cardinality);
            let field = schema_builder.add_u64_field(&field_name, numeric_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::U64(options.clone()),
                cardinality: *cardinality,
            };
            Ok(MappingTree::Leaf(mapping_leaf))
        }
        FieldMappingType::F64(options, cardinality) => {
            let numeric_options = get_numeric_options(options, *cardinality);
            let field = schema_builder.add_f64_field(&field_name, numeric_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::F64(options.clone()),
                cardinality: *cardinality,
            };
            Ok(MappingTree::Leaf(mapping_leaf))
        }
        FieldMappingType::Date(options, cardinality) => {
            let numeric_options = get_numeric_options(options, *cardinality);
            let field = schema_builder.add_date_field(&field_name, numeric_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::Date(options.clone()),
                cardinality: *cardinality,
            };
            Ok(MappingTree::Leaf(mapping_leaf))
        }
        FieldMappingType::Bytes(options, cardinality) => {
            let bytes_options = get_bytes_options(options);
            let field = schema_builder.add_bytes_field(&field_name, bytes_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::Bytes(options.clone()),
                cardinality: *cardinality,
            };
            Ok(MappingTree::Leaf(mapping_leaf))
        }
        FieldMappingType::Json(options, cardinality) => {
            let json_options = JsonObjectOptions::from(options.clone());
            let field = schema_builder.add_json_field(&field_name, json_options);
            Ok(MappingTree::Leaf(MappingLeaf {
                field,
                typ: LeafType::Json(options.clone()),
                cardinality: *cardinality,
            }))
        }
        FieldMappingType::Object(entries) => {
            let mapping_node = build_mapping_tree_from_entries(
                &entries.field_mappings,
                field_path,
                schema_builder,
            )?;
            Ok(MappingTree::Node(mapping_node))
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tantivy::schema::{Cardinality, Field, Value};
    use tantivy::time::{Date, Month, PrimitiveDateTime, Time};
    use tantivy::{DateTime, Document};

    use super::{LeafType, MappingLeaf};
    use crate::default_doc_mapper::field_mapping_entry::{
        QuickwitNumericOptions, QuickwitTextOptions,
    };

    #[test]
    fn test_get_or_insert_path() {
        let mut map = Default::default();
        super::get_or_insert_path(&["a".to_string(), "b".to_string()], &mut map)
            .insert("c".to_string(), serde_json::Value::from(3u64));
        assert_eq!(
            &serde_json::to_value(&map).unwrap(),
            &serde_json::json!({
                "a": {
                    "b": {
                        "c": 3u64
                    }
                }
            })
        );
        super::get_or_insert_path(&["a".to_string(), "b".to_string()], &mut map)
            .insert("d".to_string(), serde_json::Value::from(2u64));
        assert_eq!(
            &serde_json::to_value(&map).unwrap(),
            &serde_json::json!({
                "a": {
                    "b": {
                        "c": 3u64,
                        "d": 2u64
                    }
                }
            })
        );
        super::get_or_insert_path(&["e".to_string()], &mut map)
            .insert("f".to_string(), serde_json::Value::from(5u64));
        assert_eq!(
            &serde_json::to_value(&map).unwrap(),
            &serde_json::json!({
                "a": {
                    "b": {
                        "c": 3u64,
                        "d": 2u64
                    }
                },
                "e": { "f": 5u64 }
            })
        );
        super::get_or_insert_path(&[], &mut map)
            .insert("g".to_string(), serde_json::Value::from(6u64));
        assert_eq!(
            &serde_json::to_value(&map).unwrap(),
            &serde_json::json!({
                "a": {
                    "b": {
                        "c": 3u64,
                        "d": 2u64
                    }
                },
                "e": { "f": 5u64 },
                "g": 6u64
            })
        );
    }

    #[test]
    fn test_parse_u64_mapping() {
        let leaf = LeafType::U64(QuickwitNumericOptions::default());
        assert_eq!(
            leaf.value_from_json(json!(20i64)).unwrap(),
            tantivy::schema::Value::U64(20u64)
        );
    }

    #[test]
    fn test_parse_u64_negative_should_error() {
        let leaf = LeafType::U64(QuickwitNumericOptions::default());
        assert_eq!(
            leaf.value_from_json(json!(-20i64)).err().unwrap(),
            "Expected u64, got inconvertible JSON number '-20'."
        );
    }

    #[test]
    fn test_parse_i64_mapping() {
        let leaf = LeafType::I64(QuickwitNumericOptions::default());
        assert_eq!(
            leaf.value_from_json(json!(20u64)).unwrap(),
            tantivy::schema::Value::I64(20i64)
        );
    }

    #[test]
    fn test_parse_i64_from_f64_should_error() {
        let leaf = LeafType::I64(QuickwitNumericOptions::default());
        assert_eq!(
            leaf.value_from_json(json!(20.2f64)).err().unwrap(),
            "Expected i64, got inconvertible JSON number '20.2'."
        );
    }

    #[test]
    fn test_parse_i64_too_large() {
        let leaf = LeafType::I64(QuickwitNumericOptions::default());
        let err = leaf.value_from_json(json!(u64::max_value())).err().unwrap();
        assert_eq!(
            err,
            "Expected i64, got inconvertible JSON number '18446744073709551615'."
        );
    }

    #[test]
    fn test_parse_f64_from_u64() {
        let leaf = LeafType::F64(QuickwitNumericOptions::default());
        assert_eq!(
            leaf.value_from_json(json!(4_000u64)).unwrap(),
            Value::F64(4_000f64)
        );
    }

    #[test]
    fn test_parse_i64_mutivalued() {
        let typ = LeafType::I64(QuickwitNumericOptions::default());
        let field = Field::from_field_id(10);
        let leaf_entry = MappingLeaf {
            field,
            typ,
            cardinality: Cardinality::MultiValues,
        };
        let mut document = Document::default();
        let mut path = Vec::new();
        leaf_entry
            .doc_from_json(serde_json::json!([10u64, 20u64]), &mut document, &mut path)
            .unwrap();
        assert_eq!(document.len(), 2);
        let values: Vec<i64> = document.get_all(field).flat_map(Value::as_i64).collect();
        assert_eq!(&values, &[10i64, 20i64]);
    }

    #[test]
    fn test_parse_null_is_just_ignored() {
        let typ = LeafType::I64(QuickwitNumericOptions::default());
        let field = Field::from_field_id(10);
        let leaf_entry = MappingLeaf {
            field,
            typ,
            cardinality: Cardinality::MultiValues,
        };
        let mut document = Document::default();
        let mut path = Vec::new();
        leaf_entry
            .doc_from_json(serde_json::json!(null), &mut document, &mut path)
            .unwrap();
        assert_eq!(document.len(), 0);
    }

    #[test]
    fn test_parse_i64_mutivalued_accepts_scalar() {
        let typ = LeafType::I64(QuickwitNumericOptions::default());
        let field = Field::from_field_id(10);
        let leaf_entry = MappingLeaf {
            field,
            typ,
            cardinality: Cardinality::MultiValues,
        };
        let mut document = Document::default();
        let mut path = Vec::new();
        leaf_entry
            .doc_from_json(serde_json::json!(10u64), &mut document, &mut path)
            .unwrap();
        assert_eq!(document.len(), 1);
        assert_eq!(document.get_first(field).unwrap().as_i64().unwrap(), 10i64);
    }

    #[test]
    fn test_parse_u64_mutivalued_nested_array_forbidden() {
        let typ = LeafType::I64(QuickwitNumericOptions::default());
        let field = Field::from_field_id(10);
        let leaf_entry = MappingLeaf {
            field,
            typ,
            cardinality: Cardinality::MultiValues,
        };
        let mut document = Document::default();
        let mut path = vec!["root".to_string(), "my_field".to_string()];
        let parse_err = leaf_entry
            .doc_from_json(
                serde_json::json!([10u64, [1u64, 2u64]]),
                &mut document,
                &mut path,
            )
            .err()
            .unwrap();
        assert_eq!(
            parse_err.to_string(),
            "The field 'root.my_field' could not be parsed: Expected JSON number, got \
             'Array([Number(1), Number(2)])'."
        );
    }

    #[test]
    fn test_parse_text() {
        let typ = LeafType::Text(QuickwitTextOptions::default());
        let parsed_value = typ.value_from_json(json!("bacon and eggs")).unwrap();
        assert_eq!(parsed_value, Value::Str("bacon and eggs".to_string()));
    }

    #[test]
    fn test_parse_text_number_should_error() {
        let typ = LeafType::Text(QuickwitTextOptions::default());
        let err = typ.value_from_json(json!(2u64)).err().unwrap();
        assert_eq!(err, "Expected JSON string, got '2'.");
    }

    #[test]
    fn test_parse_date() {
        let typ = LeafType::Date(QuickwitNumericOptions::default());
        let value = typ
            .value_from_json(json!("2021-12-19T16:39:57-01:00"))
            .unwrap();
        let datetime = PrimitiveDateTime::new(
            Date::from_calendar_date(2021, Month::December, 19).unwrap(),
            Time::from_hms(17, 39, 57).unwrap(),
        );
        let datetime_utc = DateTime::from_primitive(datetime);
        assert_eq!(value, Value::Date(datetime_utc));
    }

    #[test]
    fn test_parse_date_number_should_error() {
        let typ = LeafType::Date(QuickwitNumericOptions::default());
        let err = typ.value_from_json(json!(123)).err().unwrap();
        assert_eq!(err, "Expected rfc3339 datetime string, got '123'.");
    }

    #[test]
    fn test_parse_date_array_should_error() {
        let typ = LeafType::Date(QuickwitNumericOptions::default());
        let err = typ.value_from_json(json!([123, 23])).err().unwrap();
        assert_eq!(err, "Expected rfc3339 datetime string, got '[123,23]'.");
    }

    #[test]
    fn test_parse_bytes() {
        let typ = LeafType::Bytes(QuickwitNumericOptions::default());
        let value = typ
            .value_from_json(json!("dGhpcyBpcyBhIGJhc2U2NCBlbmNvZGVkIHN0cmluZw=="))
            .unwrap();
        assert_eq!(
            value.as_bytes().unwrap(),
            b"this is a base64 encoded string"
        );
    }

    #[test]
    fn test_parse_bytes_number_should_err() {
        let typ = LeafType::Bytes(QuickwitNumericOptions::default());
        let error = typ.value_from_json(json!(2u64)).err().unwrap();
        assert_eq!(error, "Expected base64 string, got '2'.");
    }

    #[test]
    fn test_parse_bytes_invalid_base64() {
        let typ = LeafType::Bytes(QuickwitNumericOptions::default());
        let error = typ.value_from_json(json!("dEwerwer#!%")).err().unwrap();
        assert_eq!(
            error,
            "Expected Base64 string, got 'dEwerwer#!%': Invalid byte 35, offset 8."
        );
    }

    #[test]
    fn test_parse_array_of_bytes() {
        let typ = LeafType::Bytes(QuickwitNumericOptions::default());
        let field = Field::from_field_id(10);
        let leaf_entry = MappingLeaf {
            field,
            typ,
            cardinality: Cardinality::MultiValues,
        };
        let mut document = Document::default();
        let mut path = vec!["root".to_string(), "my_field".to_string()];
        leaf_entry
            .doc_from_json(
                serde_json::json!([
                    "dGhpcyBpcyBhIGJhc2U2NCBlbmNvZGVkIHN0cmluZw==",
                    "dGhpcyBpcyBhIGJhc2U2NCBlbmNvZGVkIHN0cmluZw=="
                ]),
                &mut document,
                &mut path,
            )
            .unwrap();
        assert_eq!(document.len(), 2);
        let bytes_vec: Vec<&[u8]> = document.get_all(field).flat_map(Value::as_bytes).collect();
        assert_eq!(
            &bytes_vec[..],
            &[
                b"this is a base64 encoded string",
                b"this is a base64 encoded string"
            ]
        )
    }
}
