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

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::str::FromStr;

use anyhow::bail;
use itertools::Itertools;
use serde_json::Value as JsonValue;
use serde_json_borrow::{Map as BorrowedJsonMap, Value as BorrowedJsonValue};
use tantivy::schema::{
    BytesOptions, DateOptions, Field, IntoIpv6Addr, IpAddrOptions, JsonObjectOptions,
    NumericOptions, OwnedValue as TantivyValue, SchemaBuilder, TextOptions,
};
use tantivy::TantivyDocument as Document;

use super::date_time_type::QuickwitDateTimeOptions;
use super::deser_num::{deserialize_f64, deserialize_i64, deserialize_u64};
use super::field_mapping_entry::QuickwitBoolOptions;
use super::tantivy_val_to_json::formatted_tantivy_value_to_json;
use crate::doc_mapper::field_mapping_entry::{
    QuickwitBytesOptions, QuickwitIpAddrOptions, QuickwitNumericOptions, QuickwitObjectOptions,
    QuickwitTextOptions,
};
use crate::doc_mapper::{FieldMappingType, QuickwitJsonOptions};
use crate::{Cardinality, DocParsingError, FieldMappingEntry, ModeType};

#[derive(Clone, Debug)]
pub enum LeafType {
    Bool(QuickwitBoolOptions),
    Bytes(QuickwitBytesOptions),
    DateTime(QuickwitDateTimeOptions),
    F64(QuickwitNumericOptions),
    I64(QuickwitNumericOptions),
    U64(QuickwitNumericOptions),
    IpAddr(QuickwitIpAddrOptions),
    Json(QuickwitJsonOptions),
    Text(QuickwitTextOptions),
}

enum MapOrArrayIter {
    Array(std::vec::IntoIter<JsonValue>),
    Map(serde_json::map::IntoIter),
    Value(JsonValue),
}

impl Iterator for MapOrArrayIter {
    type Item = JsonValue;

    fn next(&mut self) -> Option<JsonValue> {
        match self {
            MapOrArrayIter::Array(iter) => iter.next(),
            MapOrArrayIter::Map(iter) => iter.next().map(|(_, val)| val),
            MapOrArrayIter::Value(val) => {
                if val.is_null() {
                    None
                } else {
                    Some(std::mem::take(val))
                }
            }
        }
    }
}

/// Iterate over all primitive values inside the provided JsonValue, ignoring Nulls, and opening
/// arrays and objects.
pub(crate) struct JsonValueIterator {
    currently_itered: Vec<MapOrArrayIter>,
}

impl JsonValueIterator {
    pub fn new(source: JsonValue) -> JsonValueIterator {
        let base_value = match source {
            JsonValue::Array(array) => MapOrArrayIter::Array(array.into_iter()),
            JsonValue::Object(map) => MapOrArrayIter::Map(map.into_iter()),
            other => MapOrArrayIter::Value(other),
        };
        JsonValueIterator {
            currently_itered: vec![base_value],
        }
    }
}

impl Iterator for JsonValueIterator {
    type Item = JsonValue;

    fn next(&mut self) -> Option<JsonValue> {
        loop {
            let currently_itered = self.currently_itered.last_mut()?;
            match currently_itered.next() {
                Some(JsonValue::Array(array)) => self
                    .currently_itered
                    .push(MapOrArrayIter::Array(array.into_iter())),
                Some(JsonValue::Object(map)) => self
                    .currently_itered
                    .push(MapOrArrayIter::Map(map.into_iter())),
                Some(JsonValue::Null) => continue,
                Some(other) => return Some(other),
                None => {
                    self.currently_itered.pop();
                    continue;
                }
            }
        }
    }
}

enum OneOrIter<T, I: Iterator<Item = T>> {
    One(Option<T>),
    Iter(I),
}

impl<T, I: Iterator<Item = T>> OneOrIter<T, I> {
    pub fn one(item: T) -> Self {
        OneOrIter::One(Some(item))
    }
}

impl<T, I: Iterator<Item = T>> Iterator for OneOrIter<T, I> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        match self {
            OneOrIter::Iter(iter) => iter.next(),
            OneOrIter::One(item) => std::mem::take(item),
        }
    }
}

pub(crate) fn map_primitive_json_to_tantivy(value: JsonValue) -> Option<TantivyValue> {
    match value {
        JsonValue::Array(_) | JsonValue::Object(_) | JsonValue::Null => None,
        JsonValue::String(text) => Some(TantivyValue::Str(text)),
        JsonValue::Bool(val) => Some((val).into()),
        JsonValue::Number(number) => number
            .as_i64()
            .map(Into::into)
            .or(number.as_u64().map(Into::into))
            .or(number.as_f64().map(Into::into)),
    }
}

impl LeafType {
    fn validate_from_json(&self, json_val: &BorrowedJsonValue) -> Result<(), String> {
        match self {
            LeafType::Text(_) => {
                if json_val.is_string() {
                    Ok(())
                } else {
                    Err(format!("expected string, got `{json_val}`"))
                }
            }
            LeafType::I64(numeric_options) => {
                deserialize_i64(json_val, numeric_options.coerce).map(|_| ())
            }
            LeafType::U64(numeric_options) => {
                deserialize_u64(json_val, numeric_options.coerce).map(|_| ())
            }
            LeafType::F64(numeric_options) => {
                deserialize_f64(json_val, numeric_options.coerce).map(|_| ())
            }
            LeafType::Bool(_) => {
                if json_val.is_bool() {
                    Ok(())
                } else {
                    Err(format!("expected boolean, got `{json_val}`"))
                }
            }
            LeafType::IpAddr(_) => {
                let Some(ip_address) = json_val.as_str() else {
                    return Err(format!("expected string, got `{json_val}`"));
                };
                IpAddr::from_str(ip_address)
                    .map_err(|err| format!("failed to parse IP address `{ip_address}`: {err}"))?;
                Ok(())
            }
            LeafType::DateTime(date_time_options) => {
                date_time_options.validate_json(json_val).map(|_| ())
            }
            LeafType::Bytes(binary_options) => {
                if let Some(byte_str) = json_val.as_str() {
                    binary_options.input_format.parse_str(byte_str)?;
                    Ok(())
                } else {
                    Err(format!(
                        "expected {} string, got `{json_val}`",
                        binary_options.input_format.as_str()
                    ))
                }
            }
            LeafType::Json(_) => {
                if json_val.is_object() {
                    Ok(())
                } else {
                    Err(format!("expected object, got `{json_val}`"))
                }
            }
        }
    }

    fn value_from_json(&self, json_val: JsonValue) -> Result<TantivyValue, String> {
        match self {
            LeafType::Text(_) => {
                if let JsonValue::String(text) = json_val {
                    Ok(TantivyValue::Str(text))
                } else {
                    Err(format!("expected string, got `{json_val}`"))
                }
            }
            LeafType::I64(numeric_options) => {
                deserialize_i64(json_val, numeric_options.coerce).map(i64::into)
            }
            LeafType::U64(numeric_options) => {
                deserialize_u64(json_val, numeric_options.coerce).map(u64::into)
            }
            LeafType::F64(numeric_options) => {
                deserialize_f64(json_val, numeric_options.coerce).map(f64::into)
            }
            LeafType::Bool(_) => {
                if let JsonValue::Bool(val) = json_val {
                    Ok(TantivyValue::Bool(val))
                } else {
                    Err(format!("expected boolean, got `{json_val}`"))
                }
            }
            LeafType::IpAddr(_) => {
                if let JsonValue::String(ip_address) = json_val {
                    let ipv6_value = IpAddr::from_str(ip_address.as_str())
                        .map_err(|err| format!("failed to parse IP address `{ip_address}`: {err}"))?
                        .into_ipv6_addr();
                    Ok(TantivyValue::IpAddr(ipv6_value))
                } else {
                    Err(format!("expected string, got `{json_val}`"))
                }
            }
            LeafType::DateTime(date_time_options) => date_time_options.parse_json(&json_val),
            LeafType::Bytes(binary_options) => binary_options.input_format.parse_json(&json_val),
            LeafType::Json(_) => {
                if let JsonValue::Object(json_obj) = json_val {
                    Ok(TantivyValue::Object(
                        json_obj
                            .into_iter()
                            .map(|(key, val)| (key, val.into()))
                            .collect(),
                    ))
                } else {
                    Err(format!("expected object, got `{json_val}`"))
                }
            }
        }
    }

    fn tantivy_value_from_json(
        &self,
        json_val: JsonValue,
    ) -> Result<impl Iterator<Item = TantivyValue>, String> {
        match self {
            LeafType::Text(_) => {
                if let JsonValue::String(text) = json_val {
                    Ok(OneOrIter::one(TantivyValue::Str(text)))
                } else {
                    Err(format!("expected string, got `{json_val}`"))
                }
            }
            LeafType::I64(numeric_options) => {
                let val = deserialize_i64(&json_val, numeric_options.coerce)?;
                Ok(OneOrIter::one((val).into()))
            }
            LeafType::U64(numeric_options) => {
                let val = deserialize_u64(&json_val, numeric_options.coerce)?;
                Ok(OneOrIter::one((val).into()))
            }
            LeafType::F64(numeric_options) => {
                let val = deserialize_f64(&json_val, numeric_options.coerce)?;
                Ok(OneOrIter::one((val).into()))
            }
            LeafType::Bool(_) => {
                if let JsonValue::Bool(val) = json_val {
                    Ok(OneOrIter::one((val).into()))
                } else {
                    Err(format!("expected boolean, got `{json_val}`"))
                }
            }
            LeafType::IpAddr(_) => Err("unsupported concat type: IpAddr".to_string()),
            LeafType::DateTime(_date_time_options) => {
                Err("unsupported concat type: DateTime".to_string())
            }
            LeafType::Bytes(_binary_options) => Err("unsupported concat type: Bytes".to_string()),
            LeafType::Json(_) => {
                if let JsonValue::Object(json_obj) = json_val {
                    Ok(OneOrIter::Iter(
                        json_obj
                            .into_iter()
                            .flat_map(|(_key, val)| JsonValueIterator::new(val))
                            .flat_map(map_primitive_json_to_tantivy),
                    ))
                } else {
                    Err(format!("expected object, got `{json_val}`"))
                }
            }
        }
    }

    fn supported_for_concat(&self) -> bool {
        use LeafType::*;
        matches!(self, Text(_) | U64(_) | I64(_) | F64(_) | Bool(_) | Json(_))
        /*
            // Since concat is a JSON field, anything that JSON supports can be supported
            DateTime(_), // Could be supported if the date is converted to Rfc3339
            IpAddr(_),
            // won't be supported
            Bytes(_),
        */
    }
}

#[derive(Clone)]
pub(crate) struct MappingLeaf {
    field: Field,
    typ: LeafType,
    cardinality: Cardinality,
    // concatenate fields this field is part of
    concatenate: Vec<Field>,
}

impl MappingLeaf {
    fn validate_from_json(
        &self,
        json_value: &BorrowedJsonValue,
        path: &[&str],
    ) -> Result<(), DocParsingError> {
        if json_value.is_null() {
            // We just ignore `null`.
            return Ok(());
        }
        if let BorrowedJsonValue::Array(els) = json_value {
            if self.cardinality == Cardinality::SingleValued {
                return Err(DocParsingError::MultiValuesNotSupported(path.join(".")));
            }
            for el_json_val in els {
                if el_json_val.is_null() {
                    // We just ignore `null`.
                    continue;
                }
                self.typ
                    .validate_from_json(el_json_val)
                    .map_err(|err_msg| DocParsingError::ValueError(path.join("."), err_msg))?;
            }
            return Ok(());
        }

        self.typ
            .validate_from_json(json_value)
            .map_err(|err_msg| DocParsingError::ValueError(path.join("."), err_msg))?;

        Ok(())
    }

    pub fn doc_from_json(
        &self,
        json_val: JsonValue,
        document: &mut Document,
        path: &mut [String],
    ) -> Result<(), DocParsingError> {
        if json_val.is_null() {
            // We just ignore `null`.
            return Ok(());
        }
        if let JsonValue::Array(els) = json_val {
            if self.cardinality == Cardinality::SingleValued {
                return Err(DocParsingError::MultiValuesNotSupported(path.join(".")));
            }
            for el_json_val in els {
                if el_json_val.is_null() {
                    // We just ignore `null`.
                    continue;
                }
                if !self.concatenate.is_empty() {
                    let concat_values = self
                        .typ
                        .tantivy_value_from_json(el_json_val.clone())
                        .map_err(|err_msg| DocParsingError::ValueError(path.join("."), err_msg))?;
                    for concat_value in concat_values {
                        for field in &self.concatenate {
                            document.add_field_value(*field, &concat_value);
                        }
                    }
                }
                let value = self
                    .typ
                    .value_from_json(el_json_val)
                    .map_err(|err_msg| DocParsingError::ValueError(path.join("."), err_msg))?;
                document.add_field_value(self.field, &value);
            }
            return Ok(());
        }

        if !self.concatenate.is_empty() {
            let concat_values = self
                .typ
                .tantivy_value_from_json(json_val.clone())
                .map_err(|err_msg| DocParsingError::ValueError(path.join("."), err_msg))?;
            for concat_value in concat_values {
                for field in &self.concatenate {
                    document.add_field_value(*field, &concat_value);
                }
            }
        }
        let value = self
            .typ
            .value_from_json(json_val)
            .map_err(|err_msg| DocParsingError::ValueError(path.join("."), err_msg))?;
        document.add_field_value(self.field, &value);
        Ok(())
    }

    fn populate_json<'a>(
        &'a self,
        named_doc: &mut BTreeMap<String, Vec<TantivyValue>>,
        field_path: &[&'a str],
        doc_json: &mut serde_json::Map<String, JsonValue>,
    ) {
        if let Some(json_val) =
            extract_json_val(self.get_type(), named_doc, field_path, self.cardinality)
        {
            insert_json_val(field_path, json_val, doc_json);
        }
    }

    pub fn get_type(&self) -> &LeafType {
        &self.typ
    }
}

fn extract_json_val(
    leaf_type: &LeafType,
    named_doc: &mut BTreeMap<String, Vec<TantivyValue>>,
    field_path: &[&str],
    cardinality: Cardinality,
) -> Option<JsonValue> {
    let mut full_path = field_path.join(".");
    let vals: Vec<TantivyValue> = if let Some(vals) = named_doc.remove(&full_path) {
        // we have our value directly
        vals
    } else {
        let mut end_range = full_path.clone();
        full_path.push('.');
        // '/' is the character directly after . lexicographically
        end_range.push('/');

        // TODO use BTreeMap::drain once it exists and is stable
        let matches = named_doc
            .range::<String, _>(&full_path..&end_range)
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();

        if !matches.is_empty() {
            let mut map = Vec::new();
            for match_ in matches {
                let Some(suffix) = match_.strip_prefix(&full_path) else {
                    // this should never happen
                    continue;
                };
                let Some(tantivy_values) = named_doc.remove(&match_) else {
                    continue;
                };

                add_key_to_vec_map(&mut map, suffix, tantivy_values);
            }
            vec![TantivyValue::Object(map)]
        } else {
            // we didn't find our value, or any child of it, but maybe what we search is actually a
            // json field closer to the root?
            let mut split_point_iter = (1..(field_path.len())).rev();
            loop {
                let split_point = split_point_iter.next()?;
                let (doc_path, json_path) = field_path.split_at(split_point);
                let prefix_path = doc_path.join(".");
                if let Some(vals) = named_doc.get_mut(&prefix_path) {
                    // if we found a possible json field, there is no point in searching higher, our
                    // result would have been in it.
                    break extract_val_from_tantivy_val(json_path, vals);
                }
            }
        }
    };
    let mut vals_with_correct_type_it = vals
        .into_iter()
        .flat_map(|value| formatted_tantivy_value_to_json(value, leaf_type));
    match cardinality {
        Cardinality::SingleValued => vals_with_correct_type_it.next(),
        Cardinality::MultiValued => Some(JsonValue::Array(vals_with_correct_type_it.collect())),
    }
}

/// extract a subfield from a TantivyValue. The path must be non-empty
fn extract_val_from_tantivy_val(
    full_path: &[&str],
    tantivy_values: &mut [TantivyValue],
) -> Vec<TantivyValue> {
    // return *objects* matching path
    fn extract_val_aux<'a>(
        path: &[&str],
        tantivy_values: &'a mut [TantivyValue],
    ) -> Vec<&'a mut Vec<(String, TantivyValue)>> {
        let mut maps: Vec<&'a mut Vec<(String, TantivyValue)>> = tantivy_values
            .iter_mut()
            .filter_map(|value| {
                if let TantivyValue::Object(map) = value {
                    Some(map)
                } else {
                    None
                }
            })
            .collect();
        let mut scratch_buffer = Vec::new();
        for path_segment in path {
            scratch_buffer.extend(
                maps.drain(..)
                    .flatten()
                    .filter(|(key, _)| key == path_segment)
                    .filter_map(|(_, value)| {
                        if let TantivyValue::Object(map) = value {
                            Some(map)
                        } else {
                            None
                        }
                    }),
            );
            std::mem::swap(&mut maps, &mut scratch_buffer);
        }
        maps
    }

    let Some((last_segment, path)) = full_path.split_last() else {
        return Vec::new();
    };

    let mut results = Vec::new();
    for object in extract_val_aux(path, tantivy_values) {
        // TODO use extract_if once it's stable
        let mut i = 0;
        while i < object.len() {
            if object[i].0 == *last_segment {
                let (_, val) = object.swap_remove(i);
                match val {
                    TantivyValue::Array(mut vals) => results.append(&mut vals),
                    _ => results.push(val),
                }
            } else {
                i += 1;
            }
        }
    }

    results
}

fn add_key_to_vec_map(
    mut map: &mut Vec<(String, TantivyValue)>,
    suffix: &str,
    mut tantivy_value: Vec<TantivyValue>,
) {
    let Ok(full_inner_path) = crate::routing_expression::parse_field_name(suffix) else {
        return;
    };
    let Some((last_segment, inner_path)) = full_inner_path.split_last() else {
        return;
    };
    for path_segment in inner_path {
        // there is a cleaner way with find(), but the borrow checker is unhappy for no real reason
        // thinking there are lifetime issues between two exclusive branches
        map = if let Some(pos) = map.iter().position(|(key, _)| key == path_segment) {
            if let (_, TantivyValue::Object(ref mut value)) = map[pos] {
                value
            } else {
                // there is already a key before the end of the path ?!
                return;
            }
        } else {
            map.push((path_segment.to_string(), TantivyValue::Object(Vec::new())));
            let TantivyValue::Object(ref mut new_map) = map.last_mut().unwrap().1 else {
                unreachable!();
            };
            new_map
        }
    }
    // if we are here the doc mapping was changed from obj to json. We don't really know if the
    // field of that obj was multivalued or not. As a best effort, we say it was multivalued
    // if we have !=1 value. We could always return a vec, but then *every* field would be
    // transformed into an array of itself.
    if tantivy_value.len() == 1 {
        map.push((last_segment.to_string(), tantivy_value.pop().unwrap()));
    } else {
        map.push((last_segment.to_string(), TantivyValue::Array(tantivy_value)));
    }
}

fn insert_json_val(
    field_path: &[&str], //< may not be empty
    json_val: JsonValue,
    mut doc_json: &mut serde_json::Map<String, JsonValue>,
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
    /// Finds the field mapping type for a given field path in the mapping tree.
    /// Dots in `field_path_as_str` define the boundaries between field names.
    /// If a dot is part of a field name, it must be escaped with '\'.
    pub fn find_field_mapping_type(&self, field_path_as_str: &str) -> Option<FieldMappingType> {
        let field_path = build_field_path_from_str(field_path_as_str);
        self.internal_find_field_mapping_type(&field_path)
    }

    fn internal_find_field_mapping_type(&self, field_path: &[String]) -> Option<FieldMappingType> {
        let (first_path_fragment, sub_field_path) = field_path.split_first()?;
        let child_tree = self.branches.get(first_path_fragment)?;
        match (child_tree, sub_field_path.is_empty()) {
            (_, true) => Some(child_tree.clone().into()),
            (MappingTree::Leaf(_), false) => None,
            (MappingTree::Node(child_node), false) => {
                child_node.internal_find_field_mapping_type(sub_field_path)
            }
        }
    }

    /// Finds the field mapping type for a given field path in the mapping tree.
    /// Dots in `field_path_as_str` define the boundaries between field names.
    /// If a dot is part of a field name, it must be escaped with '\'.
    pub fn find_field_mapping_leaf(
        &mut self,
        field_path_as_str: &str,
    ) -> Option<impl Iterator<Item = &mut MappingLeaf>> {
        let field_path = build_field_path_from_str(field_path_as_str);
        self.internal_find_field_mapping_leaf(&field_path)
    }

    fn internal_find_field_mapping_leaf(
        &mut self,
        field_path: &[String],
    ) -> Option<impl Iterator<Item = &mut MappingLeaf>> {
        let (first_path_fragment, sub_field_path) = field_path.split_first()?;
        let child_tree = self.branches.get_mut(first_path_fragment)?;
        match (child_tree, sub_field_path.is_empty()) {
            (MappingTree::Leaf(_), false) => None,
            (MappingTree::Node(child_node), false) => {
                child_node.internal_find_field_mapping_leaf(sub_field_path)
            }
            (MappingTree::Leaf(leaf), true) => Some([leaf].into_iter()),
            (MappingTree::Node(_), true) => None,
        }
    }

    #[cfg(test)]
    pub fn num_fields(&self) -> usize {
        self.branches.len()
    }

    pub fn insert(&mut self, path: &str, node: MappingTree) {
        self.branches_order.push(path.to_string());
        self.branches.insert(path.to_string(), node);
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

    pub fn validate_from_json<'a>(
        &self,
        json_obj: &'a BorrowedJsonMap,
        strict_mode: bool,
        path: &mut Vec<&'a str>,
    ) -> Result<(), DocParsingError> {
        for (field_name, json_val) in json_obj.iter() {
            if let Some(child_tree) = self.branches.get(field_name) {
                path.push(field_name);
                child_tree.validate_from_json(json_val, path, strict_mode)?;
                path.pop();
            } else if strict_mode {
                path.push(field_name);
                let field_path = path.join(".");
                return Err(DocParsingError::NoSuchFieldInSchema(field_path));
            }
        }
        Ok(())
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
        named_doc: &mut BTreeMap<String, Vec<TantivyValue>>,
        field_path: &mut Vec<&'a str>,
        doc_json: &mut serde_json::Map<String, JsonValue>,
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
            LeafType::Bool(opt) => FieldMappingType::Bool(opt, leaf.cardinality),
            LeafType::IpAddr(opt) => FieldMappingType::IpAddr(opt, leaf.cardinality),
            LeafType::DateTime(opt) => FieldMappingType::DateTime(opt, leaf.cardinality),
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
    fn validate_from_json<'a>(
        &self,
        json_value: &'a BorrowedJsonValue<'a>,
        field_path: &mut Vec<&'a str>,
        strict_mode: bool,
    ) -> Result<(), DocParsingError> {
        match self {
            MappingTree::Leaf(mapping_leaf) => {
                mapping_leaf.validate_from_json(json_value, field_path)
            }
            MappingTree::Node(mapping_node) => {
                if let Some(json_obj) = json_value.as_object() {
                    mapping_node.validate_from_json(json_obj, strict_mode, field_path)
                } else {
                    Err(DocParsingError::ValueError(
                        field_path.join("."),
                        format!("expected an JSON object, got {json_value}"),
                    ))
                }
            }
        }
    }

    fn doc_from_json(
        &self,
        json_value: JsonValue,
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
                        format!("expected an JSON object, got {json_value}"),
                    ))
                }
            }
        }
    }

    fn populate_json<'a>(
        &'a self,
        named_doc: &mut BTreeMap<String, Vec<TantivyValue>>,
        field_path: &mut Vec<&'a str>,
        doc_json: &mut serde_json::Map<String, JsonValue>,
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

pub(crate) struct MappingNodeRoot {
    /// The root of a mapping tree
    pub field_mappings: MappingNode,
    /// The list of concatenate fields which includes the dynamic field
    pub concatenate_dynamic_fields: Vec<Field>,
}

pub(crate) fn build_mapping_tree(
    entries: &[FieldMappingEntry],
    schema: &mut SchemaBuilder,
) -> anyhow::Result<MappingNodeRoot> {
    let mut field_path = Vec::new();
    build_mapping_tree_from_entries(entries, &mut field_path, schema)
}

fn build_mapping_tree_from_entries<'a>(
    entries: &'a [FieldMappingEntry],
    field_path: &mut Vec<&'a str>,
    schema: &mut SchemaBuilder,
) -> anyhow::Result<MappingNodeRoot> {
    let mut mapping_node = MappingNode::default();
    let mut concatenate_fields = Vec::new();
    let mut concatenate_dynamic_fields = Vec::new();
    for entry in entries {
        if let FieldMappingType::Concatenate(_) = &entry.mapping_type {
            concatenate_fields.push(entry);
        } else {
            field_path.push(&entry.name);
            if mapping_node.branches.contains_key(&entry.name) {
                bail!("duplicated field definition `{}`", entry.name);
            }
            let (child_tree, mut dynamic_fields) =
                build_mapping_from_field_type(&entry.mapping_type, field_path, schema)?;
            field_path.pop();
            mapping_node.insert(&entry.name, child_tree);
            concatenate_dynamic_fields.append(&mut dynamic_fields);
        }
    }
    for concatenate_field_entry in concatenate_fields {
        let FieldMappingType::Concatenate(options) = &concatenate_field_entry.mapping_type else {
            // we only pushed Concatenate fields in `concatenate_fields`
            unreachable!();
        };
        let name = &concatenate_field_entry.name;
        if mapping_node.branches.contains_key(name) {
            bail!("duplicated field definition `{}`", name);
        }
        let text_options: JsonObjectOptions = options.clone().into();
        let field = schema.add_json_field(name, text_options);
        for sub_field in &options.concatenate_fields {
            for matched_field in
                mapping_node
                    .find_field_mapping_leaf(sub_field)
                    .ok_or_else(|| {
                        anyhow::anyhow!("concatenate field uses an unknown field `{sub_field}`")
                    })?
            {
                if !matched_field.typ.supported_for_concat() {
                    bail!(
                        "subfield `{}` not supported inside a concatenate field",
                        sub_field
                    );
                }
                matched_field.concatenate.push(field);
            }
        }
        if options.include_dynamic_fields {
            concatenate_dynamic_fields.push(field);
        }
    }
    Ok(MappingNodeRoot {
        field_mappings: mapping_node,
        concatenate_dynamic_fields,
    })
}

fn get_numeric_options_for_bool_field(
    quickwit_bool_options: &QuickwitBoolOptions,
) -> NumericOptions {
    let mut numeric_options = NumericOptions::default();
    if quickwit_bool_options.stored {
        numeric_options = numeric_options.set_stored();
    }
    if quickwit_bool_options.indexed {
        numeric_options = numeric_options.set_indexed();
    }
    if quickwit_bool_options.fast {
        numeric_options = numeric_options.set_fast();
    }
    numeric_options
}

fn get_numeric_options_for_numeric_field(
    quickwit_numeric_options: &QuickwitNumericOptions,
) -> NumericOptions {
    let mut numeric_options = NumericOptions::default();
    if quickwit_numeric_options.stored {
        numeric_options = numeric_options.set_stored();
    }
    if quickwit_numeric_options.indexed {
        numeric_options = numeric_options.set_indexed();
    }
    if quickwit_numeric_options.fast {
        numeric_options = numeric_options.set_fast();
    }
    numeric_options
}

fn get_date_time_options(quickwit_date_time_options: &QuickwitDateTimeOptions) -> DateOptions {
    let mut date_time_options = DateOptions::default();
    if quickwit_date_time_options.stored {
        date_time_options = date_time_options.set_stored();
    }
    if quickwit_date_time_options.indexed {
        date_time_options = date_time_options.set_indexed();
    }
    if quickwit_date_time_options.fast {
        date_time_options = date_time_options.set_fast();
    }
    date_time_options.set_precision(quickwit_date_time_options.fast_precision)
}

fn get_bytes_options(quickwit_numeric_options: &QuickwitBytesOptions) -> BytesOptions {
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

fn get_ip_address_options(quickwit_ip_address_options: &QuickwitIpAddrOptions) -> IpAddrOptions {
    let mut ip_address_options = IpAddrOptions::default();
    if quickwit_ip_address_options.stored {
        ip_address_options = ip_address_options.set_stored();
    }
    if quickwit_ip_address_options.indexed {
        ip_address_options = ip_address_options.set_indexed();
    }
    if quickwit_ip_address_options.fast {
        ip_address_options = ip_address_options.set_fast();
    }
    ip_address_options
}

/// Creates a tantivy field name for a given field path.
///
/// By field path, we mean the list of `field_name` that are crossed
/// to reach the field starting from the root of the document.
/// There can be more than one due to quickwit object type.
///
/// We simply concatenate these field names, interleaving them with '.'.
/// If a fieldname itself contains a '.', we escape it with '\'.
/// ('\' itself is forbidden).
fn field_name_for_field_path(field_path: &[&str]) -> String {
    field_path.iter().cloned().map(escape_dots).join(".")
}

/// Builds the sequence of field names crossed to reach the field
/// starting from the root of the document.
/// Dots '.' define the boundaries between field names.
/// If a dot is part of a field name, it must be escaped with '\'.
pub(crate) fn build_field_path_from_str(field_path_as_str: &str) -> Vec<String> {
    let mut field_path = Vec::new();
    let mut current_path_fragment = String::new();
    let mut escaped = false;
    for char in field_path_as_str.chars() {
        if escaped {
            current_path_fragment.push(char);
            escaped = false;
        } else if char == '\\' {
            escaped = true;
        } else if char == '.' {
            let path_fragment = std::mem::take(&mut current_path_fragment);
            field_path.push(path_fragment);
        } else {
            current_path_fragment.push(char);
        }
    }
    if !current_path_fragment.is_empty() {
        field_path.push(current_path_fragment);
    }
    field_path
}

fn escape_dots(field_name: &str) -> String {
    let mut escaped_field_name = String::new();
    for chr in field_name.chars() {
        if chr == '.' {
            escaped_field_name.push('\\');
        }
        escaped_field_name.push(chr);
    }
    escaped_field_name
}

/// build a sub-mapping tree from the fields it contains.
///
/// also returns the list of concatenate fields which consume the dynamic field
fn build_mapping_from_field_type<'a>(
    field_mapping_type: &'a FieldMappingType,
    field_path: &mut Vec<&'a str>,
    schema_builder: &mut SchemaBuilder,
) -> anyhow::Result<(MappingTree, Vec<Field>)> {
    let field_name = field_name_for_field_path(field_path);
    match field_mapping_type {
        FieldMappingType::Text(options, cardinality) => {
            let text_options: TextOptions = options.clone().into();
            let field = schema_builder.add_text_field(&field_name, text_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::Text(options.clone()),
                cardinality: *cardinality,
                concatenate: Vec::new(),
            };
            Ok((MappingTree::Leaf(mapping_leaf), Vec::new()))
        }
        FieldMappingType::I64(options, cardinality) => {
            let numeric_options = get_numeric_options_for_numeric_field(options);
            let field = schema_builder.add_i64_field(&field_name, numeric_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::I64(options.clone()),
                cardinality: *cardinality,
                concatenate: Vec::new(),
            };
            Ok((MappingTree::Leaf(mapping_leaf), Vec::new()))
        }
        FieldMappingType::U64(options, cardinality) => {
            let numeric_options = get_numeric_options_for_numeric_field(options);
            let field = schema_builder.add_u64_field(&field_name, numeric_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::U64(options.clone()),
                cardinality: *cardinality,
                concatenate: Vec::new(),
            };
            Ok((MappingTree::Leaf(mapping_leaf), Vec::new()))
        }
        FieldMappingType::F64(options, cardinality) => {
            let numeric_options = get_numeric_options_for_numeric_field(options);
            let field = schema_builder.add_f64_field(&field_name, numeric_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::F64(options.clone()),
                cardinality: *cardinality,
                concatenate: Vec::new(),
            };
            Ok((MappingTree::Leaf(mapping_leaf), Vec::new()))
        }
        FieldMappingType::Bool(options, cardinality) => {
            let numeric_options = get_numeric_options_for_bool_field(options);
            let field = schema_builder.add_bool_field(&field_name, numeric_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::Bool(options.clone()),
                cardinality: *cardinality,
                concatenate: Vec::new(),
            };
            Ok((MappingTree::Leaf(mapping_leaf), Vec::new()))
        }
        FieldMappingType::IpAddr(options, cardinality) => {
            let ip_addr_options = get_ip_address_options(options);
            let field = schema_builder.add_ip_addr_field(&field_name, ip_addr_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::IpAddr(options.clone()),
                cardinality: *cardinality,
                concatenate: Vec::new(),
            };
            Ok((MappingTree::Leaf(mapping_leaf), Vec::new()))
        }
        FieldMappingType::DateTime(options, cardinality) => {
            let date_time_options = get_date_time_options(options);
            let field = schema_builder.add_date_field(&field_name, date_time_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::DateTime(options.clone()),
                cardinality: *cardinality,
                concatenate: Vec::new(),
            };
            Ok((MappingTree::Leaf(mapping_leaf), Vec::new()))
        }
        FieldMappingType::Bytes(options, cardinality) => {
            let bytes_options = get_bytes_options(options);
            let field = schema_builder.add_bytes_field(&field_name, bytes_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::Bytes(options.clone()),
                cardinality: *cardinality,
                concatenate: Vec::new(),
            };
            Ok((MappingTree::Leaf(mapping_leaf), Vec::new()))
        }
        FieldMappingType::Json(options, cardinality) => {
            let json_options = JsonObjectOptions::from(options.clone());
            let field = schema_builder.add_json_field(&field_name, json_options);
            let mapping_leaf = MappingLeaf {
                field,
                typ: LeafType::Json(options.clone()),
                cardinality: *cardinality,
                concatenate: Vec::new(),
            };
            Ok((MappingTree::Leaf(mapping_leaf), Vec::new()))
        }
        FieldMappingType::Object(entries) => {
            let MappingNodeRoot {
                field_mappings,
                concatenate_dynamic_fields,
            } = build_mapping_tree_from_entries(
                &entries.field_mappings,
                field_path,
                schema_builder,
            )?;
            Ok((
                MappingTree::Node(field_mappings),
                concatenate_dynamic_fields,
            ))
        }
        FieldMappingType::Concatenate(_) => {
            bail!("Concatenate shouldn't reach build_mapping_from_field_type: this is a bug")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use serde_json::{json, Value as JsonValue};
    use tantivy::schema::{Field, IntoIpv6Addr, OwnedValue as TantivyValue, Value};
    use tantivy::{DateTime, TantivyDocument as Document};
    use time::macros::datetime;
    use time::OffsetDateTime;

    use super::{
        add_key_to_vec_map, extract_val_from_tantivy_val, JsonValueIterator, LeafType,
        MapOrArrayIter, MappingLeaf,
    };
    use crate::doc_mapper::date_time_type::QuickwitDateTimeOptions;
    use crate::doc_mapper::field_mapping_entry::{
        BinaryFormat, QuickwitBoolOptions, QuickwitBytesOptions, QuickwitIpAddrOptions,
        QuickwitNumericOptions, QuickwitTextOptions,
    };
    use crate::Cardinality;

    #[test]
    fn test_field_name_from_field_path() {
        // not really a possibility, but still, let's test it.
        assert_eq!(super::field_name_for_field_path(&[]), "");
        assert_eq!(super::field_name_for_field_path(&["hello"]), "hello");
        assert_eq!(
            super::field_name_for_field_path(&["one", "two", "three"]),
            "one.two.three"
        );
        assert_eq!(
            super::field_name_for_field_path(&["one", "two", "three"]),
            "one.two.three"
        );
        assert_eq!(super::field_name_for_field_path(&["one.two"]), r"one\.two");
        assert_eq!(
            super::field_name_for_field_path(&["one.two", "three"]),
            r"one\.two.three"
        );
    }

    #[test]
    fn test_get_or_insert_path() {
        let mut map = Default::default();
        super::get_or_insert_path(&["a".to_string(), "b".to_string()], &mut map)
            .insert("c".to_string(), JsonValue::from(3u64));
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
            .insert("d".to_string(), JsonValue::from(2u64));
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
            .insert("f".to_string(), JsonValue::from(5u64));
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
        super::get_or_insert_path(&[], &mut map).insert("g".to_string(), JsonValue::from(6u64));
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
            TantivyValue::U64(20u64)
        );
    }

    #[test]
    fn test_parse_u64_coercion() {
        let leaf = LeafType::U64(QuickwitNumericOptions::default());
        assert_eq!(
            leaf.value_from_json(json!("20")).unwrap(),
            TantivyValue::U64(20u64)
        );
        assert_eq!(
            leaf.value_from_json(json!("foo")).unwrap_err(),
            "failed to coerce JSON string `\"foo\"` to u64"
        );

        let numeric_options = QuickwitNumericOptions {
            coerce: false,
            ..Default::default()
        };
        let leaf = LeafType::U64(numeric_options);
        assert_eq!(
            leaf.value_from_json(json!("20")).unwrap_err(),
            "expected JSON number, got string `\"20\"`. enable coercion to u64 with the `coerce` \
             parameter in the field mapping"
        );
    }

    #[test]
    fn test_parse_u64_negative_should_error() {
        let leaf = LeafType::U64(QuickwitNumericOptions::default());
        assert_eq!(
            leaf.value_from_json(json!(-20i64)).unwrap_err(),
            "expected u64, got inconvertible JSON number `-20`"
        );
    }

    #[test]
    fn test_parse_i64_mapping() {
        let leaf = LeafType::I64(QuickwitNumericOptions::default());
        assert_eq!(
            leaf.value_from_json(json!(20u64)).unwrap(),
            TantivyValue::I64(20i64)
        );
    }

    #[test]
    fn test_parse_i64_from_f64_should_error() {
        let leaf = LeafType::I64(QuickwitNumericOptions::default());
        assert_eq!(
            leaf.value_from_json(json!(20.2f64)).unwrap_err(),
            "expected i64, got inconvertible JSON number `20.2`"
        );
    }

    #[test]
    fn test_parse_i64_too_large() {
        let leaf = LeafType::I64(QuickwitNumericOptions::default());
        let err = leaf.value_from_json(json!(u64::MAX)).err().unwrap();
        assert_eq!(
            err,
            "expected i64, got inconvertible JSON number `18446744073709551615`"
        );
    }

    #[test]
    fn test_parse_f64_from_u64() {
        let leaf = LeafType::F64(QuickwitNumericOptions::default());
        assert_eq!(
            leaf.value_from_json(json!(4_000u64)).unwrap(),
            TantivyValue::F64(4_000f64)
        );
    }

    #[test]
    fn test_parse_bool_mapping() {
        let leaf = LeafType::Bool(QuickwitBoolOptions::default());
        assert_eq!(
            leaf.value_from_json(json!(true)).unwrap(),
            TantivyValue::Bool(true)
        );
    }

    #[test]
    fn test_parse_bool_multivalued() {
        let typ = LeafType::Bool(QuickwitBoolOptions::default());
        let field = Field::from_field_id(10);
        let leaf_entry = MappingLeaf {
            field,
            typ,
            cardinality: Cardinality::MultiValued,
            concatenate: Vec::new(),
        };
        let mut document = Document::default();
        let mut path = Vec::new();
        leaf_entry
            .doc_from_json(json!([true, false, true]), &mut document, &mut path)
            .unwrap();
        assert_eq!(document.len(), 3);
        let values: Vec<bool> = document
            .get_all(field)
            .flat_map(|val| val.as_bool())
            .collect();
        assert_eq!(&values, &[true, false, true])
    }

    #[test]
    fn test_parse_ip_addr_from_str() {
        let leaf = LeafType::IpAddr(QuickwitIpAddrOptions::default());
        let ips = vec![
            "127.0.0.0",
            "2605:2700:0:3::4713:93e3",
            "::afff:4567:890a",
            "10.10.12.123",
            "192.168.0.1",
            "2001:db8::1:0:0:1",
        ];
        for ip_str in ips {
            let parsed_ip_addr = leaf.value_from_json(json!(ip_str)).unwrap();
            let expected_ip_addr =
                TantivyValue::IpAddr(ip_str.parse::<IpAddr>().unwrap().into_ipv6_addr());
            assert_eq!(parsed_ip_addr, expected_ip_addr);
        }
    }

    #[test]
    fn test_parse_ip_addr_should_error() {
        let typ = LeafType::IpAddr(QuickwitIpAddrOptions::default());
        let err = typ.value_from_json(json!("foo")).err().unwrap();
        assert!(err.contains("failed to parse IP address `foo`"));

        let err = typ.value_from_json(json!(1200)).err().unwrap();
        assert!(err.contains("expected string, got `1200`"));
    }

    #[test]
    fn test_parse_i64_mutivalued() {
        let typ = LeafType::I64(QuickwitNumericOptions::default());
        let field = Field::from_field_id(10);
        let leaf_entry = MappingLeaf {
            field,
            typ,
            cardinality: Cardinality::MultiValued,
            concatenate: Vec::new(),
        };
        let mut document = Document::default();
        let mut path = Vec::new();
        leaf_entry
            .doc_from_json(serde_json::json!([10u64, 20u64]), &mut document, &mut path)
            .unwrap();
        assert_eq!(document.len(), 2);
        let values: Vec<i64> = document
            .get_all(field)
            .flat_map(|val| val.as_i64())
            .collect();
        assert_eq!(&values, &[10i64, 20i64]);
    }

    #[test]
    fn test_parse_null_is_just_ignored() {
        let typ = LeafType::I64(QuickwitNumericOptions::default());
        let field = Field::from_field_id(10);
        let leaf_entry = MappingLeaf {
            field,
            typ,
            cardinality: Cardinality::MultiValued,
            concatenate: Vec::new(),
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
            cardinality: Cardinality::MultiValued,
            concatenate: Vec::new(),
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
            cardinality: Cardinality::MultiValued,
            concatenate: Vec::new(),
        };
        let mut document = Document::default();
        let mut path = vec!["root".to_string(), "my_field".to_string()];
        let parse_err = leaf_entry
            .doc_from_json(
                serde_json::json!([10u64, [1u64, 2u64]]),
                &mut document,
                &mut path,
            )
            .unwrap_err();
        assert_eq!(
            parse_err.to_string(),
            "the field `root.my_field` could not be parsed: expected JSON number or string, got \
             `[1,2]`"
        );
    }

    #[test]
    fn test_parse_text() {
        let typ = LeafType::Text(QuickwitTextOptions::default());
        let parsed_value = typ.value_from_json(json!("bacon and eggs")).unwrap();
        assert_eq!(
            parsed_value,
            TantivyValue::Str("bacon and eggs".to_string())
        );
    }

    #[test]
    fn test_parse_text_number_should_error() {
        let typ = LeafType::Text(QuickwitTextOptions::default());
        let err = typ.value_from_json(json!(2u64)).err().unwrap();
        assert_eq!(err, "expected string, got `2`");
    }

    #[test]
    fn test_parse_date_time_str() {
        let typ = LeafType::DateTime(QuickwitDateTimeOptions::default());
        let value = typ
            .value_from_json(json!("2021-12-19T16:39:57-01:00"))
            .unwrap();
        let date_time = datetime!(2021-12-19 17:39:57 UTC);
        assert_eq!(value, TantivyValue::Date(DateTime::from_utc(date_time)));
    }

    #[test]
    fn test_parse_timestamp_float() {
        let typ = LeafType::DateTime(QuickwitDateTimeOptions::default());
        let unix_ts_secs = OffsetDateTime::now_utc().unix_timestamp();
        let value = typ
            .value_from_json(json!(unix_ts_secs as f64 + 0.1))
            .unwrap();
        let date_time = match value {
            TantivyValue::Date(date_time) => date_time,
            other => panic!("Expected a tantivy date time, got `{other:?}`."),
        };
        assert!((date_time.into_timestamp_millis() - (unix_ts_secs * 1_000 + 100)).abs() <= 1);
    }

    #[test]
    fn test_parse_timestamp_int() {
        let typ = LeafType::DateTime(QuickwitDateTimeOptions::default());
        let unix_ts_secs = OffsetDateTime::now_utc().unix_timestamp();
        let value = typ.value_from_json(json!(unix_ts_secs)).unwrap();
        assert_eq!(
            value,
            TantivyValue::Date(DateTime::from_timestamp_secs(unix_ts_secs))
        );
    }

    #[test]
    fn test_parse_date_number_should_error() {
        let typ = LeafType::DateTime(QuickwitDateTimeOptions::default());
        let err = typ.value_from_json(json!("foo-datetime")).unwrap_err();
        assert_eq!(
            err,
            "failed to parse datetime `foo-datetime` using the following formats: `rfc3339`, \
             `unix_timestamp`"
        );
    }

    #[test]
    fn test_parse_date_array_should_error() {
        let typ = LeafType::DateTime(QuickwitDateTimeOptions::default());
        let err = typ.value_from_json(json!(["foo", "bar"])).err().unwrap();
        assert_eq!(
            err,
            "failed to parse datetime: expected a float, integer, or string, got \
             `[\"foo\",\"bar\"]`"
        );
    }

    #[test]
    fn test_parse_bytes() {
        let typ = LeafType::Bytes(QuickwitBytesOptions::default());
        let value = typ
            .value_from_json(json!("dGhpcyBpcyBhIGJhc2U2NCBlbmNvZGVkIHN0cmluZw=="))
            .unwrap();
        assert_eq!(
            (&value).as_bytes().unwrap(),
            b"this is a base64 encoded string"
        );
    }

    #[test]
    fn test_parse_bytes_hex() {
        let typ = LeafType::Bytes(QuickwitBytesOptions {
            input_format: BinaryFormat::Hex,
            ..QuickwitBytesOptions::default()
        });
        let value = typ
            .value_from_json(json!(
                "7468697320697320612068657820656e636f64656420737472696e67"
            ))
            .unwrap();
        assert_eq!(
            (&value).as_bytes().unwrap(),
            b"this is a hex encoded string"
        );
    }

    #[test]
    fn test_parse_bytes_number_should_err() {
        let typ = LeafType::Bytes(QuickwitBytesOptions::default());
        let error = typ.value_from_json(json!(2u64)).err().unwrap();
        assert_eq!(error, "expected base64 string, got `2`");
    }

    #[test]
    fn test_parse_bytes_invalid_base64() {
        let typ = LeafType::Bytes(QuickwitBytesOptions::default());
        let error = typ.value_from_json(json!("dEwerwer#!%")).err().unwrap();
        assert_eq!(
            error,
            "expected base64 string, got `dEwerwer#!%`: Invalid symbol 35, offset 8."
        );
    }

    #[test]
    fn test_parse_array_of_bytes() {
        let typ = LeafType::Bytes(QuickwitBytesOptions::default());
        let field = Field::from_field_id(10);
        let leaf_entry = MappingLeaf {
            field,
            typ,
            cardinality: Cardinality::MultiValued,
            concatenate: Vec::new(),
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
        let bytes_vec: Vec<&[u8]> = document
            .get_all(field)
            .flat_map(|val| val.as_bytes())
            .collect();
        assert_eq!(
            &bytes_vec[..],
            &[
                b"this is a base64 encoded string",
                b"this is a base64 encoded string"
            ]
        )
    }

    #[test]
    fn test_field_path_for_field_name() {
        assert_eq!(super::build_field_path_from_str(""), Vec::<String>::new());
        assert_eq!(super::build_field_path_from_str("hello"), vec!["hello"]);
        assert_eq!(
            super::build_field_path_from_str("one.two.three"),
            vec!["one", "two", "three"]
        );
        assert_eq!(
            super::build_field_path_from_str(r"one\.two"),
            vec!["one.two"]
        );
        assert_eq!(
            super::build_field_path_from_str(r"one\.two.three"),
            vec!["one.two", "three"]
        );
        assert_eq!(super::build_field_path_from_str(r#"one."#), vec!["one"]);
        // Those are invalid field paths, but we check that it does not panic.
        // Issue #3538 is about validating field paths before trying to build the path.
        assert_eq!(super::build_field_path_from_str("\\."), vec!["."]);
        assert_eq!(super::build_field_path_from_str("a."), vec!["a"]);
        assert_eq!(super::build_field_path_from_str(".a"), vec!["", "a"]);
    }

    #[test]
    fn test_map_or_array_iter() {
        // single element
        let single_value = MapOrArrayIter::Value(json!({"a": "b", "c": 4}));
        let res: Vec<_> = single_value.collect();
        assert_eq!(res, vec![json!({"a": "b", "c": 4})]);

        // array of elements
        let multiple_values =
            MapOrArrayIter::Array(vec![json!({"a": "b", "c": 4}), json!(5)].into_iter());
        let res: Vec<_> = multiple_values.collect();
        assert_eq!(res, vec![json!({"a": "b", "c": 4}), json!(5)]);

        // map of elements
        let multiple_values = MapOrArrayIter::Map(
            json!({"a": {"a": "b", "c": 4}, "b":5})
                .as_object()
                .unwrap()
                .clone()
                .into_iter(),
        );
        let res: Vec<_> = multiple_values.collect();
        assert_eq!(res, vec![json!({"a": "b", "c": 4}), json!(5)]);
    }

    #[test]
    fn test_json_value_iterator() {
        assert_eq!(
            JsonValueIterator::new(json!(5)).collect::<Vec<_>>(),
            vec![json!(5)]
        );
        assert_eq!(
            JsonValueIterator::new(json!([5, "a"])).collect::<Vec<_>>(),
            vec![json!(5), json!("a")]
        );
        assert_eq!(
            JsonValueIterator::new(json!({"a":1, "b": 2})).collect::<Vec<_>>(),
            vec![json!(1), json!(2)]
        );
        assert_eq!(
            JsonValueIterator::new(json!([{"a":1, "b": 2}, "a"])).collect::<Vec<_>>(),
            vec![json!(1), json!(2), json!("a")]
        );
        assert_eq!(
            JsonValueIterator::new(json!([{"a":1, "b": 2}, {"a": {"b": [3, 4]}}]))
                .collect::<Vec<_>>(),
            vec![json!(1), json!(2), json!(3), json!(4)]
        );
    }

    #[test]
    fn test_extract_val_from_tantivy_val() {
        let obj = TantivyValue::Object;
        fn array(val: impl IntoIterator<Item = impl Into<TantivyValue>>) -> TantivyValue {
            TantivyValue::Array(val.into_iter().map(Into::into).collect())
        }

        let mut sample = vec![obj(vec![
            (
                "some".to_string(),
                obj(vec![
                    (
                        "path".to_string(),
                        obj(vec![("with.dots".to_string(), 1u64.into())]),
                    ),
                    (
                        "other".to_string(),
                        obj(vec![("path".to_string(), array([2u64, 3]))]),
                    ),
                ]),
            ),
            ("short".to_string(), 4u64.into()),
        ])];

        assert_eq!(
            extract_val_from_tantivy_val(&["some", "other"], &mut sample),
            vec![obj(vec![("path".to_string(), array([2u64, 3]))])]
        );
        assert_eq!(
            extract_val_from_tantivy_val(&["some", "other"], &mut sample),
            Vec::new()
        );
        assert_eq!(
            extract_val_from_tantivy_val(&["some", "path", "with.dots"], &mut sample),
            vec![1u64.into()]
        );
        assert_eq!(
            extract_val_from_tantivy_val(&["some", "path", "with.dots"], &mut sample),
            Vec::new()
        );
        assert_eq!(
            extract_val_from_tantivy_val(&["short"], &mut sample),
            vec![4u64.into()]
        );
        assert_eq!(
            extract_val_from_tantivy_val(&["short"], &mut sample),
            Vec::new()
        );
    }

    #[test]
    fn test_add_key_to_vec_map() {
        let obj = TantivyValue::Object;
        fn array(val: impl IntoIterator<Item = impl Into<TantivyValue>>) -> TantivyValue {
            TantivyValue::Array(val.into_iter().map(Into::into).collect())
        }

        let mut map = Vec::new();

        add_key_to_vec_map(&mut map, "some.path.with\\.dots", vec![1u64.into()]);
        assert_eq!(
            map,
            &[(
                "some".to_string(),
                obj(vec![(
                    "path".to_string(),
                    obj(vec![("with.dots".to_string(), 1u64.into())])
                )])
            )]
        );

        add_key_to_vec_map(&mut map, "some.other.path", vec![2u64.into(), 3u64.into()]);
        assert_eq!(
            map,
            &[(
                "some".to_string(),
                obj(vec![
                    (
                        "path".to_string(),
                        obj(vec![("with.dots".to_string(), 1u64.into())])
                    ),
                    (
                        "other".to_string(),
                        obj(vec![("path".to_string(), array([2u64, 3]))])
                    ),
                ])
            )]
        );

        add_key_to_vec_map(&mut map, "short", vec![4u64.into()]);
        assert_eq!(
            map,
            &[
                (
                    "some".to_string(),
                    obj(vec![
                        (
                            "path".to_string(),
                            obj(vec![("with.dots".to_string(), 1u64.into())])
                        ),
                        (
                            "other".to_string(),
                            obj(vec![("path".to_string(), array([2u64, 3]))])
                        ),
                    ])
                ),
                ("short".to_string(), 4u64.into())
            ]
        );
    }
}
