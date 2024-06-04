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

use std::any::type_name;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::str::FromStr;

use anyhow::bail;
use itertools::Itertools;
use serde_json::Value as JsonValue;
use tantivy::schema::{
    BytesOptions, DateOptions, Field, IntoIpv6Addr, IpAddrOptions, JsonObjectOptions,
    NumericOptions, OwnedValue as TantivyValue, SchemaBuilder, TextOptions,
};
use tantivy::tokenizer::{PreTokenizedString, Token};
use tantivy::TantivyDocument as Document;
use tracing::warn;

use super::date_time_type::QuickwitDateTimeOptions;
use super::field_mapping_entry::{NumericOutputFormat, QuickwitBoolOptions};
use crate::default_doc_mapper::field_mapping_entry::{
    QuickwitBytesOptions, QuickwitIpAddrOptions, QuickwitNumericOptions, QuickwitObjectOptions,
    QuickwitTextOptions,
};
use crate::default_doc_mapper::{FieldMappingType, QuickwitJsonOptions};
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

pub(crate) fn value_to_pretokenized<T: ToString>(val: T) -> PreTokenizedString {
    let text = val.to_string();
    PreTokenizedString {
        text: text.clone(),
        tokens: vec![Token {
            offset_from: 0,
            offset_to: 1,
            position: 0,
            text,
            position_length: 1,
        }],
    }
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
        JsonValue::Bool(val) => Some(value_to_pretokenized(val).into()),
        JsonValue::Number(number) => {
            if let Some(val) = u64::from_json_number(&number) {
                Some(value_to_pretokenized(val).into())
            } else {
                i64::from_json_number(&number).map(|val| value_to_pretokenized(val).into())
            }
        }
    }
}

impl LeafType {
    fn value_from_json(&self, json_val: JsonValue) -> Result<TantivyValue, String> {
        match self {
            LeafType::Text(_) => {
                if let JsonValue::String(text) = json_val {
                    Ok(TantivyValue::Str(text))
                } else {
                    Err(format!("expected string, got `{json_val}`"))
                }
            }
            LeafType::I64(numeric_options) => i64::from_json(json_val, numeric_options.coerce),
            LeafType::U64(numeric_options) => u64::from_json(json_val, numeric_options.coerce),
            LeafType::F64(numeric_options) => f64::from_json(json_val, numeric_options.coerce),
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
            LeafType::DateTime(date_time_options) => date_time_options.parse_json(json_val),
            LeafType::Bytes(binary_options) => binary_options.input_format.parse_json(json_val),
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

    fn tantivy_string_value_from_json(
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
                let val = i64::from_json_to_self(json_val, numeric_options.coerce)?;
                Ok(OneOrIter::one(value_to_pretokenized(val).into()))
            }
            LeafType::U64(numeric_options) => {
                let val = u64::from_json_to_self(json_val, numeric_options.coerce)?;
                Ok(OneOrIter::one(value_to_pretokenized(val).into()))
            }
            LeafType::F64(_) => Err("unsuported concat type: f64".to_string()),
            LeafType::Bool(_) => {
                if let JsonValue::Bool(val) = json_val {
                    Ok(OneOrIter::one(value_to_pretokenized(val).into()))
                } else {
                    Err(format!("expected boolean, got `{json_val}`"))
                }
            }
            LeafType::IpAddr(_) => Err("unsuported concat type: IpAddr".to_string()),
            LeafType::DateTime(_date_time_options) => {
                Err("unsuported concat type: DateTime".to_string())
            }
            LeafType::Bytes(_binary_options) => Err("unsuported concat type: DateTime".to_string()),
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
        matches!(self, Text(_) | U64(_) | I64(_) | Bool(_) | Json(_))
        /*
            // will be supported if possible
            DateTime(_),
            IpAddr(_),
            // won't be supported
            Bytes(_),
            F64(_),
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
            if self.cardinality == Cardinality::SingleValue {
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
                        .tantivy_string_value_from_json(el_json_val.clone())
                        .map_err(|err_msg| DocParsingError::ValueError(path.join("."), err_msg))?;
                    for concat_value in concat_values {
                        for field in &self.concatenate {
                            document.add_field_value(*field, concat_value.clone());
                        }
                    }
                }
                let value = self
                    .typ
                    .value_from_json(el_json_val)
                    .map_err(|err_msg| DocParsingError::ValueError(path.join("."), err_msg))?;
                document.add_field_value(self.field, value);
            }
            return Ok(());
        }

        if !self.concatenate.is_empty() {
            let concat_values = self
                .typ
                .tantivy_string_value_from_json(json_val.clone())
                .map_err(|err_msg| DocParsingError::ValueError(path.join("."), err_msg))?;
            for concat_value in concat_values {
                for field in &self.concatenate {
                    document.add_field_value(*field, concat_value.clone());
                }
            }
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
    let full_path = field_path.join(".");
    let vals = named_doc.remove(&full_path)?;
    let mut vals_with_correct_type_it = vals
        .into_iter()
        .flat_map(|value| value_to_json(value, leaf_type));
    match cardinality {
        Cardinality::SingleValue => vals_with_correct_type_it.next(),
        Cardinality::MultiValues => Some(JsonValue::Array(vals_with_correct_type_it.collect())),
    }
}

fn value_to_string(value: &TantivyValue) -> Option<JsonValue> {
    match value {
        TantivyValue::Str(s) => Some(s.clone()),
        TantivyValue::U64(number) => Some(number.to_string()),
        TantivyValue::I64(number) => Some(number.to_string()),
        TantivyValue::F64(number) => Some(number.to_string()),
        TantivyValue::Bool(b) => Some(b.to_string()),
        TantivyValue::Date(date) => {
            return quickwit_datetime::DateTimeOutputFormat::default()
                .format_to_json(*date)
                .ok()
        }
        TantivyValue::IpAddr(ip) => Some(ip.to_string()),
        _ => None,
    }
    .map(JsonValue::String)
}

fn value_to_bool(value: &TantivyValue) -> Option<JsonValue> {
    match value {
        TantivyValue::Str(s) => s.parse().ok(),
        TantivyValue::U64(number) => match number {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        },
        TantivyValue::I64(number) => match number {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        },
        TantivyValue::Bool(b) => Some(*b),
        _ => None,
    }
    .map(JsonValue::Bool)
}

fn value_to_ip(value: &TantivyValue) -> Option<JsonValue> {
    match value {
        TantivyValue::Str(s) => s
            .parse()
            .or_else(|_| {
                s.parse::<std::net::Ipv4Addr>()
                    .map(|ip| ip.to_ipv6_mapped())
            })
            .ok(),
        TantivyValue::IpAddr(ip) => Some(*ip),
        _ => None,
    }
    .map(|ip| {
        serde_json::to_value(TantivyValue::IpAddr(ip))
            .expect("Json serialization should never fail.")
    })
}

fn value_to_float(
    value: &TantivyValue,
    numeric_options: &QuickwitNumericOptions,
) -> Option<JsonValue> {
    match value {
        TantivyValue::Str(s) => s.parse().ok(),
        TantivyValue::U64(number) => Some(*number as f64),
        TantivyValue::I64(number) => Some(*number as f64),
        TantivyValue::F64(number) => Some(*number),
        TantivyValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
    .and_then(|f64_val| f64_val.to_json(numeric_options.output_format))
}

fn value_to_u64(
    value: &TantivyValue,
    numeric_options: &QuickwitNumericOptions,
) -> Option<JsonValue> {
    match value {
        TantivyValue::Str(s) => s.parse().ok(),
        TantivyValue::U64(number) => Some(*number),
        TantivyValue::I64(number) => (*number).try_into().ok(),
        TantivyValue::F64(number) => {
            if (0.0..=(u64::MAX as f64)).contains(number) {
                Some(*number as u64)
            } else {
                None
            }
        }
        TantivyValue::Bool(b) => Some(*b as u64),
        _ => None,
    }
    .and_then(|u64_val| u64_val.to_json(numeric_options.output_format))
}

fn value_to_i64(
    value: &TantivyValue,
    numeric_options: &QuickwitNumericOptions,
) -> Option<JsonValue> {
    match value {
        TantivyValue::Str(s) => s.parse().ok(),
        TantivyValue::U64(number) => (*number).try_into().ok(),
        TantivyValue::I64(number) => Some(*number),
        TantivyValue::F64(number) => {
            if ((i64::MIN as f64)..=(i64::MAX as f64)).contains(number) {
                Some(*number as i64)
            } else {
                None
            }
        }
        TantivyValue::Bool(b) => Some(*b as i64),
        _ => None,
    }
    .and_then(|u64_val| u64_val.to_json(numeric_options.output_format))
}

/// Converts Tantivy::Value into Json Value.
///
/// Makes sure the type and value are consistent before converting.
/// For certain LeafType, we use the type options to format the output.
fn value_to_json(value: TantivyValue, leaf_type: &LeafType) -> Option<JsonValue> {
    let res = match (&value, leaf_type) {
        (_, LeafType::Text(_)) => value_to_string(&value),
        (_, LeafType::Bool(_)) => value_to_bool(&value),
        (_, LeafType::IpAddr(_)) => value_to_ip(&value),
        (_, LeafType::F64(numeric_options)) => value_to_float(&value, numeric_options),
        (_, LeafType::U64(numeric_options)) => value_to_u64(&value, numeric_options),
        (_, LeafType::I64(numeric_options)) => value_to_i64(&value, numeric_options),
        (TantivyValue::Object(_), LeafType::Json(_)) => {
            // TODO do we want to allow almost everything here?
            let json_value =
                serde_json::to_value(&value).expect("Json serialization should never fail.");
            Some(json_value)
        }
        (TantivyValue::Bytes(bytes), LeafType::Bytes(bytes_options)) => {
            // TODO we could cast str to bytes
            let json_value = bytes_options.output_format.format_to_json(bytes);
            Some(json_value)
        }
        (_, LeafType::DateTime(date_time_options)) => {
            let date_time = match date_time_options.reparse_tantivy_value(&value) {
                Some(date_time) => date_time,
                None => {
                    warn!("TODO");
                    return None;
                }
            };
            let json_value = date_time_options
                .output_format
                .format_to_json(date_time)
                .expect("Invalid datetime is not allowed.");
            Some(json_value)
        }
        _ => None,
    };
    if res.is_none() {
        quickwit_common::rate_limited_warn!(
            limit_per_min = 2,
            "The value type `{:?}` doesn't match the requested type `{:?}`",
            value,
            leaf_type
        );
    }
    res
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

trait NumVal: Sized + FromStr + ToString + Into<TantivyValue> {
    fn from_json_number(num: &serde_json::Number) -> Option<Self>;

    fn from_json_to_self(json_val: JsonValue, coerce: bool) -> Result<Self, String> {
        match json_val {
            JsonValue::Number(num_val) => Self::from_json_number(&num_val).ok_or_else(|| {
                format!(
                    "expected {}, got inconvertible JSON number `{}`",
                    type_name::<Self>(),
                    num_val
                )
            }),
            JsonValue::String(str_val) => {
                if coerce {
                    str_val.parse::<Self>().map_err(|_| {
                        format!(
                            "failed to coerce JSON string `\"{str_val}\"` to {}",
                            type_name::<Self>()
                        )
                    })
                } else {
                    Err(format!(
                        "expected JSON number, got string `\"{str_val}\"`. enable coercion to {} \
                         with the `coerce` parameter in the field mapping",
                        type_name::<Self>()
                    ))
                }
            }
            _ => {
                let message = if coerce {
                    format!("expected JSON number or string, got `{json_val}`")
                } else {
                    format!("expected JSON number, got `{json_val}`")
                };
                Err(message)
            }
        }
    }

    fn from_json(json_val: JsonValue, coerce: bool) -> Result<TantivyValue, String> {
        Self::from_json_to_self(json_val, coerce).map(Self::into)
    }

    fn to_json(&self, output_format: NumericOutputFormat) -> Option<JsonValue>;
}

impl NumVal for u64 {
    fn from_json_number(num: &serde_json::Number) -> Option<Self> {
        num.as_u64()
    }

    fn to_json(&self, output_format: NumericOutputFormat) -> Option<JsonValue> {
        let json_value = match output_format {
            NumericOutputFormat::String => JsonValue::String(self.to_string()),
            NumericOutputFormat::Number => JsonValue::Number(serde_json::Number::from(*self)),
        };
        Some(json_value)
    }
}

impl NumVal for i64 {
    fn from_json_number(num: &serde_json::Number) -> Option<Self> {
        num.as_i64()
    }

    fn to_json(&self, output_format: NumericOutputFormat) -> Option<JsonValue> {
        let json_value = match output_format {
            NumericOutputFormat::String => JsonValue::String(self.to_string()),
            NumericOutputFormat::Number => JsonValue::Number(serde_json::Number::from(*self)),
        };
        Some(json_value)
    }
}
impl NumVal for f64 {
    fn from_json_number(num: &serde_json::Number) -> Option<Self> {
        num.as_f64()
    }

    fn to_json(&self, output_format: NumericOutputFormat) -> Option<JsonValue> {
        match output_format {
            NumericOutputFormat::String => Some(JsonValue::String(self.to_string())),
            NumericOutputFormat::Number => {
                serde_json::Number::from_f64(*self).map(JsonValue::Number)
            }
        }
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
        let text_options: TextOptions = options.clone().into();
        let field = schema.add_text_field(name, text_options);
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
fn build_field_path_from_str(field_path_as_str: &str) -> Vec<String> {
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

    use super::{value_to_json, JsonValueIterator, LeafType, MapOrArrayIter, MappingLeaf};
    use crate::default_doc_mapper::date_time_type::QuickwitDateTimeOptions;
    use crate::default_doc_mapper::field_mapping_entry::{
        BinaryFormat, NumericOutputFormat, QuickwitBoolOptions, QuickwitBytesOptions,
        QuickwitIpAddrOptions, QuickwitNumericOptions, QuickwitTextOptions,
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
        let err = leaf.value_from_json(json!(u64::max_value())).err().unwrap();
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
            cardinality: Cardinality::MultiValues,
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
            .flat_map(|val| (&val).as_bool())
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
            cardinality: Cardinality::MultiValues,
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
            .flat_map(|val| (&val).as_i64())
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
            cardinality: Cardinality::MultiValues,
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
            cardinality: Cardinality::MultiValues,
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
            cardinality: Cardinality::MultiValues,
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
            cardinality: Cardinality::MultiValues,
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
            .flat_map(|val| (&val).as_bytes())
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
    fn test_tantivy_value_to_json_value_bytes() {
        let bytes_options_base64 = QuickwitBytesOptions::default();
        assert_eq!(
            value_to_json(
                TantivyValue::Bytes(vec![1, 2, 3]),
                &LeafType::Bytes(bytes_options_base64)
            )
            .unwrap(),
            serde_json::json!("AQID")
        );

        let bytes_options_hex = QuickwitBytesOptions {
            output_format: BinaryFormat::Hex,
            ..Default::default()
        };
        assert_eq!(
            value_to_json(
                TantivyValue::Bytes(vec![1, 2, 3]),
                &LeafType::Bytes(bytes_options_hex)
            )
            .unwrap(),
            serde_json::json!("010203")
        );
    }

    #[test]
    fn test_tantivy_value_to_json_value_f64() {
        let numeric_options_number = QuickwitNumericOptions::default();
        assert_eq!(
            value_to_json(
                TantivyValue::F64(0.1),
                &LeafType::F64(numeric_options_number.clone())
            )
            .unwrap(),
            serde_json::json!(0.1)
        );
        assert_eq!(
            value_to_json(
                TantivyValue::U64(1),
                &LeafType::F64(numeric_options_number.clone())
            )
            .unwrap(),
            serde_json::json!(1.0)
        );
        assert_eq!(
            value_to_json(
                TantivyValue::Str("0.1".to_string()),
                &LeafType::F64(numeric_options_number.clone())
            )
            .unwrap(),
            serde_json::json!(0.1)
        );

        let numeric_options_str = QuickwitNumericOptions {
            output_format: NumericOutputFormat::String,
            ..Default::default()
        };
        assert_eq!(
            value_to_json(TantivyValue::F64(0.1), &LeafType::F64(numeric_options_str)).unwrap(),
            serde_json::json!("0.1")
        );
    }

    #[test]
    fn test_tantivy_value_to_json_value_i64() {
        let numeric_options_number = QuickwitNumericOptions::default();
        assert_eq!(
            value_to_json(
                TantivyValue::I64(-1),
                &LeafType::I64(numeric_options_number.clone())
            )
            .unwrap(),
            serde_json::json!(-1)
        );
        assert_eq!(
            value_to_json(TantivyValue::I64(1), &LeafType::I64(numeric_options_number)).unwrap(),
            serde_json::json!(1)
        );

        let numeric_options_str = QuickwitNumericOptions {
            output_format: NumericOutputFormat::String,
            ..Default::default()
        };
        assert_eq!(
            value_to_json(TantivyValue::I64(-1), &LeafType::I64(numeric_options_str)).unwrap(),
            serde_json::json!("-1")
        );
    }

    #[test]
    fn test_tantivy_value_to_json_value_u64() {
        let numeric_options_number = QuickwitNumericOptions::default();
        assert_eq!(
            value_to_json(
                TantivyValue::U64(1),
                &LeafType::U64(numeric_options_number.clone())
            )
            .unwrap(),
            serde_json::json!(1u64)
        );
        assert_eq!(
            value_to_json(TantivyValue::I64(1), &LeafType::U64(numeric_options_number)).unwrap(),
            serde_json::json!(1u64)
        );

        let numeric_options_str = QuickwitNumericOptions {
            output_format: NumericOutputFormat::String,
            ..Default::default()
        };
        assert_eq!(
            value_to_json(TantivyValue::U64(1), &LeafType::U64(numeric_options_str)).unwrap(),
            serde_json::json!("1")
        );
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
        // Those are invalid field paths, but we chekc that it does not panick.
        // Issue #3538 is about validating field paths before trying ot build the path.
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
}
