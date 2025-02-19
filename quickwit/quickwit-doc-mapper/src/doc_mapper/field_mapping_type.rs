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

use tantivy::schema::Type;

use super::date_time_type::QuickwitDateTimeOptions;
use super::field_mapping_entry::QuickwitBoolOptions;
use crate::doc_mapper::field_mapping_entry::{
    QuickwitBytesOptions, QuickwitConcatenateOptions, QuickwitIpAddrOptions, QuickwitJsonOptions,
    QuickwitNumericOptions, QuickwitObjectOptions, QuickwitTextOptions,
};
use crate::Cardinality;

/// A `FieldMappingType` defines the type and indexing options
/// of a mapping field.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldMappingType {
    /// String mapping type configuration.
    Text(QuickwitTextOptions, Cardinality),
    /// Signed 64-bit integer mapping type configuration.
    I64(QuickwitNumericOptions, Cardinality),
    /// Unsigned 64-bit integer mapping type configuration.
    U64(QuickwitNumericOptions, Cardinality),
    /// DateTime mapping type configuration
    DateTime(QuickwitDateTimeOptions, Cardinality),
    /// 64-bit float mapping type configuration.
    F64(QuickwitNumericOptions, Cardinality),
    /// Bool mapping type configuration.
    Bool(QuickwitBoolOptions, Cardinality),
    /// IP Address mapping type configuration.
    IpAddr(QuickwitIpAddrOptions, Cardinality),
    /// Bytes mapping type configuration.
    Bytes(QuickwitBytesOptions, Cardinality),
    /// Json mapping type configuration.
    Json(QuickwitJsonOptions, Cardinality),
    /// Object mapping type configuration.
    Object(QuickwitObjectOptions),
    /// Concatenate field mapping type configuration.
    Concatenate(QuickwitConcatenateOptions),
}

impl FieldMappingType {
    /// Returns the field mapping type name.
    pub fn quickwit_field_type(&self) -> QuickwitFieldType {
        let (primitive_type, cardinality) = match self {
            FieldMappingType::Text(_, cardinality) => (Type::Str, *cardinality),
            FieldMappingType::I64(_, cardinality) => (Type::I64, *cardinality),
            FieldMappingType::U64(_, cardinality) => (Type::U64, *cardinality),
            FieldMappingType::F64(_, cardinality) => (Type::F64, *cardinality),
            FieldMappingType::Bool(_, cardinality) => (Type::Bool, *cardinality),
            FieldMappingType::IpAddr(_, cardinality) => (Type::IpAddr, *cardinality),
            FieldMappingType::DateTime(_, cardinality) => (Type::Date, *cardinality),
            FieldMappingType::Bytes(_, cardinality) => (Type::Bytes, *cardinality),
            FieldMappingType::Json(_, cardinality) => (Type::Json, *cardinality),
            FieldMappingType::Object(_) => {
                return QuickwitFieldType::Object;
            }
            FieldMappingType::Concatenate(_) => return QuickwitFieldType::Concatenate,
        };
        match cardinality {
            Cardinality::SingleValued => QuickwitFieldType::Simple(primitive_type),
            Cardinality::MultiValued => QuickwitFieldType::Array(primitive_type),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum QuickwitFieldType {
    Simple(Type),
    Object,
    Concatenate,
    Array(Type),
}

impl QuickwitFieldType {
    pub fn to_type_id(&self) -> String {
        match self {
            QuickwitFieldType::Simple(typ) => primitive_type_to_str(typ).to_string(),
            QuickwitFieldType::Object => "object".to_string(),
            QuickwitFieldType::Array(typ) => format!("array<{}>", primitive_type_to_str(typ)),
            QuickwitFieldType::Concatenate => "concatenate".to_string(),
        }
    }

    pub fn parse_type_id(type_str: &str) -> Option<QuickwitFieldType> {
        if type_str == "object" {
            return Some(QuickwitFieldType::Object);
        }
        if type_str == "concatenate" {
            return Some(QuickwitFieldType::Concatenate);
        }
        if type_str.starts_with("array<") && type_str.ends_with('>') {
            let parsed_type_str = parse_primitive_type(&type_str[6..type_str.len() - 1])?;
            return Some(QuickwitFieldType::Array(parsed_type_str));
        }
        let parsed_type_str = parse_primitive_type(type_str)?;
        Some(QuickwitFieldType::Simple(parsed_type_str))
    }
}

fn parse_primitive_type(primitive_type_str: &str) -> Option<Type> {
    match primitive_type_str {
        "text" => Some(Type::Str),
        "u64" => Some(Type::U64),
        "i64" => Some(Type::I64),
        "f64" => Some(Type::F64),
        "bool" => Some(Type::Bool),
        "ip" => Some(Type::IpAddr),
        "datetime" => Some(Type::Date),
        "bytes" => Some(Type::Bytes),
        "json" => Some(Type::Json),
        _unknown_type => None,
    }
}

fn primitive_type_to_str(primitive_type: &Type) -> &'static str {
    match primitive_type {
        Type::Str => "text",
        Type::U64 => "u64",
        Type::I64 => "i64",
        Type::F64 => "f64",
        Type::Bool => "bool",
        Type::IpAddr => "ip",
        Type::Date => "datetime",
        Type::Bytes => "bytes",
        Type::Json => "json",
        Type::Facet => {
            unimplemented!("Facets are not supported by quickwit at the moment.")
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::schema::Type;

    use super::QuickwitFieldType;

    #[track_caller]
    fn test_parse_type_aux(type_str: &str, expected: Option<QuickwitFieldType>) {
        let quickwit_field_type = QuickwitFieldType::parse_type_id(type_str);
        assert_eq!(quickwit_field_type, expected);
    }

    #[test]
    fn test_parse_type() {
        test_parse_type_aux("array<i64>", Some(QuickwitFieldType::Array(Type::I64)));
        test_parse_type_aux("array<text>", Some(QuickwitFieldType::Array(Type::Str)));
        test_parse_type_aux("array<texto>", None);
        test_parse_type_aux("text", Some(QuickwitFieldType::Simple(Type::Str)));
        test_parse_type_aux("object", Some(QuickwitFieldType::Object));
        test_parse_type_aux("object2", None);
        test_parse_type_aux("bool", Some(QuickwitFieldType::Simple(Type::Bool)));
        test_parse_type_aux("ip", Some(QuickwitFieldType::Simple(Type::IpAddr)));
    }
}
