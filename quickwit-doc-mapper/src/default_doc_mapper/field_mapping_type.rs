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

use tantivy::schema::{Cardinality, Type};

use super::date_time_type::QuickwitDateTimeOptions;
use crate::default_doc_mapper::field_mapping_entry::{
    QuickwitJsonOptions, QuickwitNumericOptions, QuickwitObjectOptions, QuickwitTextOptions,
};

/// A `FieldMappingType` defines the type and indexing options
/// of a mapping field.
#[derive(Clone, Debug)]
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
    /// Bytes mapping type configuration.
    Bytes(QuickwitNumericOptions, Cardinality),
    Json(QuickwitJsonOptions, Cardinality),
    /// Object mapping type configuration.
    Object(QuickwitObjectOptions),
}

impl FieldMappingType {
    pub fn quickwit_field_type(&self) -> QuickwitFieldType {
        let (primitive_type, cardinality) = match self {
            FieldMappingType::Text(_, cardinality) => (Type::Str, *cardinality),
            FieldMappingType::I64(_, cardinality) => (Type::I64, *cardinality),
            FieldMappingType::U64(_, cardinality) => (Type::U64, *cardinality),
            FieldMappingType::F64(_, cardinality) => (Type::F64, *cardinality),
            FieldMappingType::DateTime(_, cardinality) => (Type::Date, *cardinality),
            FieldMappingType::Bytes(_, cardinality) => (Type::Bytes, *cardinality),
            FieldMappingType::Json(_, cardinality) => (Type::Json, *cardinality),
            FieldMappingType::Object(_) => {
                return QuickwitFieldType::Object;
            }
        };
        match cardinality {
            Cardinality::SingleValue => QuickwitFieldType::Simple(primitive_type),
            Cardinality::MultiValues => QuickwitFieldType::Array(primitive_type),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum QuickwitFieldType {
    Simple(Type),
    Object,
    Array(Type),
}

impl QuickwitFieldType {
    pub fn to_type_id(&self) -> String {
        match self {
            QuickwitFieldType::Simple(typ) => primitive_type_to_str(typ).to_string(),
            QuickwitFieldType::Object => "object".to_string(),
            QuickwitFieldType::Array(typ) => format!("array<{}>", primitive_type_to_str(typ)),
        }
    }

    pub fn parse_type_id(type_str: &str) -> Option<QuickwitFieldType> {
        if type_str == "object" {
            return Some(QuickwitFieldType::Object);
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
    }
}
