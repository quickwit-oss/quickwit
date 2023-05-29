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

use anyhow::bail;
use serde_json::Value as JsonValue;
use tantivy::schema::Type;

use super::date_time_type::QuickwitDateTimeOptions;
use crate::default_doc_mapper::field_mapping_entry::{
    QuickwitIpAddrOptions, QuickwitJsonOptions, QuickwitNumericOptions, QuickwitObjectOptions,
    QuickwitTextOptions,
};
use crate::Cardinality;

/// A `FieldMappingType` defines the type and indexing options
/// of a mapping field.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FieldMappingType {
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
    Bool(QuickwitNumericOptions, Cardinality),
    /// IP Address mapping type configuration.
    IpAddr(QuickwitIpAddrOptions, Cardinality),
    /// Bytes mapping type configuration.
    Bytes(QuickwitNumericOptions, Cardinality),
    /// Json mapping type configuration.
    Json(QuickwitJsonOptions, Cardinality),
    /// Object mapping type configuration.
    Object(QuickwitObjectOptions, Cardinality),
}

impl FieldMappingType {
    pub fn type_id(&self) -> String {
        self.quickwit_field_type().type_id()
    }

    fn quickwit_field_type(&self) -> QuickwitFieldType {
        let (object_or_type, cardinality) = match self {
            FieldMappingType::Text(_, cardinality) => (Type::Str.into(), *cardinality),
            FieldMappingType::I64(_, cardinality) => (Type::I64.into(), *cardinality),
            FieldMappingType::U64(_, cardinality) => (Type::U64.into(), *cardinality),
            FieldMappingType::F64(_, cardinality) => (Type::F64.into(), *cardinality),
            FieldMappingType::Bool(_, cardinality) => (Type::Bool.into(), *cardinality),
            FieldMappingType::IpAddr(_, cardinality) => (Type::IpAddr.into(), *cardinality),
            FieldMappingType::DateTime(_, cardinality) => (Type::Date.into(), *cardinality),
            FieldMappingType::Bytes(_, cardinality) => (Type::Bytes.into(), *cardinality),
            FieldMappingType::Json(_, cardinality) => (Type::Json.into(), *cardinality),
            FieldMappingType::Object(_, cardinality) => (ObjectOrType::Object, *cardinality),
        };
        QuickwitFieldType {
            cardinality,
            object_or_type,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum ObjectOrType {
    Object,
    TantivyType(Type),
}

impl From<Type> for ObjectOrType {
    fn from(typ: Type) -> Self {
        ObjectOrType::TantivyType(typ)
    }
}

impl ObjectOrType {
    fn from_id(typ: &str) -> Option<ObjectOrType> {
        match typ {
            "text" => Some(Type::Str.into()),
            "u64" => Some(Type::U64.into()),
            "i64" => Some(Type::I64.into()),
            "f64" => Some(Type::F64.into()),
            "bool" => Some(Type::Bool.into()),
            "ip" => Some(Type::IpAddr.into()),
            "datetime" => Some(Type::Date.into()),
            "bytes" => Some(Type::Bytes.into()),
            "json" => Some(Type::Json.into()),
            "object" => Some(ObjectOrType::Object),
            _unknown_type => None,
        }
    }

    fn to_id(&self) -> &'static str {
        let typ = match self {
            ObjectOrType::Object => return "object",
            ObjectOrType::TantivyType(typ) => typ,
        };
        match typ {
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
}

/// Helper type only used for serialization.
#[derive(Debug, Eq, PartialEq)]
struct QuickwitFieldType {
    cardinality: Cardinality,
    object_or_type: ObjectOrType,
}

impl QuickwitFieldType {
    fn type_id(&self) -> String {
        match self.cardinality {
            Cardinality::SingleValue => self.object_or_type.to_id().to_string(),
            Cardinality::MultiValues => {
                format!("array<{}>", self.object_or_type.to_id())
            }
        }
    }

    fn parse_from_type_id(typ_card_str: &str) -> Option<QuickwitFieldType> {
        let (cardinality, type_str) =
            if typ_card_str.starts_with("array<") && typ_card_str.ends_with('>') {
                (
                    Cardinality::MultiValues,
                    &typ_card_str[6..typ_card_str.len() - 1],
                )
            } else {
                (Cardinality::SingleValue, typ_card_str)
            };
        let object_or_type = ObjectOrType::from_id(type_str)?;
        Some(QuickwitFieldType {
            cardinality,
            object_or_type,
        })
    }
}

fn deserialize_mapping_type(
    quickwit_field_type: QuickwitFieldType,
    json: JsonValue,
) -> anyhow::Result<FieldMappingType> {
    let cardinality = quickwit_field_type.cardinality;
    match quickwit_field_type.object_or_type {
        ObjectOrType::Object => {
            let object_options: QuickwitObjectOptions = serde_json::from_value(json)?;
            if object_options.field_mappings.is_empty() {
                anyhow::bail!("object type must have at least one field mapping.");
            }
            Ok(FieldMappingType::Object(object_options, cardinality))
        }
        ObjectOrType::TantivyType(Type::Bool) => {
            let numeric_options: QuickwitNumericOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::Bool(numeric_options, cardinality))
        }
        ObjectOrType::TantivyType(Type::Str) => {
            let text_options: QuickwitTextOptions = serde_json::from_value(json)?;
            #[allow(clippy::collapsible_if)]
            if !text_options.indexed {
                if text_options.tokenizer.is_some()
                    || text_options.record.is_some()
                    || text_options.fieldnorms
                {
                    bail!(
                        "`record`, `tokenizer`, and `fieldnorms` parameters are allowed only if \
                         indexed is true."
                    );
                }
            }
            Ok(FieldMappingType::Text(text_options, cardinality))
        }
        ObjectOrType::TantivyType(Type::U64) => {
            let numeric_options: QuickwitNumericOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::U64(numeric_options, cardinality))
        }
        ObjectOrType::TantivyType(Type::I64) => {
            let numeric_options: QuickwitNumericOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::I64(numeric_options, cardinality))
        }
        ObjectOrType::TantivyType(Type::F64) => {
            let numeric_options: QuickwitNumericOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::F64(numeric_options, cardinality))
        }
        ObjectOrType::TantivyType(Type::IpAddr) => {
            let ip_addr_options: QuickwitIpAddrOptions = serde_json::from_value(json)?;
            Ok(FieldMappingType::IpAddr(ip_addr_options, cardinality))
        }
        ObjectOrType::TantivyType(Type::Date) => {
            let date_time_options = serde_json::from_value::<QuickwitDateTimeOptions>(json)?;
            Ok(FieldMappingType::DateTime(date_time_options, cardinality))
        }
        ObjectOrType::TantivyType(Type::Facet) => {
            unimplemented!("Facet are not supported in quickwit yet.")
        }
        ObjectOrType::TantivyType(Type::Bytes) => {
            let numeric_options: QuickwitNumericOptions = serde_json::from_value(json)?;
            if numeric_options.fast && cardinality == Cardinality::MultiValues {
                bail!("fast field is not allowed for array<bytes>.");
            }
            Ok(FieldMappingType::Bytes(numeric_options, cardinality))
        }
        ObjectOrType::TantivyType(Type::Json) => {
            let json_options: QuickwitJsonOptions = serde_json::from_value(json)?;
            #[allow(clippy::collapsible_if)]
            if !json_options.indexed {
                if json_options.tokenizer.is_some() || json_options.record.is_some() {
                    bail!(
                        "`record` and `tokenizer` parameters are allowed only if indexed is true."
                    );
                }
            }
            Ok(FieldMappingType::Json(json_options, cardinality))
        }
    }
}

pub(crate) fn parse_field_mapping_type(
    field_name: &str,
    type_id: &str,
    params: serde_json::Map<String, JsonValue>,
) -> Result<FieldMappingType, String> {
    let quickwit_field_type = QuickwitFieldType::parse_from_type_id(&type_id)
        .ok_or_else(|| format!("Field `{field_name}` has an unknown type: `{type_id}`."))?;
    deserialize_mapping_type(quickwit_field_type, JsonValue::Object(params))
        .map_err(|err| format!("Error while parsing field `{field_name}`: {err}"))
}

#[cfg(test)]
mod tests {
    use tantivy::schema::Type;

    use super::QuickwitFieldType;
    use crate::default_doc_mapper::field_mapping_type::ObjectOrType;
    use crate::Cardinality;

    #[track_caller]
    fn test_parse_type_aux(typ: QuickwitFieldType) {
        let deser_ser_typ = QuickwitFieldType::parse_from_type_id(&typ.type_id()).unwrap();
        assert_eq!(deser_ser_typ, typ);
    }

    #[test]
    fn test_type_serialization() {
        for object_or_type in [
            Type::Str.into(),
            Type::U64.into(),
            Type::I64.into(),
            Type::F64.into(),
            Type::Bool.into(),
            Type::IpAddr.into(),
            Type::Date.into(),
            Type::Bytes.into(),
            Type::Json.into(),
            ObjectOrType::Object,
        ] {
            for &cardinality in &[Cardinality::SingleValue, Cardinality::MultiValues] {
                let quickwit_field_type = QuickwitFieldType {
                    cardinality,
                    object_or_type,
                };
                test_parse_type_aux(quickwit_field_type);
            }
        }
    }

    #[test]
    fn test_type_serialize_failures() {
        assert!(QuickwitFieldType::parse_from_type_id("array<i64").is_none());
        assert!(QuickwitFieldType::parse_from_type_id("array<i64!").is_none());
        assert!(QuickwitFieldType::parse_from_type_id("array<d64>").is_none());
        assert!(QuickwitFieldType::parse_from_type_id("objet").is_none());
    }
}
