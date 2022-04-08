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

use super::FieldMappingEntry;
use crate::default_doc_mapper::field_mapping_entry::{
    QuickwitFieldType, QuickwitNumericOptions, QuickwitTextOptions,
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
    /// 64-bit float mapping type configuration.
    F64(QuickwitNumericOptions, Cardinality),
    /// RFC 3339 date mapping type configuration.
    Date(QuickwitNumericOptions, Cardinality),
    /// Bytes mapping type configuration.
    Bytes(QuickwitNumericOptions),
    /// Object mapping type configuration.
    Object(Vec<FieldMappingEntry>),
}

impl FieldMappingType {
    pub fn quickwit_field_type(&self) -> QuickwitFieldType {
        let (primitive_type, cardinality) = match self {
            FieldMappingType::Text(_, cardinality) => (Type::Str, *cardinality),
            FieldMappingType::I64(_, cardinality) => (Type::I64, *cardinality),
            FieldMappingType::U64(_, cardinality) => (Type::U64, *cardinality),
            FieldMappingType::F64(_, cardinality) => (Type::F64, *cardinality),
            FieldMappingType::Date(_, cardinality) => (Type::Date, *cardinality),
            FieldMappingType::Bytes(_) => (Type::Bytes, Cardinality::SingleValue),
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
