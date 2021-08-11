/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use tantivy::schema::{BytesOptions, Cardinality, IntOptions, TextOptions};

use super::FieldMappingEntry;

/// A `FieldMappingType` defines the type and indexing options
/// of a mapping field.
#[derive(Clone, Debug)]
pub enum FieldMappingType {
    /// String mapping type configuration.
    Text(TextOptions, Cardinality),
    /// Signed 64-bit integer mapping type configuration.
    I64(IntOptions, Cardinality),
    /// Unsigned 64-bit integer mapping type configuration.
    U64(IntOptions, Cardinality),
    /// 64-bit float mapping type configuration.
    F64(IntOptions, Cardinality),
    /// RFC 3339 date mapping type configuration.
    Date(IntOptions, Cardinality),
    /// Bytes mapping type configuration.
    Bytes(BytesOptions, Cardinality),
    /// Object mapping type configuration.
    Object(Vec<FieldMappingEntry>),
}

impl FieldMappingType {
    ///
    pub fn type_with_cardinality(&self) -> String {
        let cardinality = match &self {
            FieldMappingType::I64(_, cardinality)
            | FieldMappingType::U64(_, cardinality)
            | FieldMappingType::Date(_, cardinality)
            | FieldMappingType::F64(_, cardinality) => cardinality,
            FieldMappingType::Text(_, cardinality) => cardinality,
            FieldMappingType::Bytes(_, cardinality) => cardinality,
            FieldMappingType::Object(_) => &Cardinality::SingleValue,
        };
        if cardinality == &Cardinality::MultiValues {
            format!("array<{}>", self.field_type_str())
        } else {
            self.field_type_str().to_string()
        }
    }

    fn field_type_str(&self) -> &str {
        match self {
            FieldMappingType::Text(..) => "text",
            FieldMappingType::I64(..) => "i64",
            FieldMappingType::U64(..) => "u64",
            FieldMappingType::F64(..) => "f64",
            FieldMappingType::Date(..) => "date",
            FieldMappingType::Bytes(..) => "bytes",
            FieldMappingType::Object(..) => "object",
        }
    }
}
