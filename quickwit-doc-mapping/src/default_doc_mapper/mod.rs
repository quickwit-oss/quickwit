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

mod default_mapper;
mod field_mapping_entry;
mod field_mapping_type;

use tantivy::schema::{Field, Schema};

use crate::mapper::TANTIVY_DOT_SYMBOL;

pub use self::default_mapper::{DefaultDocMapper, DefaultDocMapperBuilder};
pub use self::field_mapping_entry::{DocParsingError, FieldMappingEntry};
pub use self::field_mapping_type::FieldMappingType;

/// Given a field name which can contains some dots, return the schema field
fn resolve_field_name(schema: &Schema, field_name: &str) -> Option<Field> {
    let rw_field_name = field_name.replace(".", TANTIVY_DOT_SYMBOL);
    schema.get_field(&rw_field_name)
}

/// Function used with serde to initialize boolean value at true if there is no value in json.
fn default_as_true() -> bool {
    true
}
