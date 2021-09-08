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

mod default_config;
mod field_mapping_entry;
mod field_mapping_type;

use once_cell::sync::Lazy;
use regex::Regex;

pub use self::default_config::{DefaultIndexConfig, DefaultIndexConfigBuilder};
pub use self::field_mapping_entry::{DocParsingError, FieldMappingEntry};
pub use self::field_mapping_type::FieldMappingType;

/// Field name reserved for storing the source document.
pub static SOURCE_FIELD_NAME: &str = "_source";

/// Field name reserved for storing the tags.
pub static TAGS_FIELD_NAME: &str = "_tags";

/// Regular expression representing the restriction on a valid field name.
pub const FIELD_MAPPING_NAME_PATTERN: &str = r#"^[_a-zA-Z][_\.\-a-zA-Z0-9]*$"#;

/// Validator for a potential `field_mapping_name`.
/// Returns true if the name can be use for a field mapping name.
///
/// A field mapping name must start by a letter `[a-zA-Z]`.
/// The other characters can be any alphanumic character `[a-ZA-Z0-9]` or `_`, `.`, `-`.
pub fn is_valid_field_mapping_name(field_mapping_name: &str) -> bool {
    static FIELD_MAPPING_NAME_PTN: Lazy<Regex> =
        Lazy::new(|| Regex::new(FIELD_MAPPING_NAME_PATTERN).unwrap());
    FIELD_MAPPING_NAME_PTN.is_match(field_mapping_name)
}

/// Function used with serde to initialize boolean value at true if there is no value in json.
fn default_as_true() -> bool {
    true
}
