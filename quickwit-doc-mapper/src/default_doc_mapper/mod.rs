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

mod default_mapper;
mod field_mapping_entry;
mod field_mapping_type;

use anyhow::bail;
use once_cell::sync::Lazy;
use regex::Regex;

pub use self::default_mapper::{DefaultDocMapper, DefaultDocMapperBuilder, SortByConfig};
pub use self::field_mapping_entry::{DocParsingError, FieldMappingEntry};
pub use self::field_mapping_type::FieldMappingType;

/// Regular expression representing the restriction on a valid field name.
pub const FIELD_MAPPING_NAME_PATTERN: &str = r#"^[_a-zA-Z][_\.\-a-zA-Z0-9]{0,254}$"#;

/// Validator for a potential `field_mapping_name`.
/// Returns true if the name can be use for a field mapping name.
///
/// A field mapping name must start by a letter `[a-zA-Z]`.
/// The other characters can be any alphanumic character `[a-ZA-Z0-9]` or `_`, `.`, `-`.
pub fn validate_field_mapping_name(field_mapping_name: &str) -> anyhow::Result<()> {
    static FIELD_MAPPING_NAME_PTN: Lazy<Regex> =
        Lazy::new(|| Regex::new(FIELD_MAPPING_NAME_PATTERN).unwrap());

    if FIELD_MAPPING_NAME_PTN.is_match(field_mapping_name) {
        return Ok(());
    }
    if field_mapping_name.is_empty() {
        bail!("Field name is empty.");
    }
    if field_mapping_name.len() > 255 {
        bail!(
            "Field name `{}` is too long. Field names must not be longer than 255 characters.",
            field_mapping_name
        )
    }
    let first_char = field_mapping_name.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        bail!(
            "Field name `{}` is invalid. Field names must start with an uppercase or lowercase \
             ASCII letter or an underscore `_`.",
            field_mapping_name
        )
    }
    bail!(
        "Field name `{}` contains illegal characters. Field names must only contain uppercase and \
         lowercase ASCII letters, digits, hyphens `-`, periods `.`, and underscores `_`.",
        field_mapping_name
    );
}

/// Function used with serde to initialize boolean value at true if there is no value in json.
fn default_as_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_field_mapping_name() {
        assert!(validate_field_mapping_name("")
            .unwrap_err()
            .to_string()
            .contains("is empty"));
        assert!(validate_field_mapping_name(&"a".repeat(256))
            .unwrap_err()
            .to_string()
            .contains("is too long"));
        assert!(validate_field_mapping_name("0")
            .unwrap_err()
            .to_string()
            .contains("must start with"));
        assert!(validate_field_mapping_name("_my-field!")
            .unwrap_err()
            .to_string()
            .contains("illegal characters"));
        assert!(validate_field_mapping_name("my-field").is_ok());
        assert!(validate_field_mapping_name("my.field").is_ok());
        assert!(validate_field_mapping_name("my_field").is_ok());
        assert!(validate_field_mapping_name(&"a".repeat(255)).is_ok());
    }
}
