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

use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};

use crate::{DocMapper, DocMapping};

/// DocMapperBuilder is here
/// to create a valid DocMapper.
///
/// It is also used to serialize/deserialize a DocMapper.
/// note that this is not the way is the DocMapping is deserialized
/// from the configuration.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocMapperBuilder {
    /// Doc mapping.
    #[serde(flatten)]
    pub doc_mapping: DocMapping,
    /// Default search field names.
    #[serde(default)]
    pub default_search_fields: Vec<String>,

    /// Allow the "type" field separately.
    /// This is a residue from when the DocMapper was a trait.
    #[serde(rename = "type", default)]
    #[serde(skip_serializing)]
    pub legacy_type_tag: Option<IgnoredAny>,
}

#[cfg(test)]
impl Default for DocMapperBuilder {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl DocMapperBuilder {
    /// Build a valid `DocMapper`.
    /// This will consume your `DocMapperBuilder`.
    pub fn try_build(self) -> anyhow::Result<DocMapper> {
        self.try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ModeType;

    #[test]
    fn test_default_mapper_builder_deserialize_from_empty_object() {
        let default_doc_mapper_builder: DocMapperBuilder = serde_json::from_str("{}").unwrap();
        assert_eq!(
            default_doc_mapper_builder.doc_mapping.mode.mode_type(),
            ModeType::Dynamic
        );
        assert!(default_doc_mapper_builder
            .doc_mapping
            .field_mappings
            .is_empty());
        assert!(default_doc_mapper_builder
            .doc_mapping
            .timestamp_field
            .is_none());
        assert!(default_doc_mapper_builder.doc_mapping.tag_fields.is_empty());
        assert_eq!(default_doc_mapper_builder.doc_mapping.store_source, false);
        assert!(default_doc_mapper_builder.default_search_fields.is_empty());
    }

    #[test]
    fn test_default_mapper_builder_extra_field() {
        assert!(serde_json::from_str::<DocMapperBuilder>(r#"{"unknownfield": "blop"}"#).is_err());
    }
}
