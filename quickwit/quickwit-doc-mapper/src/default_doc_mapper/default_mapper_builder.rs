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

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use super::tokenizer_entry::TokenizerEntry;
use super::FieldMappingEntry;
use crate::default_doc_mapper::QuickwitJsonOptions;
use crate::DefaultDocMapper;

/// DefaultDocMapperBuilder is here
/// to create a valid DocMapper.
///
/// It is also used to serialize/deserialize a DocMapper.
/// note that this is not the way is the DocMapping is deserialized
/// from the configuration.
#[quickwit_macros::serde_multikey]
#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DefaultDocMapperBuilder {
    /// Stores the original source document when set to true.
    #[serde(default)]
    pub store_source: bool,
    /// Indexes field presence.
    #[serde(default)]
    pub index_field_presence: bool,
    /// Name of the fields that are searched by default, unless overridden.
    #[serde(default)]
    pub default_search_fields: Vec<String>,
    /// Name of the field storing the timestamp of the event for time series data.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_field: Option<String>,
    /// Describes which fields are indexed and how.
    #[serde(default)]
    pub field_mappings: Vec<FieldMappingEntry>,
    /// Name of the fields that are tagged.
    #[serde(default)]
    pub tag_fields: Vec<String>,
    /// The partition key is a DSL used to route documents
    /// into specific splits.
    #[serde(default)]
    pub partition_key: Option<String>,
    /// Maximum number of partitions.
    #[serde(default = "DefaultDocMapper::default_max_num_partitions")]
    pub max_num_partitions: NonZeroU32,
    #[serde_multikey(
        deserializer = Mode::from_parts,
        serializer = Mode::into_parts,
        fields = (
            /// Defines the indexing mode.
            #[serde(default)]
            mode: ModeType,
            /// If mode is set to dynamic, `dynamic_mapping` defines
            /// how the unmapped fields should be handled.
            #[serde(default)]
            dynamic_mapping: Option<QuickwitJsonOptions>,
        ),
    )]
    /// Defines how the unmapped fields should be handled.
    pub mode: Mode,
    /// User-defined tokenizers.
    #[serde(default)]
    pub tokenizers: Vec<TokenizerEntry>,
}

/// Defines how an unmapped field should be handled.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Mode {
    /// Lenient mode: unmapped fields are just ignored.
    Lenient,
    /// Strict mode: when parsing a document with an unmapped field, an error is yielded.
    Strict,
    /// Dynamic mode: unmapped fields are captured and handled according to the provided
    /// configuration.
    Dynamic(QuickwitJsonOptions),
}

impl Mode {
    /// Extact the `ModeType` of this `Mode`
    pub fn mode_type(&self) -> ModeType {
        match self {
            Mode::Lenient => ModeType::Lenient,
            Mode::Strict => ModeType::Strict,
            Mode::Dynamic(_) => ModeType::Dynamic,
        }
    }

    /// Build a Mode from its type and optional dynamic mapping options
    pub fn from_parts(
        mode: ModeType,
        dynamic_mapping: Option<QuickwitJsonOptions>,
    ) -> anyhow::Result<Mode> {
        Ok(match (mode, dynamic_mapping) {
            (ModeType::Lenient, None) => Mode::Lenient,
            (ModeType::Strict, None) => Mode::Strict,
            (ModeType::Dynamic, Some(dynamic_mapping)) => Mode::Dynamic(dynamic_mapping),
            (ModeType::Dynamic, None) => Mode::default(), // Dynamic with default options
            (_, Some(_)) => anyhow::bail!(
                "`dynamic_mapping` is only allowed with mode=dynamic. (here mode=`{:?}`)",
                mode
            ),
        })
    }

    /// Obtain the mode type and dynamic options from a Mode
    pub fn into_parts(self) -> (ModeType, Option<QuickwitJsonOptions>) {
        match self {
            Mode::Lenient => (ModeType::Lenient, None),
            Mode::Strict => (ModeType::Strict, None),
            Mode::Dynamic(json_options) => (ModeType::Dynamic, Some(json_options)),
        }
    }
}

impl Default for Mode {
    fn default() -> Self {
        Mode::Dynamic(QuickwitJsonOptions::default_dynamic())
    }
}

/// `Mode` describing how the unmapped field should be handled.
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ModeType {
    /// Lenient mode: unmapped fields are just ignored.
    Lenient,
    /// Strict mode: when parsing a document with an unmapped field, an error is yielded.
    Strict,
    /// Dynamic mode: unmapped fields are captured and handled according to the
    /// `dynamic_mapping` configuration.
    #[default]
    Dynamic,
}

#[cfg(test)]
impl Default for DefaultDocMapperBuilder {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl DefaultDocMapperBuilder {
    /// Build a valid `DefaultDocMapper`.
    /// This will consume your `DefaultDocMapperBuilder`.
    pub fn try_build(self) -> anyhow::Result<DefaultDocMapper> {
        self.try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_mapper_builder_deserialize_from_empty_object() {
        let default_mapper_builder: DefaultDocMapperBuilder =
            serde_json::from_str::<DefaultDocMapperBuilder>("{}").unwrap();
        assert!(default_mapper_builder.default_search_fields.is_empty());
        assert!(default_mapper_builder.field_mappings.is_empty());
        assert!(default_mapper_builder.tag_fields.is_empty());
        assert_eq!(default_mapper_builder.mode.mode_type(), ModeType::Dynamic);
        assert_eq!(default_mapper_builder.store_source, false);
        assert!(default_mapper_builder.timestamp_field.is_none());
    }

    #[test]
    fn test_default_mapper_builder_extra_field() {
        assert!(
            serde_json::from_str::<DefaultDocMapperBuilder>(r#"{"unknownfield": "blop"}"#).is_err()
        );
    }
}
