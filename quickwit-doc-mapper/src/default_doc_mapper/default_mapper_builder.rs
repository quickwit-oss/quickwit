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

use anyhow::bail;
use serde::{Deserialize, Serialize};

use super::FieldMappingEntry;
use crate::default_doc_mapper::default_mapper::Mode;
use crate::default_doc_mapper::QuickwitJsonOptions;
use crate::{DefaultDocMapper, SortByConfig};

/// DefaultDocMapperBuilder is here
/// to create a valid DocMapper.
///
/// It is also used to serialize/deserialize a DocMapper.
/// note that this is not the way is the DocMapping is deserialized
/// from the configuration.
#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DefaultDocMapperBuilder {
    /// Stores the original source document when set to true.
    #[serde(default)]
    pub store_source: bool,
    /// Name of the fields that are searched by default, unless overridden.
    #[serde(default)]
    pub default_search_fields: Vec<String>,
    /// Name of the field storing the timestamp of the event for time series data.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_field: Option<String>,
    /// Specifies the name of the sort field and the sort order.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_by: Option<SortByConfig>,
    /// Describes which fields are indexed and how.
    #[serde(default)]
    pub field_mappings: Vec<FieldMappingEntry>,
    /// Name of the fields that are tagged.
    #[serde(default)]
    pub tag_fields: Vec<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Name of the field to demux by.
    pub demux_field: Option<String>,
    /// Defines the indexing mode.
    #[serde(default)]
    pub mode: ModeType,
    /// If mode is set to dynamic, `dynamic_mapping` defines
    /// how the unmapped fields should be handled.
    #[serde(default)]
    pub dynamic_mapping: Option<QuickwitJsonOptions>,
}

/// `Mode` describing how the unmapped field should be handled.
#[derive(Serialize, Deserialize, Copy, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ModeType {
    /// Lenient mode: unmapped fields are just ignored.
    Lenient,
    /// Strict mode: when parsing a document with an unmapped field, an error is yielded.
    Strict,
    /// Dynamic mode: unmapped fields are captured and handled according to the
    /// `dynamic_mapping` configuration.
    Dynamic,
}

impl Default for ModeType {
    fn default() -> Self {
        ModeType::Lenient
    }
}

#[cfg(test)]
impl Default for DefaultDocMapperBuilder {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl DefaultDocMapperBuilder {
    pub(crate) fn mode(&self) -> anyhow::Result<Mode> {
        if self.mode != ModeType::Dynamic && self.dynamic_mapping.is_some() {
            bail!(
                "`dynamic_mapping` is only allowed with mode=dynamic. (Here mode=`{:?}`)",
                self.mode
            );
        }
        Ok(match self.mode {
            ModeType::Lenient => Mode::Lenient,
            ModeType::Strict => Mode::Strict,
            ModeType::Dynamic => Mode::Dynamic(self.dynamic_mapping.clone().unwrap_or_default()),
        })
    }

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
        assert_eq!(default_mapper_builder.mode, ModeType::Lenient);
        assert!(default_mapper_builder.dynamic_mapping.is_none());
        assert!(default_mapper_builder.demux_field.is_none());
        assert!(default_mapper_builder.sort_by.is_none());
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
