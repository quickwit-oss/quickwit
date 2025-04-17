// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
        assert!(
            default_doc_mapper_builder
                .doc_mapping
                .field_mappings
                .is_empty()
        );
        assert!(
            default_doc_mapper_builder
                .doc_mapping
                .timestamp_field
                .is_none()
        );
        assert!(default_doc_mapper_builder.doc_mapping.tag_fields.is_empty());
        assert_eq!(default_doc_mapper_builder.doc_mapping.store_source, false);
        assert!(default_doc_mapper_builder.default_search_fields.is_empty());
    }

    #[test]
    fn test_default_mapper_builder_extra_field() {
        assert!(serde_json::from_str::<DocMapperBuilder>(r#"{"unknownfield": "blop"}"#).is_err());
    }
}
