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

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use quickwit_proto::types::DocMappingUid;
use serde::{Deserialize, Serialize};

use crate::{FieldMappingEntry, QuickwitJsonOptions, TokenizerEntry};

/// Defines how unmapped fields should be handled.
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ModeType {
    /// Lenient mode: ignores unmapped fields.
    Lenient,
    /// Strict mode: returns an error when an unmapped field is encountered.
    Strict,
    /// Dynamic mode: captures and handles unmapped fields according to the dynamic field
    /// configuration.
    #[default]
    Dynamic,
}

/// Defines how unmapped fields should be handled.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Mode {
    /// Lenient mode: ignores unmapped fields.
    Lenient,
    /// Strict mode: returns an error when an unmapped field is encountered.
    Strict,
    /// Dynamic mode: captures and handles unmapped fields according to the dynamic field
    /// configuration.
    Dynamic(QuickwitJsonOptions),
}

impl Mode {
    /// Extracts the [`ModeType`] of this [`Mode`]
    pub fn mode_type(&self) -> ModeType {
        match self {
            Self::Lenient => ModeType::Lenient,
            Self::Strict => ModeType::Strict,
            Self::Dynamic(_) => ModeType::Dynamic,
        }
    }

    /// Builds a [`Mode`] from its type and optional dynamic mapping options.
    pub fn from_parts(
        mode: ModeType,
        dynamic_mapping: Option<QuickwitJsonOptions>,
    ) -> anyhow::Result<Mode> {
        Ok(match (mode, dynamic_mapping) {
            (ModeType::Lenient, None) => Self::Lenient,
            (ModeType::Strict, None) => Self::Strict,
            (ModeType::Dynamic, Some(dynamic_mapping)) => Self::Dynamic(dynamic_mapping),
            (ModeType::Dynamic, None) => Self::default(), // Dynamic with default options
            (_, Some(_)) => anyhow::bail!(
                "`dynamic_mapping` is only allowed with mode=dynamic. (here mode=`{:?}`)",
                mode
            ),
        })
    }

    /// Obtains the mode type and dynamic options from a [`Mode`].
    pub fn into_parts(self) -> (ModeType, Option<QuickwitJsonOptions>) {
        match self {
            Self::Lenient => (ModeType::Lenient, None),
            Self::Strict => (ModeType::Strict, None),
            Self::Dynamic(json_options) => (ModeType::Dynamic, Some(json_options)),
        }
    }
}

impl Default for Mode {
    fn default() -> Self {
        Self::Dynamic(QuickwitJsonOptions::default_dynamic())
    }
}

/// Defines how the document of an index should be parsed, tokenized, partitioned, indexed, and
/// stored.
#[quickwit_macros::serde_multikey]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DocMapping {
    /// Doc mapping UID.
    ///
    /// Splits with the same doc mapping UID share the same schema and should use the same doc
    /// mapper during indexing and querying.
    #[serde(default = "DocMappingUid::random")]
    pub doc_mapping_uid: DocMappingUid,

    /// Defines how unmapped fields should be handled.
    #[serde_multikey(
        deserializer = Mode::from_parts,
        serializer = Mode::into_parts,
        fields = (
            #[serde(default)]
            mode: ModeType,
            #[serde(skip_serializing_if = "Option::is_none")]
            dynamic_mapping: Option<QuickwitJsonOptions>
        ),
    )]
    pub mode: Mode,

    /// Defines the schema of ingested documents and describes how each field value should be
    /// parsed, tokenized, indexed, and stored.
    #[serde(default)]
    #[schema(value_type = Vec<FieldMappingEntryForSerialization>)]
    pub field_mappings: Vec<FieldMappingEntry>,

    /// Declares the field which contains the date or timestamp at which the document
    /// was emitted.
    #[serde(default)]
    pub timestamp_field: Option<String>,

    /// Declares the low cardinality fields for which the values ​​are recorded directly in the
    /// splits metadata.
    #[schema(value_type = Vec<String>)]
    #[serde(default)]
    pub tag_fields: BTreeSet<String>,

    /// Expresses via a "mini-DSL" how to route documents to split partitions.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,

    /// The maximum number of partitions that an indexer can generate.
    #[schema(value_type = u32)]
    #[serde(default = "DocMapping::default_max_num_partitions")]
    pub max_num_partitions: NonZeroU32,

    /// Whether to record the presence of the fields of each indexed document to allow `exists`
    /// queries.
    #[serde(default)]
    pub index_field_presence: bool,

    /// Whether to record and store the size (bytes) of each ingested document in a fast field.
    #[serde(alias = "document_length")]
    #[serde(default)]
    pub store_document_size: bool,

    /// Whether to store the original source documents in the doc store.
    #[serde(default)]
    pub store_source: bool,

    /// A set of additional user-defined tokenizers to be used during indexing.
    #[serde(default)]
    pub tokenizers: Vec<TokenizerEntry>,
}

impl DocMapping {
    /// Returns the default value for `max_num_partitions`.
    pub fn default_max_num_partitions() -> NonZeroU32 {
        NonZeroU32::new(200).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doc_mapper::{QuickwitNumericOptions, QuickwitTextOptions};
    use crate::{
        Cardinality, FieldMappingType, RegexTokenizerOption, TokenFilterType, TokenizerConfig,
        TokenizerType,
    };

    #[test]
    fn test_doc_mapping_serde_roundtrip() {
        let doc_mapping = DocMapping {
            doc_mapping_uid: DocMappingUid::random(),
            mode: Mode::Strict,
            field_mappings: vec![
                FieldMappingEntry {
                    name: "timestamp".to_string(),
                    mapping_type: FieldMappingType::U64(
                        QuickwitNumericOptions::default(),
                        Cardinality::SingleValued,
                    ),
                },
                FieldMappingEntry {
                    name: "message".to_string(),
                    mapping_type: FieldMappingType::Text(
                        QuickwitTextOptions::default(),
                        Cardinality::SingleValued,
                    ),
                },
            ],
            timestamp_field: Some("timestamp".to_string()),
            tag_fields: BTreeSet::from_iter(["level".to_string()]),
            partition_key: Some("tenant_id".to_string()),
            max_num_partitions: NonZeroU32::new(100).unwrap(),
            index_field_presence: true,
            store_document_size: true,
            store_source: true,
            tokenizers: vec![TokenizerEntry {
                name: "whitespace".to_string(),
                config: TokenizerConfig {
                    tokenizer_type: TokenizerType::Regex(RegexTokenizerOption {
                        pattern: r"\s+".to_string(),
                    }),
                    filters: vec![TokenFilterType::LowerCaser],
                },
            }],
        };
        let serialized = serde_json::to_string(&doc_mapping).unwrap();
        let deserialized: DocMapping = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, doc_mapping);
    }

    #[test]
    fn test_doc_mapping_serde_default_values() {
        let doc_mapping: DocMapping = serde_json::from_str("{}").unwrap();
        assert_eq!(
            doc_mapping.mode,
            Mode::Dynamic(QuickwitJsonOptions::default_dynamic())
        );
        assert!(doc_mapping.field_mappings.is_empty());
        assert_eq!(doc_mapping.timestamp_field, None);
        assert!(doc_mapping.tag_fields.is_empty());
        assert_eq!(doc_mapping.partition_key, None);
        assert_eq!(
            doc_mapping.max_num_partitions,
            NonZeroU32::new(200).unwrap()
        );
        assert_eq!(doc_mapping.index_field_presence, false);
        assert_eq!(doc_mapping.store_document_size, false);
        assert_eq!(doc_mapping.store_source, false);
    }
}
