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

#![warn(missing_docs)]
#![allow(clippy::bool_assert_comparison)]
#![deny(clippy::disallowed_methods)]

//! Index config defines how to configure an index and especially how
//! to convert a json like documents to a document indexable by tantivy
//! engine, aka tantivy::Document.

mod doc_mapper;
mod doc_mapping;
mod error;
mod query_builder;
mod routing_expression;

/// Pruning tags manipulation.
pub mod tag_pruning;

pub use doc_mapper::{
    Automaton, BinaryFormat, DocMapper, DocMapperBuilder, FastFieldWarmupInfo, FieldMappingEntry,
    FieldMappingType, JsonObject, NamedField, QuickwitBytesOptions, QuickwitJsonOptions, TermRange,
    TokenizerConfig, TokenizerEntry, WarmupInfo, analyze_text,
};
use doc_mapper::{
    FastFieldOptions, FieldMappingEntryForSerialization, IndexRecordOptionSchema,
    NgramTokenizerOption, QuickwitTextNormalizer, QuickwitTextTokenizer, RegexTokenizerOption,
    TokenFilterType, TokenizerType,
};
pub use doc_mapping::{DocMapping, Mode, ModeType};
pub use error::{DocParsingError, QueryParserError};
use quickwit_common::shared_consts::FIELD_PRESENCE_FIELD_NAME;
use quickwit_proto::types::DocMappingUid;
pub use routing_expression::RoutingExpr;

/// Field name reserved for storing the source document.
pub const SOURCE_FIELD_NAME: &str = "_source";

/// Field name reserved for storing the dynamically indexed fields.
pub const DYNAMIC_FIELD_NAME: &str = "_dynamic";

/// Field name reserved for storing the length of source document.
pub const DOCUMENT_SIZE_FIELD_NAME: &str = "_doc_length";

/// Quickwit reserved field names.
const QW_RESERVED_FIELD_NAMES: &[&str] = &[
    DOCUMENT_SIZE_FIELD_NAME,
    DYNAMIC_FIELD_NAME,
    FIELD_PRESENCE_FIELD_NAME,
    SOURCE_FIELD_NAME,
];

/// Cardinality of a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Cardinality {
    /// Single-valued field.
    SingleValued,
    /// Multivalued field.
    MultiValued,
}

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(
    DocMappingUid,
    FastFieldOptions,
    FieldMappingEntryForSerialization,
    IndexRecordOptionSchema,
    ModeType,
    NgramTokenizerOption,
    QuickwitJsonOptions,
    QuickwitTextNormalizer,
    QuickwitTextTokenizer,
    RegexTokenizerOption,
    TokenFilterType,
    TokenizerConfig,
    TokenizerEntry,
    TokenizerType,
)))]
/// Schema used for the OpenAPI generation which are apart of this crate.
pub struct DocMapperApiSchemas;

/// Returns a default `DefaultIndexConfig` for unit tests.
#[cfg(any(test, feature = "testsuite"))]
pub fn default_doc_mapper_for_test() -> DocMapper {
    const JSON_CONFIG_VALUE: &str = r#"
        {
            "store_source": true,
            "index_field_presence": true,
            "default_search_fields": [
                "body", "attributes.server", "attributes.server\\.status"
            ],
            "timestamp_field": "timestamp",
            "tag_fields": ["owner"],
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "datetime",
                    "output_format": "unix_timestamp_secs",
                    "fast": true
                },
                {
                    "name": "body",
                    "type": "text",
                    "stored": true
                },
                {
                    "name": "response_date",
                    "type": "datetime",
                    "input_formats": ["rfc3339", "unix_timestamp"],
                    "fast": true
                },
                {
                    "name": "response_time",
                    "type": "f64",
                    "fast": true
                },
                {
                    "name": "response_payload",
                    "type": "bytes",
                    "fast": true
                },
                {
                    "name": "owner",
                    "type": "text",
                    "tokenizer": "raw"
                },
                {
                    "name": "isImportant",
                    "type": "bool"
                },
                {
                    "name": "properties",
                    "type": "json"
                },
                {
                    "name": "children",
                    "type": "array<json>"
                },
                {
                    "name": "attributes",
                    "type": "object",
                    "field_mappings": [
                        {
                            "name": "tags",
                            "type": "array<i64>"
                        },
                        {
                            "name": "server",
                            "type": "text"
                        },
                        {
                            "name": "server.status",
                            "type": "array<text>"
                        },
                        {
                            "name": "server.payload",
                            "type": "array<bytes>"
                        }
                    ]
                }
            ]
        }"#;
    serde_json::from_str::<DocMapper>(JSON_CONFIG_VALUE).unwrap()
}
