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
    analyze_text, BinaryFormat, DocMapper, DocMapperBuilder, FastFieldWarmupInfo,
    FieldMappingEntry, FieldMappingType, JsonObject, NamedField, QuickwitBytesOptions,
    QuickwitJsonOptions, TermRange, TokenizerConfig, TokenizerEntry, WarmupInfo,
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
