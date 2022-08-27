// Copyright (C) 2022 Quickwit, Inc.
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

//! Index config defines how to configure an index and especially how
//! to convert a json like documents to a document indexable by tantivy
//! engine, aka tantivy::Document.

mod default_doc_mapper;
mod doc_mapper;
mod error;
mod query_builder;
mod routing_expression;
mod sort_by;
mod tokenizers;

/// Pruning tags manipulation.
pub mod tag_pruning;

pub use default_doc_mapper::{
    DefaultDocMapper, DefaultDocMapperBuilder, FieldMappingEntry, ModeType, QuickwitJsonOptions,
    SortByConfig,
};
pub use doc_mapper::DocMapper;
pub use error::{DocParsingError, QueryParserError};
pub use sort_by::{SortBy, SortByField, SortOrder};
pub use tokenizers::QUICKWIT_TOKENIZER_MANAGER;

/// Field name reserved for storing the source document.
pub const SOURCE_FIELD_NAME: &str = "_source";

/// Field name reserved for storing the dynamically indexed fields.
pub const DYNAMIC_FIELD_NAME: &str = "_dynamic";

/// Returns a default `DefaultIndexConfig` for unit tests.
#[cfg(any(test, feature = "testsuite"))]
pub fn default_doc_mapper_for_test() -> DefaultDocMapper {
    const JSON_CONFIG_VALUE: &str = r#"
        {
            "store_source": true,
            "default_search_fields": [
                "body", "attributes.server", "attributes.server\\.status"
            ],
            "timestamp_field": "timestamp",
            "sort_by": {
                "field_name": "timestamp",
                "order": "desc"
            },
            "tag_fields": ["owner"],
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "i64",
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
    serde_json::from_str::<DefaultDocMapper>(JSON_CONFIG_VALUE).unwrap()
}

/// Returns a default `DefaultIndexConfig` for unit tests.
#[cfg(any(test, feature = "testsuite"))]
pub fn default_config_with_demux_for_tests() -> DefaultDocMapper {
    const JSON_CONFIG_VALUE: &str = r#"
        {
            "store_source": true,
            "default_search_fields": [
                "body", "tenant_id"
            ],
            "timestamp_field": "timestamp",
            "sort_by": {
                "field_name": "timestamp",
                "order": "desc"
            },
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "i64",
                    "fast": true
                },
                {
                    "name": "body",
                    "type": "text",
                    "stored": true
                },
                {
                    "name": "tenant_id",
                    "type": "u64",
                    "fast": true
                }
            ]
        }"#;
    serde_json::from_str::<DefaultDocMapper>(JSON_CONFIG_VALUE).unwrap()
}
