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

use quickwit_proto::SearchRequest;
use serde::{Deserialize, Serialize};
use tantivy::query::Query;
use tantivy::schema::{Schema, TextFieldIndexing, TextOptions, STRING};
use tantivy::tokenizer::TokenizerManager;
use tantivy::Document;

use crate::default_index_config::TAGS_FIELD_NAME;
use crate::query_builder::build_query;
use crate::{DocParsingError, IndexConfig, QueryParserError};

/// A document config tailored for the wikipedia corpus.
#[derive(Clone, Serialize, Deserialize)]
pub struct WikipediaIndexConfig {
    #[serde(skip_serializing, default = "WikipediaIndexConfig::default_schema")]
    schema: Schema,
    #[serde(skip_deserializing, skip_serializing, default)]
    tokenizer_manager: TokenizerManager,
}

impl std::fmt::Debug for WikipediaIndexConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "WikipediaIndexConfig")
    }
}

impl WikipediaIndexConfig {
    /// Create a new instance of wikipedia document config.
    pub fn new() -> Self {
        WikipediaIndexConfig {
            schema: Self::default_schema(),
            tokenizer_manager: Default::default(),
        }
    }

    fn default_schema() -> Schema {
        let mut schema_builder = Schema::builder();
        let text_options = TextOptions::default()
            .set_stored()
            .set_indexing_options(TextFieldIndexing::default());
        schema_builder.add_text_field("title", text_options.clone());
        schema_builder.add_text_field("body", text_options.clone());
        schema_builder.add_text_field("url", text_options);
        schema_builder.add_text_field(TAGS_FIELD_NAME, STRING);
        schema_builder.build()
    }
}

impl Default for WikipediaIndexConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[typetag::serde(name = "wikipedia")]
impl IndexConfig for WikipediaIndexConfig {
    fn doc_from_json(&self, doc_json: &str) -> Result<Document, DocParsingError> {
        self.schema
            .parse_document(doc_json)
            .map_err(DocParsingError::from)
    }

    fn query(
        &self,
        split_schema: Schema,
        request: &SearchRequest,
    ) -> Result<Box<dyn Query>, QueryParserError> {
        let default_search_field_names = &["body".to_string(), "title".to_string()];
        build_query(split_schema, request, default_search_field_names)
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}
