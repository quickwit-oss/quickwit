/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use crate::{mapper::SearchRequest, DocMapper};
use serde::{Deserialize, Serialize};
use tantivy::{
    query::Query,
    schema::{DocParsingError, Schema, SchemaBuilder, TextFieldIndexing, TextOptions},
    Document,
};

/// A document mapper tailored for the wikipedia corpus.
#[derive(Clone, Serialize, Deserialize)]
pub struct WikipediaMapper {
    #[serde(skip_serializing, default = "WikipediaMapper::default_schema")]
    schema: Schema,
}

impl std::fmt::Debug for WikipediaMapper {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "WikipediaMapper")
    }
}

impl WikipediaMapper {
    /// Create a new instance of wikipedia document mapper.
    pub fn new() -> anyhow::Result<Self> {
        let mut schema_builder = Schema::builder();
        let text_options = TextOptions::default()
            .set_stored()
            .set_indexing_options(TextFieldIndexing::default());
        schema_builder.add_text_field("title", text_options.clone());
        schema_builder.add_text_field("body", text_options.clone());
        schema_builder.add_text_field("url", text_options);
        Ok(WikipediaMapper {
            schema: schema_builder.build(),
        })
    }

    fn default_schema() -> Schema {
        SchemaBuilder::new().build()
    }
}

#[typetag::serde(name = "wikipedia")]
impl DocMapper for WikipediaMapper {
    fn doc_from_json(&self, doc_json: &str) -> Result<Document, DocParsingError> {
        self.schema.parse_document(doc_json)
    }

    fn query(&self, _request: SearchRequest) -> Box<dyn Query> {
        todo!()
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}
