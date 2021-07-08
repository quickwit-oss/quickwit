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

use crate::{
    default_doc_mapper::SOURCE_FIELD_NAME, query_builder::build_query, DocParsingError,
    IndexConfig, QueryParserError,
};
use quickwit_proto::SearchRequest;
use serde::{Deserialize, Serialize};
use tantivy::{
    query::Query,
    schema::{Schema, SchemaBuilder, STORED},
    Document,
};

/// A mapper that flatten the document to have all fields at top level.
#[derive(Clone, Serialize, Deserialize)]
pub struct AllFlattenIndexConfig {
    #[serde(skip_serializing, default = "AllFlattenIndexConfig::default_schema")]
    schema: Schema,
}

impl std::fmt::Debug for AllFlattenIndexConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "AllFlattenDocMapper")
    }
}

impl Default for AllFlattenIndexConfig {
    fn default() -> Self {
        AllFlattenIndexConfig::new()
    }
}

impl AllFlattenIndexConfig {
    /// Creates new instance of all flatten mapper
    pub fn new() -> Self {
        AllFlattenIndexConfig {
            schema: AllFlattenIndexConfig::default_schema(),
        }
    }

    fn default_schema() -> Schema {
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.add_text_field(SOURCE_FIELD_NAME, STORED);
        schema_builder.build()
    }
}

#[typetag::serde(name = "all_flatten")]
impl IndexConfig for AllFlattenIndexConfig {
    fn doc_from_json(&self, doc_json: &str) -> Result<Document, DocParsingError> {
        let source = self
            .schema
            .get_field(SOURCE_FIELD_NAME)
            .ok_or_else(|| DocParsingError::NoSuchFieldInSchema(SOURCE_FIELD_NAME.to_string()))?;
        let mut document = self.schema.parse_document(doc_json)?;
        document.add_text(source, doc_json);
        Ok(document)
    }

    fn query(&self, request: &SearchRequest) -> Result<Box<dyn Query>, QueryParserError> {
        let default_search_field_names = vec![SOURCE_FIELD_NAME.to_string()];
        build_query(self.schema(), request, &default_search_field_names)
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}
