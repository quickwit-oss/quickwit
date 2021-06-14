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
    schema::{DocParsingError, Schema, SchemaBuilder, STORED},
    Document,
};

/// A mapper that flatten the document to have all fields at top level.
#[derive(Clone, Serialize, Deserialize)]
pub struct AllFlattenDocMapper {
    #[serde(skip_serializing, default = "AllFlattenDocMapper::default_schema")]
    schema: Schema,
}

impl std::fmt::Debug for AllFlattenDocMapper {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "AllFlattenDocMapper")
    }
}

impl Default for AllFlattenDocMapper {
    fn default() -> Self {
        AllFlattenDocMapper {
            schema: SchemaBuilder::new().build(),
        }
    }
}

impl AllFlattenDocMapper {
    /// Creates new instance of all flatten mapper
    pub fn new() -> Self {
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.add_text_field("_source", STORED);
        AllFlattenDocMapper {
            schema: schema_builder.build(),
        }
    }

    fn default_schema() -> Schema {
        SchemaBuilder::new().build()
    }
}

#[typetag::serde(name = "all_flatten")]
impl DocMapper for AllFlattenDocMapper {
    fn doc_from_json(&self, doc_json: &str) -> Result<Document, DocParsingError> {
        let source = self
            .schema
            .get_field("_source")
            .ok_or_else(|| DocParsingError::NoSuchFieldInSchema("_source".to_string()))?;
        let mut document = self.schema.parse_document(doc_json)?;
        document.add_text(source, doc_json);
        Ok(document)
    }

    fn query(&self, _request: SearchRequest) -> Box<dyn Query> {
        todo!()
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}
