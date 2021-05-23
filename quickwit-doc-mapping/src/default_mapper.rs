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

use serde::{Deserialize, Serialize};
use tantivy::{Document, query::Query, schema::{DocParsingError, FieldEntry, STORED, Schema, SchemaBuilder}};
use crate::{DocMapper, mapper::SearchRequest};

pub struct DefaultDocMapper {
    schema: Schema, // transient
    config: DocMapperConfig,
}

impl DefaultDocMapper {
    pub fn new(config: DocMapperConfig) -> anyhow::Result<DefaultDocMapper> {
        Ok(DefaultDocMapper {schema: config.schema(), config})
    }
}

impl DocMapper for DefaultDocMapper {
    fn doc_from_json(&self, doc_json: &str) -> Result<Document, DocParsingError> {
        // TODO: this is an oversimplied approach.
        // We also want to handle nested json objects such as
        // { "resources": {"label": "T_T"} }
        // KISS: just put the value in the field with name "resources.label"
        let mut document = self.schema.parse_document(doc_json)?;
        if self.config.store_source {
            let source = self.schema.get_field("_source")
            .ok_or_else(|| DocParsingError::NoSuchFieldInSchema("_source".to_string()))?;
            document.add_text(source, doc_json);
        }
        Ok(document)
    }

    fn query(&self, _request: SearchRequest) -> Box<dyn Query> {
        todo!()
    }

    fn schema(&self) -> Schema {
        self.config.schema()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DocMapperConfig {
    store_source: bool,
    properties: Vec<FieldEntry>,
}

impl DocMapperConfig {
    pub fn schema(&self) -> Schema {
        let mut builder = SchemaBuilder::new();
        self.properties.iter().for_each(|entry| {
            builder.add_field(entry.clone());
        });
        if self.store_source {
            builder.add_text_field("_source", STORED);
        }
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::{DocMapper, DocMapperConfig, DefaultDocMapper};
    use std::str;

    const JSON_DOC_VALUE: &str = r#"
        {
            "timestamp": 1586960586000,
            "severity_text": "INFO",
            "resources.label": "20200415T072306-0700 INFO This is a great log"
        }"#;

    const JSON_MAPPING_VALUE: &str = r#"
        {
            "store_source": true,
            "sharding_timestamp_field_name": "timestamp",
            "properties": [{
                "name": "timestamp",
                "type": "u64",
                "options": {
                    "indexed": true,
                    "fast": "single",
                    "stored": false
                  }
            }, {
                "name": "severity_text",
                "type": "text",
                "options": {
                    "indexing": {
                      "record": "basic",
                      "tokenizer": "raw"
                    },
                    "stored": false
                }
            }, {
                "name": "resources.label",
                "type": "text",
                "options": {
                    "indexing": {
                      "record": "position",
                      "tokenizer": "default"
                    },
                    "stored": false
                }
            }]
        }"#;


    #[test]
    fn test_deserialize_mapping() -> anyhow::Result<()> {
        serde_json::from_str::<DocMapperConfig>(JSON_MAPPING_VALUE)?;
        Ok(())
    }

    #[test]
    fn test_parsing_document() -> anyhow::Result<()> {
        let config = serde_json::from_str::<DocMapperConfig>(JSON_MAPPING_VALUE)?;
        let doc_mapper = DefaultDocMapper::new(config)?;
        let document = doc_mapper.doc_from_json(JSON_DOC_VALUE)?;
        assert_eq!(document.len(), 4);
        let source = document.field_values()[3].value().text().unwrap();
        assert_eq!(source, JSON_DOC_VALUE);
        Ok(())
    }
}

