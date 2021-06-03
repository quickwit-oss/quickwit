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

use dyn_clone::clone_trait_object;
use dyn_clone::DynClone;
use std::fmt::Debug;

use crate::{
    all_flatten_mapper::AllFlattenDocMapper,
    default_mapper::{DefaultDocMapper, DocMapperConfig},
    wikipedia_mapper::WikipediaMapper,
};
use tantivy::{
    query::Query,
    schema::{DocParsingError, Schema},
    Document,
};

/// The `DocMapper` trait defines the way of defining how a (json) document,
/// and the fields it contains, are stored and indexed.
///
/// The `DocMapper` trait is in charge of implementing :
///
/// - a way to build a tantivy::Document from a json payload
/// - a way to build a tantivy::Query from a SearchRequest
/// - a way to build a tantivy:Schema
///
#[typetag::serde(tag = "type")]
pub trait DocMapper: Send + Sync + Debug + DynClone + 'static {
    /// Returns the document built from a json string.
    fn doc_from_json(&self, doc_json: &str) -> Result<Document, DocParsingError>;
    /// Returns the schema.
    fn schema(&self) -> Schema;
    /// Returns the query.
    fn query(&self, _request: SearchRequest) -> Box<dyn Query>;
}

clone_trait_object!(DocMapper);

// TODO: this is a placeholder, to be removed when it will be implementend in the search-api crate
pub struct SearchRequest {}

/// Build a doc mapper given the doc mapper type.
pub fn build_doc_mapper(
    mapper_type: &str,
    _mapper_config: Option<&str>,
) -> anyhow::Result<Box<dyn DocMapper>> {
    match mapper_type.trim().to_lowercase().as_str() {
        "default" => {
            // TODO: build config from `mapper_config` json string
            let config = DocMapperConfig::default();
            DefaultDocMapper::new(config).map(|mapper| Box::new(mapper) as Box<dyn DocMapper>)
        }
        "all_flatten" => {
            AllFlattenDocMapper::new().map(|mapper| Box::new(mapper) as Box<dyn DocMapper>)
        }
        "wikipedia" => WikipediaMapper::new().map(|mapper| Box::new(mapper) as Box<dyn DocMapper>),
        _ => Err(anyhow::anyhow!(format!(
            "Could not parse `{}` as valid doc mapper type.",
            mapper_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        default_mapper::{DefaultDocMapper, DocMapperConfig},
        DocMapper,
    };

    const JSON_ALL_FLATTEN_DOC_MAPPER: &str = r#"
        {
            "type": "all_flatten", "attributes": {}
        }"#;

    const JSON_DEFAULT_DOC_MAPPER: &str = r#"
        {
            "type": "default",
            "attributes": {
                "config": {
                    "store_source": true,
                    "ignore_unknown_fields": false,
                    "properties": []
                }
            }
        }"#;

    #[test]
    fn test_deserialize_doc_mapper() -> anyhow::Result<()> {
        let all_flatten_mapper =
            serde_json::from_str::<Box<dyn DocMapper>>(JSON_ALL_FLATTEN_DOC_MAPPER)?;
        let deserialized_default_mapper =
            serde_json::from_str::<Box<dyn DocMapper>>(JSON_DEFAULT_DOC_MAPPER)?;

        assert_eq!(
            format!("{:?}", all_flatten_mapper),
            "AllFlattenDocMapper".to_string()
        );

        let expected_default_mapper = DefaultDocMapper::new(DocMapperConfig {
            store_source: true,
            ignore_unknown_fields: false,
            properties: vec![],
        })?;
        assert_eq!(
            format!("{:?}", deserialized_default_mapper),
            format!("{:?}", expected_default_mapper),
        );

        Ok(())
    }
}
