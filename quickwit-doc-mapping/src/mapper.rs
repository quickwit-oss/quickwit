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

use tantivy::{Document, query::Query, schema::{DocParsingError, Schema}};
use crate::{all_flatten_mapper::AllFlattenDocMapper, default_mapper::{DefaultDocMapper, DocMapperConfig}};


/// The `DocMapper` trait defines the way of defining how a (json) document, 
/// and the fields it contains, are stored and indexed.
///
/// The `DocMapper` trait is in charge of implementing :
///
/// - a way to build a tantivy::Document from a json payload
/// - a way to build a tantivy::Query from a SearchRequest 
/// - a way to build a tantivy:Schema
/// 
pub trait DocMapper: Send + Sync + 'static {
    fn doc_from_json(&self, doc_json: &str) -> Result<Document, DocParsingError>;
    fn schema(&self) -> Schema;
    fn query(&self, _request: SearchRequest) -> Box<dyn Query>;
}
// TODO: this is a placeholder, to be removed when it will be implementend in the search-api crate
pub struct SearchRequest {}

#[derive(Clone)]
pub enum DocMapperType {
    Default(DocMapperConfig),
    AllFlatten,
}

pub fn build_doc_mapper(mapper_type: DocMapperType) -> anyhow::Result<Box<dyn DocMapper>> {
    match mapper_type {
        DocMapperType::Default(config) => 
            DefaultDocMapper::new(config)
                .map(|mapper| Box::new(mapper) as Box<dyn DocMapper>),
        DocMapperType::AllFlatten => Box::new(AllFlattenDocMapper::new())
            .map(|mapper| Box::new(mapper) as Box<dyn DocMapper>),
    }
}
