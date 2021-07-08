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

#![warn(missing_docs)]
#![allow(clippy::bool_assert_comparison)]

//! Doc mapping defines the way to convert a json like documents to
//! a document indexable by tantivy engine, aka tantivy::Document.

mod all_flatten_mapper;
mod default_doc_mapper;
mod error;
mod mapper;
mod query_builder;
mod wikipedia_mapper;

pub use error::QueryParserError;

pub use all_flatten_mapper::AllFlattenDocMapper;
pub use default_doc_mapper::{DefaultDocMapper, DefaultDocMapperBuilder, DocParsingError};
pub use mapper::{IndexConfig, SortBy, SortOrder};
pub use wikipedia_mapper::WikipediaMapper;
