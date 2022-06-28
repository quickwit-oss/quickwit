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

use tantivy::schema::DocParsingError as TantivyDocParsingError;
use thiserror::Error;

/// Failed to parse query.
#[derive(Error, Debug)]
#[error("{0}")]
pub struct QueryParserError(#[from] anyhow::Error);

impl From<tantivy::query::QueryParserError> for QueryParserError {
    fn from(tantivy_error: tantivy::query::QueryParserError) -> Self {
        QueryParserError(From::from(tantivy_error))
    }
}

/// Error that may happen when parsing
/// a document from JSON.
#[derive(Debug, Error, PartialEq)]
pub enum DocParsingError {
    /// The provided string is not valid JSON.
    #[error("The provided string is not a valid JSON object. {0}")]
    NotJsonObject(String),
    /// One of the value could not be parsed.
    #[error("The field '{0}' could not be parsed: {1}")]
    ValueError(String, String),
    /// The json-document contains a field that is not declared in the schema.
    #[error("The document contains a field that is not declared in the schema: {0:?}")]
    NoSuchFieldInSchema(String),
    /// The document contains a array of values but a single value is expected.
    #[error("The document contains an array of values but a single value is expected: {0:?}")]
    MultiValuesNotSupported(String),
    /// The document does not contains a field that is required.
    #[error("The document must contain field {0:?}. As a fast field, it is implicitly required.")]
    RequiredFastField(String),
}

impl From<TantivyDocParsingError> for DocParsingError {
    fn from(value: TantivyDocParsingError) -> Self {
        match value {
            TantivyDocParsingError::InvalidJson(text) => DocParsingError::NoSuchFieldInSchema(text),
            TantivyDocParsingError::ValueError(text, error) => {
                DocParsingError::ValueError(text, format!("{:?}", error))
            }
        }
    }
}
