// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use quickwit_query::InvalidQuery;
use tantivy::schema::DocParsingError as TantivyDocParsingError;
use thiserror::Error;

/// Failed to parse query.
#[derive(Error, Debug)]
#[allow(missing_docs)]
pub enum QueryParserError {
    #[error("invalid json: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("invalid query: {0}")]
    InvalidQuery(#[from] InvalidQuery),
    #[error("invalid default search field: `{field_name}` {cause}")]
    InvalidDefaultField {
        cause: &'static str,
        field_name: String,
    },
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

/// Error that may happen when parsing
/// a document from JSON.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum DocParsingError {
    /// The provided string is not a syntactically valid JSON object.
    #[error("the provided string is not a syntactically valid JSON object: {0}")]
    NotJsonObject(String),
    /// One of the value could not be parsed.
    #[error("the field `{0}` could not be parsed: {1}")]
    ValueError(String, String),
    /// The json-document contains a field that is not declared in the schema.
    #[error("the document contains a field that is not declared in the schema: {0:?}")]
    NoSuchFieldInSchema(String),
    /// The document contains a array of values but a single value is expected.
    #[error("the document contains an array of values but a single value is expected: {0:?}")]
    MultiValuesNotSupported(String),
    /// The document does not contain a field that is required.
    #[error("the document must contain field {0:?}")]
    RequiredField(String),
}

impl From<TantivyDocParsingError> for DocParsingError {
    fn from(value: TantivyDocParsingError) -> Self {
        match value {
            TantivyDocParsingError::InvalidJson(text) => DocParsingError::NoSuchFieldInSchema(text),
            TantivyDocParsingError::ValueError(text, error) => {
                DocParsingError::ValueError(text, format!("{error:?}"))
            }
        }
    }
}
