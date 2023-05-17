// Copyright (C) 2023 Quickwit, Inc.
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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum InvalidQuery {
    #[error("Query is incompatible with schema. {0}).")]
    SchemaError(String),
    #[error("Expected `{expected_value_type}` boundary for field `{field_name}`")]
    InvalidBoundary {
        expected_value_type: &'static str,
        field_name: String,
    },
    #[error(
        "Expected a `{expected_value_type}` search value for field `{field_name}`. Got `{value}`."
    )]
    InvalidSearchTerm {
        expected_value_type: &'static str,
        field_name: String,
        value: String,
    },
    #[error("Range query on `{value_type}` field (`{field_name}`) forbidden")]
    RangeQueryNotSupportedForField {
        value_type: &'static str,
        field_name: String,
    },
    #[error("Field does not exist: `{full_path}`")]
    FieldDoesNotExist { full_path: String },
    #[error("Json field root is not a valid search field: `{full_path}`")]
    JsonFieldRootNotSearchable { full_path: String },
    #[error("User query should have been parsed")]
    UserQueryNotParsed,
}
