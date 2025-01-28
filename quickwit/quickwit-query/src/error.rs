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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum InvalidQuery {
    #[error("query is incompatible with schema. {0})")]
    SchemaError(String),
    #[error("expected `{expected_value_type}` boundary for field `{field_name}`")]
    InvalidBoundary {
        expected_value_type: &'static str,
        field_name: String,
    },
    #[error(
        "expected a `{expected_value_type}` search value for field `{field_name}`, got `{value}`"
    )]
    InvalidSearchTerm {
        expected_value_type: &'static str,
        field_name: String,
        value: String,
    },
    #[error("range query on `{value_type}` field (`{field_name}`) forbidden")]
    RangeQueryNotSupportedForField {
        value_type: &'static str,
        field_name: String,
    },
    #[error("field does not exist: `{full_path}`")]
    FieldDoesNotExist { full_path: String },
    #[error("Json field root is not a valid search field: `{full_path}`")]
    JsonFieldRootNotSearchable { full_path: String },
    #[error("user query should have been parsed")]
    UserQueryNotParsed,
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}
