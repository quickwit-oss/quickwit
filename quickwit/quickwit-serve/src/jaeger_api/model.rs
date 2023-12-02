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

use hyper::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct JaegerSearchBody { // TODO remove
    #[serde(default)]
    pub data: Option<Vec<String>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct JaegerResponseBody<T> {
    pub data: T,
}

#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Serialize, Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
pub struct TracesSearchQueryParams {
    #[serde(default)]
    pub service: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub lookback: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerError {
    #[serde(with = "http_serde::status_code")]
    pub status: StatusCode,
    pub message: String,
}

// impl JaegerError { // TODO remove?
//     pub fn internal_jaeger_error() -> Self {
//         JaegerError {
//             status: StatusCode::INTERNAL_SERVER_ERROR,
//             message: "Jaeger is not available".to_string(),
//         }
//     }
// }
