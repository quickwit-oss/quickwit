// Copyright (C) 2021 Quickwit, Inc.
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

use hyper::header::CONTENT_TYPE;
use hyper::StatusCode;
use serde::Deserialize;
use warp::{reply, Reply};

use crate::error::ApiError;

/// Output format for the search results.
#[derive(Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Json,
    PrettyJson,
}

impl Default for Format {
    fn default() -> Self {
        Format::PrettyJson
    }
}

impl ToString for Format {
    fn to_string(&self) -> String {
        match &self {
            Self::Json => "json".to_string(),
            Self::PrettyJson => "prety-json".to_string(),
        }
    }
}

impl Format {
    fn resp_body<T: serde::Serialize>(self, val: T) -> serde_json::Result<String> {
        match self {
            Format::Json => serde_json::to_string(&val),
            Format::PrettyJson => serde_json::to_string_pretty(&val),
        }
    }

    pub fn make_reply<T: serde::Serialize>(self, result: Result<T, ApiError>) -> impl Reply {
        let status_code: StatusCode;
        let body_json = match result {
            Ok(success) => {
                status_code = StatusCode::OK;
                self.resp_body(success)
            }
            Err(err) => {
                status_code = err.http_status_code();
                self.resp_body(err)
            } //< yeah it is lame it is not formatted, but it should never happen really.
        }
        .unwrap_or_else(|_| {
            tracing::error!("Error: the response serialization failed.");
            "Error: Failed to serialize response.".to_string()
        });
        let reply_with_header = reply::with_header(body_json, CONTENT_TYPE, "application/json");
        reply::with_status(reply_with_header, status_code)
    }
}
