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

use std::path::PathBuf;

use reqwest::StatusCode;
use serde::de::DeserializeOwned;

use crate::error::{ApiError, Error, ErrorResponsePayload};

pub struct ApiResponse {
    inner: reqwest::Response,
}

impl ApiResponse {
    pub fn new(inner: reqwest::Response) -> Self {
        Self { inner }
    }
    /// Get the HTTP status code of the response
    pub fn status_code(&self) -> StatusCode {
        self.inner.status()
    }

    /// Checks status and returns error if appropriate.
    pub async fn check(self) -> Result<(), Error> {
        if self.inner.status().is_client_error() || self.inner.status().is_server_error() {
            return Err(self.api_error().await);
        }
        Ok(())
    }

    async fn api_error(self) -> Error {
        let code = self.inner.status();
        if let Ok(error_payload) = self.inner.json::<ErrorResponsePayload>().await {
            Error::from(ApiError {
                message: Some(error_payload.message),
                code,
            })
        } else {
            Error::from(ApiError {
                message: None,
                code,
            })
        }
    }

    pub async fn deserialize<T: DeserializeOwned>(self) -> Result<T, Error> {
        if self.inner.status().is_client_error() || self.inner.status().is_server_error() {
            Err(self.api_error().await)
        } else {
            let object = self.inner.json::<T>().await?;
            Ok(object)
        }
    }
}

pub enum IngestSource {
    File(PathBuf),
    Stdin,
}
