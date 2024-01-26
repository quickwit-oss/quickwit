// Copyright (C) 2024 Quickwit, Inc.
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
use std::time::Duration;

use reqwest::StatusCode;
use serde::de::DeserializeOwned;

use crate::error::{ApiError, Error, ErrorResponsePayload};

#[derive(Debug)]
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

    async fn extract_error_message(self) -> Option<String> {
        let error_body_bytes = self.inner.bytes().await.ok()?;
        let error_body_text = std::str::from_utf8(&error_body_bytes).ok()?;
        if let Ok(error_payload) = serde_json::from_str::<ErrorResponsePayload>(error_body_text) {
            Some(error_payload.message)
        } else {
            Some(error_body_text.to_string())
        }
    }

    async fn api_error(self) -> Error {
        let code = self.inner.status();
        let error_message = self.extract_error_message().await;
        Error::from(ApiError {
            message: error_message,
            code,
        })
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

#[derive(Clone)]
pub enum IngestSource {
    Str(String),
    File(PathBuf),
    Stdin,
}

/// A structure that represent a timeout. Unlike Duration it can also represent an infinite or no
/// timeout value.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
pub struct Timeout {
    duration: Duration,
}

const SECS_PER_MIN: u64 = 60;
const MINS_PER_HOUR: u64 = 60;
const HOURS_PER_DAY: u64 = 24;

impl Timeout {
    /// Creates a new timeout from duration
    pub const fn new(duration: Duration) -> Timeout {
        Timeout { duration }
    }

    /// Creates a new timeout from seconds
    pub const fn from_secs(secs: u64) -> Timeout {
        Timeout {
            duration: Duration::from_secs(secs),
        }
    }

    /// Creates a new timeout from minutes
    pub const fn from_mins(mins: u64) -> Timeout {
        Self::from_secs(mins * SECS_PER_MIN)
    }

    /// Creates a new timeout from hours
    pub const fn from_hours(hours: u64) -> Timeout {
        Self::from_secs(hours * SECS_PER_MIN * MINS_PER_HOUR)
    }

    /// Creates a new timeout from days
    pub const fn from_days(days: u64) -> Timeout {
        Self::from_secs(days * SECS_PER_MIN * MINS_PER_HOUR * HOURS_PER_DAY)
    }

    /// Creates a new infinite timeout
    pub const fn none() -> Timeout {
        Timeout {
            duration: Duration::MAX,
        }
    }

    /// Converts timeout into Some(Duration) or None if it is infinite.
    pub fn as_duration_opt(&self) -> Option<Duration> {
        if self.duration != Duration::MAX {
            Some(self.duration)
        } else {
            None
        }
    }
}
