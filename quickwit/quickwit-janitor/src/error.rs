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

use quickwit_metastore::MetastoreError;
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Janitor errors.
#[allow(missing_docs)]
#[derive(Error, Debug, Serialize, Deserialize)]
pub enum JanitorError {
    #[error("Invalid delete query: `{0}`.")]
    InvalidDeleteQuery(String),
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Metastore error `{0}`.")]
    MetastoreError(#[from] MetastoreError),
}

impl ServiceError for JanitorError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            JanitorError::InvalidDeleteQuery(_) => ServiceErrorCode::BadRequest,
            JanitorError::InternalError(_) => ServiceErrorCode::Internal,
            JanitorError::MetastoreError(error) => error.status_code(),
        }
    }
}
