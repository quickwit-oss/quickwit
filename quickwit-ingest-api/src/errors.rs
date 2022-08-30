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

use std::io;

use quickwit_proto::{tonic, ServiceError, ServiceErrorCode};
use serde::Serialize;
use thiserror::Error;

use crate::recordlog::ReadRecordError;

#[derive(Error, Debug, Serialize)]
pub enum IngestApiError {
    #[error("Data Corruption : {msg}.")]
    Corruption { msg: String },
    #[error("Index `{index_id}` does not exist.")]
    IndexDoesNotExist { index_id: String },
    #[error("Index `{index_id}` already exists.")]
    IndexAlreadyExists { index_id: String },
    #[error("Ingest API service is down")]
    IngestAPIServiceDown,
    #[error("Read record error. `{0:?}`")]
    ReadRecordError(#[from] ReadRecordError),
    #[error("Io Error")]
    IoError(String)
}

impl From<io::Error> for IngestApiError {
    fn from(io_err: io::Error) -> Self {
        IngestApiError::IoError(format!("{io_err:?}"))
    }
}

impl ServiceError for IngestApiError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            IngestApiError::Corruption { .. } => ServiceErrorCode::Internal,
            IngestApiError::IndexDoesNotExist { .. } => ServiceErrorCode::NotFound,
            IngestApiError::IndexAlreadyExists { .. } => ServiceErrorCode::BadRequest,
            IngestApiError::IngestAPIServiceDown => ServiceErrorCode::Internal,
            IngestApiError::ReadRecordError(_) => ServiceErrorCode::Internal,
            IngestApiError::IoError(_) => ServiceErrorCode::Internal,
        }
    }
}

#[derive(Error, Debug)]
#[error("Key should contain 16 bytes. It contained {0} bytes.")]
pub struct CorruptedKey(pub usize);

impl From<CorruptedKey> for IngestApiError {
    fn from(err: CorruptedKey) -> Self {
        IngestApiError::Corruption {
            msg: format!("CorruptedKey: {err:?}"),
        }
    }
}

// TODO remove this.
impl From<IngestApiError> for tonic::Status {
    fn from(error: IngestApiError) -> tonic::Status {
        let code = match &error {
            IngestApiError::Corruption { .. } => tonic::Code::Internal,
            IngestApiError::IndexDoesNotExist { .. } => tonic::Code::NotFound,
            IngestApiError::IndexAlreadyExists { .. } => tonic::Code::AlreadyExists,
            IngestApiError::IngestAPIServiceDown => tonic::Code::Internal,
            IngestApiError::ReadRecordError(_) => tonic::Code::Internal,
            IngestApiError::IoError(_) => tonic::Code::Internal,
        };
        let message = error.to_string();
        tonic::Status::new(code, message)
    }
}

pub type Result<T> = std::result::Result<T, IngestApiError>;
