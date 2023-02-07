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

use mrecordlog::error::*;
use quickwit_proto::{tonic, ServiceError, ServiceErrorCode};
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug, Serialize)]
pub enum IngestApiError {
    #[error("Data corruption: {msg}.")]
    Corruption { msg: String },
    #[error("Index `{index_id}` does not exist.")]
    IndexDoesNotExist { index_id: String },
    #[error("Index `{index_id}` already exists.")]
    IndexAlreadyExists { index_id: String },
    #[error("Ingest API service is down")]
    IngestAPIServiceDown,
    #[error("Io Error {msg}")]
    IoError { msg: String },
    #[error("Invalid position: {0}.")]
    InvalidPosition(String),
    #[error("Rate limited")]
    RateLimited,
}

impl From<io::Error> for IngestApiError {
    fn from(io_err: io::Error) -> Self {
        IngestApiError::IoError {
            msg: format!("{io_err:?}"),
        }
    }
}

impl ServiceError for IngestApiError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            IngestApiError::Corruption { .. } => ServiceErrorCode::Internal,
            IngestApiError::IndexDoesNotExist { .. } => ServiceErrorCode::NotFound,
            IngestApiError::IndexAlreadyExists { .. } => ServiceErrorCode::BadRequest,
            IngestApiError::IngestAPIServiceDown => ServiceErrorCode::Internal,
            IngestApiError::IoError { .. } => ServiceErrorCode::Internal,
            IngestApiError::InvalidPosition(_) => ServiceErrorCode::BadRequest,
            IngestApiError::RateLimited => ServiceErrorCode::RateLimited,
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

impl From<IngestApiError> for tonic::Status {
    fn from(error: IngestApiError) -> tonic::Status {
        let code = match &error {
            IngestApiError::Corruption { .. } => tonic::Code::Internal,
            IngestApiError::IndexDoesNotExist { .. } => tonic::Code::NotFound,
            IngestApiError::IndexAlreadyExists { .. } => tonic::Code::AlreadyExists,
            IngestApiError::IngestAPIServiceDown => tonic::Code::Internal,
            IngestApiError::IoError { .. } => tonic::Code::Internal,
            IngestApiError::InvalidPosition(_) => tonic::Code::InvalidArgument,
            IngestApiError::RateLimited => tonic::Code::ResourceExhausted,
        };
        let message = error.to_string();
        tonic::Status::new(code, message)
    }
}

impl From<ReadRecordError> for IngestApiError {
    fn from(err: ReadRecordError) -> IngestApiError {
        match err {
            ReadRecordError::IoError(io_err) => io_err.into(),
            ReadRecordError::Corruption => IngestApiError::Corruption {
                msg: "failed to read record".to_owned(),
            },
        }
    }
}

impl From<AppendError> for IngestApiError {
    fn from(err: AppendError) -> IngestApiError {
        match err {
            AppendError::IoError(io_err) => io_err.into(),
            AppendError::MissingQueue(index_id) => IngestApiError::IndexDoesNotExist { index_id },
            // these errors can't be reached right now
            AppendError::Past => {
                IngestApiError::InvalidPosition("appending record in the past".to_owned())
            }
            AppendError::Future => {
                IngestApiError::InvalidPosition("appending record in the future".to_owned())
            }
        }
    }
}

impl From<DeleteQueueError> for IngestApiError {
    fn from(err: DeleteQueueError) -> IngestApiError {
        match err {
            DeleteQueueError::IoError(io_err) => io_err.into(),
            DeleteQueueError::MissingQueue(index_id) => {
                IngestApiError::IndexDoesNotExist { index_id }
            }
        }
    }
}

impl From<TruncateError> for IngestApiError {
    fn from(err: TruncateError) -> IngestApiError {
        match err {
            TruncateError::IoError(io_err) => io_err.into(),
            TruncateError::MissingQueue(index_id) => IngestApiError::IndexDoesNotExist { index_id },
            // this error shouldn't happen (except due to a bug in MRecordLog?)
            TruncateError::TouchError(_) => {
                IngestApiError::InvalidPosition("touching at an invalid position".to_owned())
            }
            // this error can happen now, it used to happily trunk everything
            TruncateError::Future => IngestApiError::InvalidPosition(
                "trying to truncate past last ingested record".to_owned(),
            ),
        }
    }
}

pub type Result<T> = std::result::Result<T, IngestApiError>;
