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

use quickwit_actors::AskError;
use thiserror;

#[path = "../codegen/quickwit/quickwit.cache_storage.rs"]
mod codegen;

pub use codegen::*;

pub type Result<T> = std::result::Result<T, CacheStorageError>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum CacheStorageError {
    #[error("An internal error occurred: {0}.")]
    Internal(String),
    #[error("Cache storage is unavailable: {0}.")]
    Unavailable(String),
}

impl From<CacheStorageError> for tonic::Status {
    fn from(error: CacheStorageError) -> Self {
        match error {
            CacheStorageError::Internal(message) => tonic::Status::internal(message),
            CacheStorageError::Unavailable(message) => tonic::Status::unavailable(message),
        }
    }
}

impl From<tonic::Status> for CacheStorageError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::Unavailable => {
                CacheStorageError::Unavailable(status.message().to_string())
            }
            _ => CacheStorageError::Internal(status.message().to_string()),
        }
    }
}

// impl ServiceError for CacheStorageError {
//     fn status_code(&self) -> ServiceErrorCode {
//         match self {
//             Self::MissingPipeline { .. } => ServiceErrorCode::NotFound,
//             Self::PipelineAlreadyExists { .. } => ServiceErrorCode::BadRequest,
//             Self::InvalidParams(_) => ServiceErrorCode::BadRequest,
//             Self::Io(_) => ServiceErrorCode::Internal,
//             Self::Internal(_) => ServiceErrorCode::Internal,
//             Self::MetastoreError(_) => ServiceErrorCode::Internal,
//             Self::StorageResolverError(_) => ServiceErrorCode::Internal,
//             Self::Unavailable => ServiceErrorCode::Unavailable,
//         }
//     }
// }

impl From<AskError<CacheStorageError>> for CacheStorageError {
    fn from(error: AskError<CacheStorageError>) -> Self {
        match error {
            AskError::ErrorReply(error) => error,
            AskError::MessageNotDelivered => CacheStorageError::Unavailable(error.to_string()),
            AskError::ProcessMessageError => CacheStorageError::Internal(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {}
