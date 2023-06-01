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

use std::sync::Arc;
use std::{fmt, io};

use serde::{Deserialize, Serialize};
use tantivy::directory::error::{OpenDirectoryError, OpenReadError};
use thiserror::Error;

/// Storage error kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageErrorKind {
    /// The target index does not exist.
    NotFound,
    /// The request credentials do not allow for this operation.
    Unauthorized,
    /// A third-party service forbids this operation, or is misconfigured.
    Service,
    /// Any generic internal error.
    InternalError,
    /// A timeout occured during the operation.
    Timeout,
    /// Io error.
    Io,
}

/// Generic Storage Resolver Error.
#[allow(missing_docs)]
#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
pub enum StorageResolverError {
    /// The storage config is invalid.
    #[error("Invalid storage config: `{0}`")]
    InvalidConfig(String),

    /// The URI does not contain sufficient information to connect to the storage.
    #[error("Invalid storage URI: `{0}`")]
    InvalidUri(String),

    /// The requested backend is unsupported or unavailable.
    #[error("Unsupported storage backend: `{0}`")]
    UnsupportedBackend(String),

    /// The URI is valid, and is meant to be handled by this resolver,
    /// but the resolver failed to actually connect to the storage.
    /// e.g. Connection error, credential error, incompatible version,
    /// internal error in third party etc.
    #[error("Failed to open storage {kind:?}: {message}.")]
    FailedToOpenStorage {
        kind: crate::StorageErrorKind,
        message: String,
    },
}

impl StorageErrorKind {
    /// Creates a StorageError.
    pub fn with_error(self, source: impl Into<anyhow::Error>) -> StorageError {
        StorageError {
            kind: self,
            source: Arc::new(source.into()),
        }
    }
}

impl From<StorageError> for io::Error {
    fn from(storage_err: StorageError) -> Self {
        let io_error_kind = match storage_err.kind() {
            StorageErrorKind::NotFound => io::ErrorKind::NotFound,
            _ => io::ErrorKind::Other,
        };
        // TODO: This is swallowing the context of the source error.
        io::Error::new(io_error_kind, storage_err.source.to_string())
    }
}

/// Generic StorageError.
#[derive(Debug, Clone, Error)]
#[error("StorageError(kind={kind:?}, source={source})")]
#[allow(missing_docs)]
pub struct StorageError {
    pub kind: StorageErrorKind,
    #[source]
    source: Arc<anyhow::Error>,
}

/// Generic Result type for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;

impl StorageError {
    /// Add some context to the wrapper error.
    pub fn add_context<C>(self, ctx: C) -> Self
    where C: fmt::Display + Send + Sync + 'static {
        StorageError {
            kind: self.kind,
            source: Arc::new(anyhow::anyhow!("{ctx}").context(self.source)),
        }
    }

    /// Returns the corresponding `StorageErrorKind` for this error.
    pub fn kind(&self) -> StorageErrorKind {
        self.kind
    }
}

impl From<io::Error> for StorageError {
    fn from(err: io::Error) -> StorageError {
        match err.kind() {
            io::ErrorKind::NotFound => StorageErrorKind::NotFound.with_error(err),
            _ => StorageErrorKind::Io.with_error(err),
        }
    }
}

impl From<OpenDirectoryError> for StorageError {
    fn from(err: OpenDirectoryError) -> StorageError {
        match err {
            OpenDirectoryError::DoesNotExist(_) => StorageErrorKind::NotFound.with_error(err),
            _ => StorageErrorKind::Io.with_error(err),
        }
    }
}

impl From<OpenReadError> for StorageError {
    fn from(err: OpenReadError) -> StorageError {
        match err {
            OpenReadError::FileDoesNotExist(_) => StorageErrorKind::NotFound.with_error(err),
            _ => StorageErrorKind::Io.with_error(err),
        }
    }
}
