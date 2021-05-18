/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::{fmt, io};
use thiserror::Error;

/// Storage error kind.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StorageErrorKind {
    /// The target index does not exist.
    DoesNotExist,
    /// The request credentials do not allow for this operation.
    Unauthorized,
    /// A third-party service forbids this operation.
    Service,
    /// Any generic internal error.
    InternalError,
    /// Io error.
    Io,
}

/// Generic Storage Resolver Error.
#[derive(Error, Debug)]
pub enum StorageResolverError {
    /// The input is not a valid URI.
    /// A protocol is required for the URI.
    #[error("Invalid format for URI: required: `{0}`")]
    InvalidUri(String),
    /// The protocol is not supported by this resolver.
    #[error("Unsupported protocol")]
    ProtocolUnsupported(String),
    /// The URI is valid, and is meant to be handled by this resolver,
    /// but the resolver failed to actually connect to the storage.
    /// e.g. Connection error, credential error, incompatible version,
    /// internal error in third party etc.
    #[error("Failed to open storage: `{0}`")]
    FailedToOpenStorage(crate::StorageError),
}

impl StorageErrorKind {
    /// Creates a StorageError.
    pub fn with_error<E>(self, source: E) -> StorageError
    where
        anyhow::Error: From<E>,
    {
        StorageError {
            kind: self,
            source: From::from(source),
        }
    }
}

impl From<StorageError> for io::Error {
    fn from(storage_err: StorageError) -> Self {
        let io_error_kind = match storage_err.kind() {
            StorageErrorKind::DoesNotExist => io::ErrorKind::NotFound,
            _ => io::ErrorKind::Other,
        };
        io::Error::new(io_error_kind, storage_err.source)
    }
}

/// Generic StorageError.
#[derive(Error, Debug)]
#[error("StorageError(kind={kind:?}, source={source})")]
pub struct StorageError {
    kind: StorageErrorKind,
    #[source]
    source: anyhow::Error,
}

/// Generic Result type for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;

impl StorageError {
    /// Add some context to the wrapper error.
    pub fn add_context<C>(self, ctx: C) -> Self
    where
        C: fmt::Display + Send + Sync + 'static,
    {
        StorageError {
            kind: self.kind,
            source: self.source.context(ctx),
        }
    }

    /// Returns the corresponding `StorageErrorKind` for this error.
    pub fn kind(&self) -> StorageErrorKind {
        self.kind
    }
}

impl From<io::Error> for StorageError {
    fn from(err: io::Error) -> StorageError {
        StorageError {
            kind: StorageErrorKind::Io,
            source: anyhow::Error::from(err),
        }
    }
}
