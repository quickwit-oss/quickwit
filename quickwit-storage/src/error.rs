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

use std::{convert::TryFrom, fmt, io};
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

impl TryFrom<&str> for StorageErrorKind {
    type Error = ();

    fn try_from(input: &str) -> Result<StorageErrorKind, ()> {
        match input {
            "DoesNotExist" => Ok(StorageErrorKind::DoesNotExist),
            "Unauthorized" => Ok(StorageErrorKind::Unauthorized),
            "Service" => Ok(StorageErrorKind::Service),
            "InternalError" => Ok(StorageErrorKind::InternalError),
            "Io" => Ok(StorageErrorKind::Io),
            _ => Err(()),
        }
    }
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

impl TryFrom<&str> for StorageResolverError {
    type Error = ();
    fn try_from(message: &str) -> Result<Self, ()> {
        if message.starts_with("Invalid format for URI: required: `") {
            Ok(message
                .split_once("Invalid format for URI: required: `")
                .map(|(_, suffix)| {
                    StorageResolverError::InvalidUri(suffix.trim_end_matches('`').to_owned())
                })
                .unwrap())
        } else if message.starts_with("Unsupported protocol") {
            Ok(StorageResolverError::ProtocolUnsupported(
                message.to_owned(),
            ))
        } else if message.starts_with("Failed to open storage: `") {
            message
                .split_once("Failed to open storage: `")
                .map(|(_, suffix)| StorageError::try_from(suffix.trim_end_matches('`')))
                .map(|error| error.map(StorageResolverError::FailedToOpenStorage))
                .unwrap()
        } else {
            Err(())
        }
    }
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
        match err.kind() {
            io::ErrorKind::NotFound => StorageErrorKind::DoesNotExist.with_error(err),
            _ => StorageErrorKind::Io.with_error(err),
        }
    }
}

impl TryFrom<&str> for StorageError {
    type Error = ();
    fn try_from(message: &str) -> Result<Self, ()> {
        // TODO: we need to parse the message to get the right kind and source.
        let (_, end) = message.split_once("source=").ok_or(())?;
        Ok(StorageError {
            kind: StorageErrorKind::InternalError,
            source: anyhow::anyhow!(end.trim_end_matches(')').to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use crate::{StorageError, StorageErrorKind, StorageResolverError};

    #[test]
    fn test_storage_kind_try_from() {
        assert!(StorageErrorKind::try_from("Service").is_ok());
        assert_eq!(
            StorageErrorKind::Service,
            StorageErrorKind::try_from("Service").unwrap()
        );
    }

    #[test]
    fn test_storage_error_try_from() {
        assert!(StorageError::try_from("StorageError(kind={kind:?}, source={source})").is_ok());
    }

    #[test]
    fn test_storage_resolver_error_try_from() {
        let message = "Invalid format for URI: required: `uri`";
        assert!(StorageResolverError::try_from(message).is_ok());
        assert_eq!(
            StorageResolverError::try_from(message).unwrap().to_string(),
            message
        );
        let message = "Failed to open storage: `StorageError(kind=InternalError, source=test)`";
        assert!(StorageResolverError::try_from(message).is_ok());
        assert_eq!(
            StorageResolverError::try_from(message).unwrap().to_string(),
            message
        );
    }
}
