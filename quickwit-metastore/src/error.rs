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

use std::fmt::Display;
use std::io;

use thiserror::Error;

/// Metastore error kinds.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MetastoreErrorKind {
    /// The target index already exists.
    ExistingIndexUri,

    /// The target split already exists.
    ExistingSplitId,

    /// Forbidden error.
    Forbidden,

    /// The target index does not exist.
    IndexDoesNotExist,

    /// Any generic internal error.
    InternalError,

    /// Invalid manifest.
    InvalidManifest,

    /// Io error.
    Io,

    /// The target split does not exist.
    SplitDoesNotExist,

    /// The target split is not staged.
    SplitIsNotStaged,
}

impl MetastoreErrorKind {
    /// Creates a MetastoreError.
    pub fn with_error<E>(self, source: E) -> MetastoreError
    where
        anyhow::Error: From<E>,
    {
        MetastoreError {
            kind: self,
            source: From::from(source),
        }
    }
}

impl From<MetastoreError> for io::Error {
    fn from(metastore_err: MetastoreError) -> Self {
        let io_error_kind = match metastore_err.kind() {
            MetastoreErrorKind::SplitDoesNotExist => io::ErrorKind::NotFound,
            _ => io::ErrorKind::Other,
        };
        io::Error::new(io_error_kind, metastore_err.source)
    }
}

/// Generic Metastore error.
#[derive(Error, Debug)]
#[error("MetastoreError(kind={kind:?}, source={source})")]
pub struct MetastoreError {
    kind: MetastoreErrorKind,
    #[source]
    source: anyhow::Error,
}

impl MetastoreError {
    /// Add some context to the wrapper error.
    pub fn add_context<C>(self, ctx: C) -> Self
    where
        C: Display + Send + Sync + 'static,
    {
        MetastoreError {
            kind: self.kind,
            source: self.source.context(ctx),
        }
    }

    /// Returns the corresponding `MetastoreErrorKind` for this error.
    pub fn kind(&self) -> MetastoreErrorKind {
        self.kind
    }
}

impl From<io::Error> for MetastoreError {
    fn from(err: io::Error) -> MetastoreError {
        MetastoreError {
            kind: MetastoreErrorKind::Io,
            source: anyhow::Error::from(err),
        }
    }
}

/// Generic Result type for metastore operations.
pub type MetastoreResult<T> = Result<T, MetastoreError>;

/// Generic Storage Resolver Error.
#[derive(Error, Debug)]
pub enum MetastoreResolverError {
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
    #[error("Failed to open metastore: `{0}`")]
    FailedToOpenMetastore(MetastoreError),
}
