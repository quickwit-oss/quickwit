// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fmt, io};

use serde::{Deserialize, Serialize};
use tantivy::directory::error::{OpenDirectoryError, OpenReadError};
use thiserror::Error;

/// Storage error kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageErrorKind {
    /// The target file does not exist.
    NotFound,
    /// The request credentials do not allow for this operation.
    Unauthorized,
    /// A third-party service forbids this operation, or is misconfigured.
    Service,
    /// Any generic internal error.
    Internal,
    /// A timeout occurred during the operation.
    Timeout,
    /// Io error.
    Io,
}

/// Generic Storage Resolver Error.
#[allow(missing_docs)]
#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
pub enum StorageResolverError {
    /// The storage config is invalid.
    #[error("invalid storage config: `{0}`")]
    InvalidConfig(String),

    /// The URI is malformed or does not contain sufficient information to connect to the storage.
    #[error("invalid storage URI: `{0}`")]
    InvalidUri(String),

    /// The requested backend is unsupported or unavailable.
    #[error("unsupported storage backend: `{0}`")]
    UnsupportedBackend(String),

    /// The URI is valid, and is meant to be handled by this resolver,
    /// but the resolver failed to actually connect to the storage.
    /// e.g. connection error, credentials error, incompatible version,
    /// internal error in third party, etc.
    #[error("failed to open storage {kind:?}: {message}")]
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
#[error("storage error(kind={kind:?}, source={source})")]
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

/// Error returned by `bulk_delete`. Under the hood, `bulk_delete` groups the files to
/// delete into multiple batches of fixed size and issues one delete objects request per batch. The
/// whole operation can fail in multiples ways, which is reflected by the quirkiness of the API of
/// [`BulkDeleteError`]. First, a batch can fail partially, i.e. some objects are deleted while
/// others are not. The `successes` and `failures` attributes of the error will be populated
/// accordingly. Second, a batch can fail completely, in which case the `error` field will be set.
/// Because a batch failing entirely usually indicates a systemic error, for instance, a connection
/// or credentials issue, `bulk_delete` does not attempt to delete the remaining batches and
/// populates the `unattempted` attribute. Consequently, the attributes of this error are not
/// "mutually exclusive": there exists a path where all those fields are not empty. The caller is
/// expected to handle this error carefully and inspect the instance thoroughly before any retry
/// attempt.
#[must_use]
#[derive(Debug, Default, thiserror::Error)]
pub struct BulkDeleteError {
    /// Error that occurred for a whole batch and caused the entire deletion operation to be
    /// aborted.
    pub error: Option<StorageError>,
    /// List of files that were successfully deleted, including non-existing files.
    pub successes: Vec<PathBuf>,
    /// List of files that failed to be deleted along with the corresponding failure descriptions.
    pub failures: HashMap<PathBuf, DeleteFailure>,
    /// List of remaining files to delete before the operation was aborted.
    pub unattempted: Vec<PathBuf>,
}

/// Describes the failure for an individual file in a batch delete operation.
#[derive(Debug, Default)]
pub struct DeleteFailure {
    /// The error that occurred for this file.
    pub error: Option<StorageError>,
    /// The failure code is a string that uniquely identifies an error condition. It is meant to be
    /// read and understood by programs that detect and handle errors by type.
    pub code: Option<String>,
    /// The error message contains a generic description of the error condition in English. It is
    /// intended for a human audience. Simple programs display the message directly to the end user
    /// if they encounter an error condition they don't know how or don't care to handle.
    /// Sophisticated programs with more exhaustive error handling and proper internationalization
    /// are more likely to ignore the error message.
    pub message: Option<String>,
}

impl fmt::Display for BulkDeleteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bulk delete error ({} success(es),  {} failure(s), {} unattempted)",
            self.successes.len(),
            self.failures.len(),
            self.unattempted.len()
        )?;
        Ok(())
    }
}
