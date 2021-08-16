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

use std::io;

use thiserror::Error;

use crate::checkpoint::IncompatibleCheckpoint;

/// Metastore error kinds.
#[derive(Debug, Error)]
pub enum MetastoreError {
    /// The target index already exists (Returned when creating a new index).
    #[error("Index `{index_id}` already exists.")]
    IndexAlreadyExists {
        /// The `index_id` of the index that failed to be created because another index
        /// with the same id already exists.
        index_id: String,
    },

    /// Forbidden error.
    #[error("Access forbidden: `{message}`.")]
    Forbidden {
        /// Error Message
        message: String,
    },

    /// The target index does not exist.
    #[error("Index `{index_id}` does not exist.")]
    IndexDoesNotExist {
        /// Index Id that was request (but is missing).
        index_id: String,
    },

    /// Any generic internal error.
    /// The message can be helpful to users, but the detail of the error
    /// are judged uncoverable and not useful for error handling.
    #[error("Internal error: `{message}` Cause: `{cause}`.")]
    InternalError {
        /// Error Message
        message: String,
        /// Root cause
        cause: anyhow::Error,
    },

    /// Invalid manifest.
    #[error("Failed to deserialize index metadata: `{cause}`")]
    InvalidManifest {
        /// Serde error
        cause: serde_json::Error,
    },

    /// Io error.
    #[error("IOError `{0}`")]
    Io(io::Error),

    /// The target split does not exist.
    #[error("Split `{split_id}` does not exist.")]
    SplitDoesNotExist {
        /// missing split id.
        split_id: String,
    },

    /// The target split is not staged.
    #[error("Split `{split_id}` is not staged.")]
    SplitIsNotStaged {
        /// Split that should have been staged.
        split_id: String,
    },

    /// The index is actually
    #[error("Publish checkpoint cannot be applied: {0:?}.")]
    IncompatibleCheckpointDelta(#[from] IncompatibleCheckpoint),
}

/// Generic Result type for metastore operations.
pub type MetastoreResult<T> = Result<T, MetastoreError>;

/// Generic Storage Resolver Error.
#[derive(Error, Debug)]
pub enum MetastoreResolverError {
    /// The input is not a valid URI.
    /// A protocol is required for the URI.
    #[error("Invalid URI format: required: `{0}`")]
    InvalidUri(String),

    /// The protocol is not supported by this resolver.
    #[error("Unsupported protocol: `{0}`")]
    ProtocolUnsupported(String),

    /// The URI is valid, and is meant to be handled by this resolver,
    /// but the resolver failed to actually connect to the storage.
    /// e.g. Connection error, credential error, incompatible version,
    /// internal error in third party, etc.
    #[error("Failed to open metastore: `{0}`")]
    FailedToOpenMetastore(MetastoreError),
}
