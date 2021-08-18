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
#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum MetastoreError {
    #[error("Index `{index_id}` already exists.")]
    IndexAlreadyExists { index_id: String },

    #[error("Access forbidden: `{message}`.")]
    Forbidden { message: String },

    #[error("Index `{index_id}` does not exist.")]
    IndexDoesNotExist { index_id: String },

    /// Any generic internal error.
    /// The message can be helpful to users, but the detail of the error
    /// are judged uncoverable and not useful for error handling.
    #[error("Internal error: `{message}` Cause: `{cause}`.")]
    InternalError {
        message: String,
        cause: anyhow::Error,
    },

    #[error("Failed to deserialize index metadata: `{cause}`")]
    InvalidManifest { cause: serde_json::Error },

    #[error("IOError `{0}`")]
    Io(io::Error),

    #[error("Split `{split_id}` does not exist.")]
    SplitDoesNotExist { split_id: String },

    #[error("Split `{split_id}` is not staged.")]
    SplitIsNotStaged { split_id: String },

    #[error("Publish checkpoint delta overlaps with the current checkpoint: {0:?}.")]
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
