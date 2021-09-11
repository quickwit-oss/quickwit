// Copyright (C) 2021 Quickwit, Inc.
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

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Cluster error kinds.
#[derive(Error, Debug, Serialize, Deserialize)]
pub enum ClusterError {
    /// Create cluster error.
    #[error("Failed to create cluster: `{message}`")]
    CreateClusterError {
        /// Underlying error message.
        message: String,
    },

    /// Port binding error.
    #[error("Failed to bind to UDP port `{port}` for the gossip membership protocol: `{message}`")]
    UDPPortBindingError {
        /// Port number.
        port: u16,
        /// Underlying error message.
        message: String,
    },

    /// Read host key error.
    #[error("Failed to read host key: `{message}`")]
    ReadHostKeyError {
        /// Underlying error message.
        message: String,
    },

    /// Write host key error.
    #[error("Failed to write host key: `{message}`")]
    WriteHostKeyError {
        /// Underlying error message.
        message: String,
    },
}

impl From<ClusterError> for tonic::Status {
    fn from(error: ClusterError) -> tonic::Status {
        let code = match error {
            ClusterError::CreateClusterError { .. } => tonic::Code::Internal,
            ClusterError::UDPPortBindingError { .. } => tonic::Code::PermissionDenied,
            ClusterError::ReadHostKeyError { .. } => tonic::Code::Internal,
            ClusterError::WriteHostKeyError { .. } => tonic::Code::Internal,
        };
        let message = error.to_string();
        tonic::Status::new(code, message)
    }
}

/// Generic Result type for cluster operations.
pub type ClusterResult<T> = Result<T, ClusterError>;
