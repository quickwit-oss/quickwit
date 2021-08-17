//  Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Cluster error kinds.
#[derive(Error, Debug, Serialize, Deserialize)]
pub enum ClusterError {
    /// Create cluster error.
    #[error("Failed to create cluster: `{message}`")]
    CreateClusterError {
        /// Message.
        message: String,
    },

    /// Port binding error.
    #[error("Failed to bind UDP port `{port}` for the gossip membership algorithm: `{message}`")]
    UDPPortBindingError {
        /// Port number.
        port: u16,

        /// Message.
        message: String,
    },

    /// Read host key error.
    #[error("Failed to read host key: `{message}`")]
    ReadHostKeyError {
        /// Message.
        message: String,
    },

    /// Write host key error.
    #[error("Failed to write host key: `{message}`")]
    WriteHostKeyError {
        /// Message.
        message: String,
    },
}

impl ClusterError {
    fn convert_to_tonic_status_code(cluster_error: &ClusterError) -> tonic::Code {
        match cluster_error {
            ClusterError::CreateClusterError { .. } => tonic::Code::Internal,
            ClusterError::UDPPortBindingError { .. } => tonic::Code::PermissionDenied,
            ClusterError::ReadHostKeyError { .. } => tonic::Code::Internal,
            ClusterError::WriteHostKeyError { .. } => tonic::Code::Internal,
        }
    }

    pub fn convert_to_tonic_status(cluster_error: ClusterError) -> tonic::Status {
        let error_json = serde_json::to_string_pretty(&cluster_error)
            .unwrap_or_else(|_| "Failed to serialize error".to_string());
        let code = ClusterError::convert_to_tonic_status_code(&cluster_error);
        tonic::Status::new(code, error_json)
    }
}

/// Generic Result type for cluster operations.
pub type ClusterResult<T> = Result<T, ClusterError>;
