// Copyright (C) 2024 Quickwit, Inc.
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

use quickwit_common::service_error::ProutServiceError;
use thiserror;

include!("../codegen/quickwit/quickwit.cluster.rs");

pub type ClusterResult<T> = std::result::Result<T, ClusterError>;

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClusterError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl ProutServiceError for ClusterError {
    fn grpc_status_code(&self) -> tonic::Code {
        match self {
            Self::Internal(_) => tonic::Code::Internal,
            Self::Timeout(_) => tonic::Code::DeadlineExceeded,
            Self::Unavailable(_) => tonic::Code::Unavailable,
        }
    }

    fn new_internal(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_timeout(message: String) -> Self {
        Self::Timeout(message)
    }

    fn new_unavailable(message: String) -> Self {
        Self::Unavailable(message)
    }

    fn label_value(&self) -> &'static str {
        match self {
            ClusterError::Internal(_) => "internal",
            ClusterError::Timeout(_) => "timeout",
            ClusterError::Unavailable(_) => "unavailable",
        }
    }
}
