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

mod authorization_layer;

#[cfg(not(feature = "enterprise"))]
#[path = "community.rs"]
mod implementation;

#[cfg(feature = "enterprise")]
#[path = "enterprise.rs"]
mod implementation;

pub use implementation::*;
use serde::{Deserialize, Serialize};

#[derive(thiserror::Error, Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub enum AuthorizationError {
    #[error("authorization token missing")]
    AuthorizationTokenMissing,
    #[error("invalid token")]
    InvalidToken,
    #[error("permission denied")]
    PermissionDenied,
}

impl From<AuthorizationError> for tonic::Status {
    fn from(authorization_error: AuthorizationError) -> tonic::Status {
        match authorization_error {
            AuthorizationError::AuthorizationTokenMissing => {
                tonic::Status::unauthenticated("Authorization token missing")
            }
            AuthorizationError::InvalidToken => {
                tonic::Status::unauthenticated("Invalid authorization token")
            }
            AuthorizationError::PermissionDenied => {
                tonic::Status::permission_denied("Permission denied")
            }
        }
    }
}
