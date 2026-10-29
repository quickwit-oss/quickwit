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

//! Biscuit-based authorization (proof of concept).
//!
//! Authorization is disabled unless [`init_authorization`] is called with some
//! [`AuthorizationSettings`]. When disabled, no token is required, emitted, or checked.
//!
//! When enabled:
//! - REST requests carry a biscuit token in the `Authorization: Bearer <token>` header. The token
//!   is validated and stored in the [`AUTHORIZATION_TOKEN`] inheritable task-local for the duration
//!   of the request.
//! - Generated gRPC clients forward the task-local token in the request metadata, or the node token
//!   when there is none (background tasks such as indexing pipelines or the janitor).
//! - Generated gRPC servers extract the token from the metadata and scope it again.
//! - The [`AuthorizationLayer`] checks every request against the token using the request's
//!   [`RequestFamily`] and the role rights from the node configuration.

mod authorizer;
mod context;
mod layer;
mod rest_layer;
mod state;
mod token;

pub use authorizer::{Authorization, RequestFamily, authorize_request};
pub use context::{
    AUTHORIZATION_TOKEN, build_tonic_request_with_auth_token, execute_with_authorization,
    extract_auth_token, inject_auth_token, spawn_inherit_auth_context,
};
pub use layer::{AuthorizationLayer, AuthorizationService};
pub use rest_layer::{
    AuthorizationTokenExtractionLayer, AuthorizationTokenExtractionService,
    check_rest_authorization_header,
};
pub use state::{AuthorizationSettings, init_authorization, is_authorization_enabled};
pub use token::{AuthorizationToken, UserTokenSpec, generate_root_key_pair, generate_token};

#[derive(
    thiserror::Error, Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationError {
    #[error("authorization token missing")]
    AuthorizationTokenMissing,
    #[error("invalid authorization token")]
    InvalidToken,
    #[error("permission denied")]
    PermissionDenied,
}

impl From<AuthorizationError> for tonic::Status {
    fn from(authorization_error: AuthorizationError) -> tonic::Status {
        match authorization_error {
            AuthorizationError::AuthorizationTokenMissing | AuthorizationError::InvalidToken => {
                tonic::Status::unauthenticated(authorization_error.to_string())
            }
            AuthorizationError::PermissionDenied => {
                tonic::Status::permission_denied(authorization_error.to_string())
            }
        }
    }
}
