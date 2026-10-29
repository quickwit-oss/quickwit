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

use std::future::Future;

use futures::future::Either;
use tokio::task::JoinHandle;
use tokio::task::futures::TaskLocalFuture;
use tokio_inherit_task_local::{FutureInheritTaskLocal, TaskLocalInheritableTable};
use tracing::error;

use crate::state::enabled_state;
use crate::{AuthorizationError, AuthorizationToken};

tokio_inherit_task_local::inheritable_task_local! {
    /// Token of the user on behalf of whom the current task runs.
    ///
    /// Only tasks spawned with [`spawn_inherit_auth_context`] (or futures wrapped with
    /// `inherit_task_local()`) inherit it.
    pub static AUTHORIZATION_TOKEN: AuthorizationToken;
}

/// Wraps a request into a `tonic::Request`, adding the token of the current task (or the node
/// token if there is none) to its metadata. No-op when authorization is disabled.
pub fn build_tonic_request_with_auth_token<R>(request: R) -> tonic::Request<R> {
    let mut tonic_request = tonic::Request::new(request);
    inject_auth_token(tonic_request.metadata_mut());
    tonic_request
}

/// Adds the token of the current task (or the node token if there is none) to the metadata of an
/// outgoing gRPC request. No-op when authorization is disabled.
pub fn inject_auth_token(metadata: &mut tonic::metadata::MetadataMap) {
    let Some(state) = enabled_state() else {
        return;
    };
    let token = AUTHORIZATION_TOKEN
        .try_with(|token| token.clone())
        .unwrap_or_else(|_| state.node_token.clone());
    let header_value_res = token.to_header_value().and_then(|header_value| {
        header_value
            .parse()
            .map_err(|_| AuthorizationError::InvalidToken)
    });
    match header_value_res {
        Ok(header_value) => {
            metadata.insert(http::header::AUTHORIZATION.as_str(), header_value);
        }
        Err(error) => {
            // The server will reject the request as unauthenticated.
            error!(%error, "failed to serialize authorization token");
        }
    }
}

/// Extracts and verifies the token from the metadata of an incoming gRPC request.
///
/// Returns `Ok(None)` when authorization is disabled, and an error if it is enabled and the token
/// is missing or invalid.
pub fn extract_auth_token(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<Option<AuthorizationToken>, tonic::Status> {
    let Some(state) = enabled_state() else {
        return Ok(None);
    };
    let header_value = metadata
        .get(http::header::AUTHORIZATION.as_str())
        .ok_or(AuthorizationError::AuthorizationTokenMissing)?
        .to_str()
        .map_err(|_| AuthorizationError::InvalidToken)?;
    let token = AuthorizationToken::parse_header_value(header_value, state.root_public_key)?;
    Ok(Some(token))
}

/// Runs `future` with `token_opt` as the token of the current task.
pub fn execute_with_authorization<F: Future>(
    token_opt: Option<AuthorizationToken>,
    future: F,
) -> Either<F, TaskLocalFuture<TaskLocalInheritableTable, F>> {
    match token_opt {
        Some(token) => Either::Right(AUTHORIZATION_TOKEN.scope(token, future)),
        None => Either::Left(future),
    }
}

/// Like `tokio::spawn`, but the spawned task inherits the authorization token of the caller.
pub fn spawn_inherit_auth_context<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future.inherit_task_local())
}
