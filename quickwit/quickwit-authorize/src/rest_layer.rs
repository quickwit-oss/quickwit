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

use std::task::{Context, Poll};

use futures::future::Either;
use http::Request;
use tokio::task::futures::TaskLocalFuture;
use tokio_inherit_task_local::TaskLocalInheritableTable;
use tower::{Layer, Service};

use crate::state::enabled_state;
use crate::{AuthorizationError, AuthorizationToken, execute_with_authorization};

/// Extracts the token from the `Authorization` header of HTTP requests and scopes it for the
/// duration of the request.
///
/// This layer never rejects a request: routes that require authentication must also check the
/// header with [`check_rest_authorization_header`]. Otherwise, a request without a valid token
/// runs with no token in context and its gRPC calls fall back to the node token.
#[derive(Clone, Copy, Debug, Default)]
pub struct AuthorizationTokenExtractionLayer;

impl<S> Layer<S> for AuthorizationTokenExtractionLayer {
    type Service = AuthorizationTokenExtractionService<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthorizationTokenExtractionService { service }
    }
}

#[derive(Clone, Debug)]
pub struct AuthorizationTokenExtractionService<S> {
    service: S,
}

fn parse_authorization_header(
    header_value_opt: Option<&str>,
) -> Result<Option<AuthorizationToken>, AuthorizationError> {
    let Some(state) = enabled_state() else {
        return Ok(None);
    };
    let header_value = header_value_opt.ok_or(AuthorizationError::AuthorizationTokenMissing)?;
    let token = AuthorizationToken::parse_header_value(header_value, state.root_public_key)?;
    Ok(Some(token))
}

/// Checks that the `Authorization` header holds a valid token when authorization is enabled.
pub fn check_rest_authorization_header(
    header_value_opt: Option<&str>,
) -> Result<(), AuthorizationError> {
    parse_authorization_header(header_value_opt).map(|_| ())
}

impl<B, S> Service<Request<B>> for AuthorizationTokenExtractionService<S>
where S: Service<Request<B>>
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Either<S::Future, TaskLocalFuture<TaskLocalInheritableTable, S::Future>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let header_value_opt = request
            .headers()
            .get(http::header::AUTHORIZATION)
            .and_then(|header_value| header_value.to_str().ok());
        let token_opt = parse_authorization_header(header_value_opt).unwrap_or(None);
        let future = self.service.call(request);
        execute_with_authorization(token_opt, future)
    }
}
