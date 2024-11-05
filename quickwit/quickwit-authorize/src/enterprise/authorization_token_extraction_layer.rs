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

use std::task::{Context, Poll};

use futures::future::Either;
use http::Request;
use tokio::task::futures::TaskLocalFuture;
use tokio_inherit_task_local::TaskLocalInheritableTable;
use tower::{Layer, Service};
use tracing::debug;

use super::AuthorizationToken;

#[derive(Clone, Copy, Debug)]
pub struct AuthorizationTokenExtractionLayer;

impl<S: Clone> Layer<S> for AuthorizationTokenExtractionLayer {
    type Service = AuthorizationTokenExtractionService<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthorizationTokenExtractionService { service }
    }
}

#[derive(Clone)]
pub struct AuthorizationTokenExtractionService<S> {
    service: S,
}

fn get_authorization_token_opt(headers: &http::HeaderMap) -> Option<AuthorizationToken> {
    let authorization_header_value = headers.get("Authorization")?;
    let authorization_header_str = authorization_header_value.to_str().ok()?;
    crate::get_auth_token_from_str(authorization_header_str).ok()
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
        let authorization_token_opt = get_authorization_token_opt(request.headers());
        debug!(authorization_token_opt = ?authorization_token_opt, "Authorization token extracted");
        let fut = self.service.call(request);
        if let Some(authorization_token) = authorization_token_opt {
            Either::Right(crate::execute_with_authorization(authorization_token, fut))
        } else {
            Either::Left(fut)
        }
    }
}
