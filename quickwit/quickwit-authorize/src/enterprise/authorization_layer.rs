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

use std::fmt;
use std::task::{Context, Poll};

use futures::future::Either;
use quickwit_common::tower::RpcName;
use tower::{Layer, Service};

use crate::AuthorizationError;

#[derive(Clone, Copy, Debug)]
pub struct AuthorizationLayer;

impl<S: Clone> Layer<S> for AuthorizationLayer {
    type Service = AuthorizationService<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthorizationService { service }
    }
}

#[derive(Clone)]
pub struct AuthorizationService<S> {
    service: S,
}

impl<S, Request> Service<Request> for AuthorizationService<S>
where
    S: Service<Request>,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: From<AuthorizationError> + Send + 'static,
    Request: fmt::Debug + Send + RpcName + crate::Authorization + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        futures::future::Either<futures::future::Ready<Result<S::Response, S::Error>>, S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        if let Err(authorization_err) = crate::authorize_request(&request) {
            let err = S::Error::from(authorization_err);
            let result: Result<S::Response, S::Error> = Err(err);
            return Either::Left(futures::future::ready(result));
        }
        let service_fut = self.service.call(request);
        Either::Right(service_fut)
    }
}
