// The Quickwit Enterprise Edition (EE) license
// Copyright (c) 2024-present Quickwit Inc.
//
// With regard to the Quickwit Software:
//
// This software and associated documentation files (the "Software") may only be
// used in production, if you (and any entity that you represent) hold a valid
// Quickwit Enterprise license corresponding to your usage.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For all third party components incorporated into the Quickwit Software, those
// components are licensed under the original license provided by the owner of the
// applicable component.

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
