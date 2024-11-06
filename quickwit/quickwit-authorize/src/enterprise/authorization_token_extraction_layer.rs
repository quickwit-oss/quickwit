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
