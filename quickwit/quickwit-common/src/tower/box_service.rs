// Copyright (C) 2023 Quickwit, Inc.
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
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Future;
use tower::{Service, ServiceExt};

pub type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

trait CloneService<R, T, E>:
    Service<R, Response = T, Error = E, Future = BoxFuture<T, E>>
    + dyn_clone::DynClone
    + Send
    + Sync
    + 'static
{
}

dyn_clone::clone_trait_object!(<R, T, E> CloneService<R, T, E>);

impl<S, R, T, E> CloneService<R, T, E> for S where S: Service<R, Response = T, Error = E, Future = BoxFuture<T, E>>
        + Clone
        + Send
        + Sync
        + 'static
{
}

pub struct BoxService<R, T, E> {
    inner: Box<dyn CloneService<R, T, E>>,
}

impl<R, T, E> Clone for BoxService<R, T, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<R, T, E> BoxService<R, T, E> {
    pub fn new<S>(inner: S) -> Self
    where
        S: Service<R, Response = T, Error = E> + Clone + Send + Sync + 'static,
        S::Future: Send + 'static,
    {
        let inner = Box::new(inner.map_future(|fut| Box::pin(fut) as _));
        BoxService { inner }
    }
}

impl<R, T, E> Service<R> for BoxService<R, T, E> {
    type Response = T;
    type Error = E;
    type Future = BoxFuture<T, E>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> BoxFuture<T, E> {
        self.inner.call(request)
    }
}

impl<T, U, E> fmt::Debug for BoxService<T, U, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("BoxService").finish()
    }
}
