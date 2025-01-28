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

use std::fmt;
use std::task::{Context, Poll};

use tower::{Service, ServiceExt};

use super::BoxFuture;

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
