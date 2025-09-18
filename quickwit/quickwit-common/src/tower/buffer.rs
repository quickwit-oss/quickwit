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

use std::error::Error;
use std::marker::PhantomData;
use std::task::{Context, Poll};
use std::{error, fmt};

use futures::TryFutureExt as _;
use tower::buffer::Buffer as TowerBuffer;
use tower::buffer::error::{Closed, ServiceError};
use tower::{Layer, Service};

use super::{BoxError, BoxFuture};

#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error("the buffer's worker closed unexpectedly")]
    Closed,
    #[error("the buffer service returned an unknown error")]
    Unknown,
}

/// A wrapper around [`tower::buffer::Buffer`] service that preserves the original error type.
pub struct Buffer<S, R>
where S: Service<R>
{
    bound: usize,
    inner: TowerBuffer<R, <S as Service<R>>::Future>,
}

impl<S, R> Buffer<S, R>
where
    S: Service<R>,
    S::Error: Into<BoxError>,
{
    pub fn new(service: S, bound: usize) -> Self
    where
        S: Send + 'static,
        S::Future: Send,
        S::Error: Send + Sync,
        R: Send + 'static,
    {
        Self {
            bound,
            inner: TowerBuffer::new(service, bound),
        }
    }
}

impl<S, R> Service<R> for Buffer<S, R>
where
    R: Send + 'static,
    S: Service<R>,
    S::Error: error::Error + From<BufferError> + Into<BoxError> + Clone + Send + Sync + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(downcast_error)
    }

    fn call(&mut self, request: R) -> Self::Future {
        let fut = self.inner.call(request).map_err(downcast_error);
        Box::pin(fut)
    }
}

/// Downcasts an error boxed as [`tower::BoxError`] by the buffer service back into the original
/// error `E`.
fn downcast_error<E>(error: BoxError) -> E
where E: error::Error + From<BufferError> + Clone + 'static {
    if let Some(error) = error.downcast_ref::<E>() {
        return error.clone();
    }
    // This happens when the buffer worker is dead.
    if error.downcast_ref::<Closed>().is_some() {
        return BufferError::Closed.into();
    }
    // This happens when the inner service returns an error on `poll_ready`.
    if let Some(service_error) = error.downcast_ref::<ServiceError>()
        && let Some(source) = service_error.source()
        && let Some(inner) = source.downcast_ref::<E>()
    {
        return inner.clone();
    }
    // This will happen only if the buffer service implementation adds a new error type.
    BufferError::Unknown.into()
}

impl<S, R> fmt::Debug for Buffer<S, R>
where S: Service<R>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Buffer")
            .field("bound", &self.bound)
            .finish()
    }
}

impl<S, R> Clone for Buffer<S, R>
where
    S: Service<R>,
    R: Send + 'static,
    <S as Service<R>>::Future: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            bound: self.bound,
            inner: self.inner.clone(),
        }
    }
}

pub struct BufferLayer<R> {
    bound: usize,
    _phantom: PhantomData<fn(R)>,
}

impl<R> BufferLayer<R> {
    pub fn new(bound: usize) -> Self {
        Self {
            bound,
            _phantom: PhantomData,
        }
    }
}

impl<S, R> Layer<S> for BufferLayer<R>
where
    S: Service<R> + Send + 'static,
    S::Future: Send,
    S::Error: error::Error + From<BufferError> + Into<BoxError> + Clone + Send + Sync + 'static,
    R: Send + 'static,
{
    type Service = Buffer<S, R>;

    fn layer(&self, service: S) -> Self::Service {
        Buffer::new(service, self.bound)
    }
}

impl<R> fmt::Debug for BufferLayer<R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BufferLayer")
            .field("bound", &self.bound)
            .finish()
    }
}

impl<R> Clone for BufferLayer<R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R> Copy for BufferLayer<R> {}

#[cfg(test)]
mod tests {
    use tower::ServiceExt;

    use super::*;

    #[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
    enum MyServiceError {
        #[error("service is exhausted")]
        Exhausted,
        #[error("service is unavailable")]
        Unavailable,
        #[error("service attempted to divide by zero")]
        ZeroDivision,
    }

    impl From<BufferError> for MyServiceError {
        fn from(_: BufferError) -> Self {
            MyServiceError::Unavailable
        }
    }

    #[derive(Debug, Default)]
    struct MyService {
        num_calls: usize,
    }

    impl Service<(usize, usize)> for MyService {
        type Response = usize;
        type Error = MyServiceError;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.num_calls += 1;

            if self.num_calls > 2 {
                Poll::Ready(Err(MyServiceError::Exhausted))
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn call(&mut self, (dividend, divisor): (usize, usize)) -> Self::Future {
            let fut = async move {
                if divisor == 0 {
                    Err(MyServiceError::ZeroDivision)
                } else {
                    Ok(dividend / divisor)
                }
            };
            Box::pin(fut)
        }
    }

    #[tokio::test]
    async fn test_buffer_error() {
        let mut service = BufferLayer::new(1).layer(MyService::default());

        assert_eq!(
            service.ready().await.unwrap().call((10, 2)).await.unwrap(),
            5
        );
        assert_eq!(
            service
                .ready()
                .await
                .unwrap()
                .call((10, 0))
                .await
                .unwrap_err(),
            MyServiceError::ZeroDivision
        );
        assert_eq!(
            service
                .ready()
                .await
                .unwrap()
                .call((10, 0))
                .await
                .unwrap_err(),
            MyServiceError::Exhausted
        );
    }

    #[tokio::test]
    async fn test_buffer_closed() {
        let (inner, worker) = TowerBuffer::pair(MyService::default(), 1);
        let handle = tokio::spawn(worker);

        let mut service: Buffer<MyService, (usize, usize)> = Buffer { bound: 1, inner };
        let res: usize = service.ready().await.unwrap().call((10, 2)).await.unwrap();
        assert_eq!(res, 5);

        handle.abort();
        handle.await.unwrap_err();

        assert_eq!(
            service.ready().await.unwrap_err(),
            MyServiceError::Unavailable
        );
    }
}
