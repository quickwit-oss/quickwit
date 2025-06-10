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
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use pin_project::pin_project;
use tokio::time::Sleep;
use tower::{Layer, Service};

#[derive(Debug, Clone)]
pub struct Timeout<S> {
    service: S,
    timeout: Duration,
}
impl<S> Timeout<S> {
    /// Creates a new [`Timeout`]
    pub fn new(service: S, timeout: Duration) -> Self {
        Timeout { service, timeout }
    }
}

impl<S, R> Service<R> for Timeout<S>
where
    S: Service<R>,
    S::Error: From<TimeoutExceeded>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = TimeoutFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> Self::Future {
        TimeoutFuture {
            inner: self.service.call(request),
            sleep: tokio::time::sleep(self.timeout),
        }
    }
}

/// The error type for the `Timeout` service.
#[derive(Debug, PartialEq, Eq)]
pub struct TimeoutExceeded;

#[pin_project]
#[derive(Debug)]
pub struct TimeoutFuture<F> {
    #[pin]
    inner: F,
    #[pin]
    sleep: Sleep,
}

impl<F, T, E> Future for TimeoutFuture<F>
where
    F: Future<Output = Result<T, E>>,
    E: From<TimeoutExceeded>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.inner.poll(cx) {
            Poll::Ready(v) => return Poll::Ready(v),
            Poll::Pending => {}
        }

        // Now check the timeout
        match this.sleep.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Err(TimeoutExceeded.into())),
        }
    }
}

/// This is similar to tower's Timeout Layer except it requires
/// the error of the service to implement `From<TimeoutExceeded>`.
///
/// If the inner service does not complete within the specified duration,
/// the response will be aborted with the error `TimeoutExceeded`.
///
/// Note that when used in combination with a retry layer, this should be
/// stacked on top of it for the timeout to be retried.
#[derive(Debug, Clone)]
pub struct TimeoutLayer {
    timeout: Duration,
}

impl TimeoutLayer {
    /// Creates a new `TimeoutLayer` with the specified delay.
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

impl<S> Layer<S> for TimeoutLayer {
    type Service = Timeout<S>;

    fn layer(&self, service: S) -> Self::Service {
        Timeout::new(service, self.timeout)
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::Duration;
    use tower::{ServiceBuilder, ServiceExt};

    use super::*;

    #[tokio::test]
    async fn test_timeout() {
        let delay = Duration::from_millis(100);
        let mut service = ServiceBuilder::new()
            .layer(TimeoutLayer::new(delay))
            .service_fn(|_| async {
                // sleep for 1 sec
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok::<_, TimeoutExceeded>(())
            });

        let res = service.ready().await.unwrap().call(()).await;
        assert_eq!(res, Err(TimeoutExceeded));
    }
}
