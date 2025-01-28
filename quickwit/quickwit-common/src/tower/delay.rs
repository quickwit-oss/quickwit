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

/// Delays a request by `delay` seconds.
#[derive(Debug, Clone)]
pub struct Delay<S> {
    inner: S,
    delay: Duration,
}

impl<S, R> Service<R> for Delay<S>
where S: Service<R>
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = DelayFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> Self::Future {
        DelayFuture {
            inner: self.inner.call(request),
            sleep: tokio::time::sleep(self.delay),
            slept: false,
        }
    }
}

#[pin_project]
#[derive(Debug)]
pub struct DelayFuture<F> {
    #[pin]
    inner: F,
    #[pin]
    sleep: Sleep,
    slept: bool,
}

impl<F, T, E> Future for DelayFuture<F>
where F: Future<Output = Result<T, E>>
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if !*this.slept {
            match this.sleep.poll(cx) {
                Poll::Ready(_) => *this.slept = true,
                Poll::Pending => return Poll::Pending,
            }
        }
        this.inner.poll(cx)
    }
}

/// Applies a delay to requests via the supplied inner service.
#[derive(Debug, Clone)]
pub struct DelayLayer {
    delay: Duration,
}

impl DelayLayer {
    /// Creates a new `DelayLayer` with the specified delay.
    pub fn new(delay: Duration) -> Self {
        Self { delay }
    }
}

impl<S> Layer<S> for DelayLayer {
    type Service = Delay<S>;

    fn layer(&self, service: S) -> Self::Service {
        Delay {
            inner: service,
            delay: self.delay,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use tokio::time::Duration;
    use tower::{ServiceBuilder, ServiceExt};

    use super::*;

    #[tokio::test]
    async fn test_delay() {
        let delay = Duration::from_millis(100);
        let mut service = ServiceBuilder::new()
            .layer(DelayLayer::new(delay))
            .service_fn(|_| async { Ok::<_, ()>(()) });

        let start = Instant::now();
        service.ready().await.unwrap().call(()).await.unwrap();

        let elapsed = start.elapsed();
        assert!(elapsed >= delay);
    }
}
