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

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use futures::{Future, ready};
use pin_project::{pin_project, pinned_drop};
use tower::{Layer, Service};

use crate::dd_metrics::{DDCounters, DDHistograms};

#[derive(Clone)]
pub struct DDGrpcMetrics<S> {
    inner: S,
    requests_total: DDCounters,
    request_duration_seconds: DDHistograms,
}

impl<S, R> Service<R> for DDGrpcMetrics<S>
where S: Service<R>
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> Self::Future {
        let start = Instant::now();
        let inner = self.inner.call(request);

        ResponseFuture {
            inner,
            start,
            status: "cancelled",
            requests_total: self.requests_total.clone(),
            request_duration_seconds: self.request_duration_seconds.clone(),
        }
    }
}

#[derive(Clone)]
pub struct DDGrpcMetricsLayer {
    requests_total: DDCounters,
    request_duration_seconds: DDHistograms,
}

impl DDGrpcMetricsLayer {
    pub fn for_metastore() -> Self {
        Self {
            requests_total: DDCounters::new(
                "metastore_requests.count",
                "status",
                &["cancelled", "success", "error"],
                &[],
            ),
            request_duration_seconds: DDHistograms::new(
                "metastore_requests.duration_seconds",
                "status",
                &["cancelled", "success", "error"],
                &[],
            ),
        }
    }
}

impl<S> Layer<S> for DDGrpcMetricsLayer {
    type Service = DDGrpcMetrics<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DDGrpcMetrics {
            inner,
            requests_total: self.requests_total.clone(),
            request_duration_seconds: self.request_duration_seconds.clone(),
        }
    }
}

/// Response future for [`PrometheusMetrics`].
#[pin_project(PinnedDrop)]
pub struct ResponseFuture<F> {
    #[pin]
    inner: F,
    start: Instant,
    status: &'static str,
    requests_total: DDCounters,
    request_duration_seconds: DDHistograms,
}

#[pinned_drop]
impl<F> PinnedDrop for ResponseFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        let elapsed = self.start.elapsed().as_secs_f64();
        self.requests_total.get(self.status).increment(1);
        self.request_duration_seconds
            .get(self.status)
            .record(elapsed);
    }
}

impl<F, T, E> Future for ResponseFuture<F>
where F: Future<Output = Result<T, E>>
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.inner.poll(cx));
        *this.status = if response.is_ok() { "success" } else { "error" };
        Poll::Ready(Ok(response?))
    }
}
