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
use std::sync::LazyLock;
use std::task::{Context, Poll};
use std::time::Instant;

use futures::{Future, ready};
use pin_project::{pin_project, pinned_drop};
use tower::{Layer, Service};

use crate::define_histogram_vec;
use crate::metrics::{
    HistogramVec, IntCounterVec, IntGaugeVec, exponential_buckets, new_counter_vec, new_gauge_vec,
};

pub trait RpcName {
    fn rpc_name() -> &'static str;
}

/// Shared gRPC request metrics. `service` distinguishes Quickwit subsystems
/// (`control_plane`, `indexing`, `ingest`, `metastore`, `cluster`, ...) and `kind` is
/// `"client"` or `"server"`.
pub static GRPC_REQUESTS_TOTAL: LazyLock<IntCounterVec<4>> = LazyLock::new(|| {
    new_counter_vec(
        "grpc_requests_total",
        "Total number of gRPC requests processed.",
        "",
        &[],
        ["service", "kind", "rpc", "status"],
    )
});

pub static GRPC_REQUESTS_IN_FLIGHT: LazyLock<IntGaugeVec<3>> = LazyLock::new(|| {
    new_gauge_vec(
        "grpc_requests_in_flight",
        "Number of gRPC requests in-flight.",
        "",
        &[],
        ["service", "kind", "rpc"],
    )
});

define_histogram_vec! {
    GRPC_REQUEST_DURATION_SECONDS,
    name: "grpc_request_duration_seconds",
    help: "Duration of gRPC request in seconds.",
    subsystem: "",
    const_labels: [],
    labels: ["service", "kind", "rpc", "status"],
    buckets: exponential_buckets(0.001, 2.0, 12).unwrap(),
}

#[derive(Clone)]
pub struct GrpcMetrics<S> {
    inner: S,
    service: &'static str,
    kind: &'static str,
    requests_total: IntCounterVec<4>,
    requests_in_flight: IntGaugeVec<3>,
    request_duration_seconds: HistogramVec<4>,
}

impl<S, R> Service<R> for GrpcMetrics<S>
where
    S: Service<R>,
    R: RpcName,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> Self::Future {
        let start = Instant::now();
        let rpc_name = R::rpc_name();
        let inner = self.inner.call(request);

        self.requests_in_flight
            .with_label_values([self.service, self.kind, rpc_name])
            .inc();

        ResponseFuture {
            inner,
            start,
            service: self.service,
            kind: self.kind,
            rpc_name,
            status: "cancelled",
            requests_total: self.requests_total.clone(),
            requests_in_flight: self.requests_in_flight.clone(),
            request_duration_seconds: self.request_duration_seconds.clone(),
        }
    }
}

#[derive(Clone)]
pub struct GrpcMetricsLayer {
    service: &'static str,
    kind: &'static str,
    requests_total: IntCounterVec<4>,
    requests_in_flight: IntGaugeVec<3>,
    request_duration_seconds: HistogramVec<4>,
}

impl GrpcMetricsLayer {
    pub fn new(service: &'static str, kind: &'static str) -> Self {
        Self {
            service,
            kind,
            requests_total: GRPC_REQUESTS_TOTAL.clone(),
            requests_in_flight: GRPC_REQUESTS_IN_FLIGHT.clone(),
            request_duration_seconds: GRPC_REQUEST_DURATION_SECONDS.clone(),
        }
    }
}

impl<S> Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetrics<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetrics {
            inner,
            service: self.service,
            kind: self.kind,
            requests_total: self.requests_total.clone(),
            requests_in_flight: self.requests_in_flight.clone(),
            request_duration_seconds: self.request_duration_seconds.clone(),
        }
    }
}

/// Response future for [`GrpcMetrics`].
#[pin_project(PinnedDrop)]
pub struct ResponseFuture<F> {
    #[pin]
    inner: F,
    start: Instant,
    service: &'static str,
    kind: &'static str,
    rpc_name: &'static str,
    status: &'static str,
    requests_total: IntCounterVec<4>,
    requests_in_flight: IntGaugeVec<3>,
    request_duration_seconds: HistogramVec<4>,
}

#[pinned_drop]
impl<F> PinnedDrop for ResponseFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let label_values = [self.service, self.kind, self.rpc_name, self.status];

        self.requests_total.with_label_values(label_values).inc();
        self.request_duration_seconds
            .with_label_values(label_values)
            .observe(elapsed);
        self.requests_in_flight
            .with_label_values([self.service, self.kind, self.rpc_name])
            .dec();
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

#[cfg(test)]
mod tests {
    use super::*;

    struct HelloRequest;

    impl RpcName for HelloRequest {
        fn rpc_name() -> &'static str {
            "hello"
        }
    }

    struct GoodbyeRequest;

    impl RpcName for GoodbyeRequest {
        fn rpc_name() -> &'static str {
            "goodbye"
        }
    }

    #[tokio::test]
    async fn test_grpc_metrics() {
        let layer = GrpcMetricsLayer::new("quickwit_test", "server");

        let mut hello_service =
            layer
                .clone()
                .layer(tower::service_fn(|request: HelloRequest| async move {
                    Ok::<_, ()>(request)
                }));
        let mut goodbye_service =
            layer
                .clone()
                .layer(tower::service_fn(|request: GoodbyeRequest| async move {
                    Ok::<_, ()>(request)
                }));

        hello_service.call(HelloRequest).await.unwrap();

        assert_eq!(
            layer
                .requests_total
                .with_label_values(["quickwit_test", "server", "hello", "success"])
                .get(),
            1
        );
        assert_eq!(
            layer
                .requests_total
                .with_label_values(["quickwit_test", "server", "goodbye", "success"])
                .get(),
            0
        );

        goodbye_service.call(GoodbyeRequest).await.unwrap();

        assert_eq!(
            layer
                .requests_total
                .with_label_values(["quickwit_test", "server", "goodbye", "success"])
                .get(),
            1
        );

        let hello_future = hello_service.call(HelloRequest);
        drop(hello_future);

        assert_eq!(
            layer
                .requests_total
                .with_label_values(["quickwit_test", "server", "hello", "cancelled"])
                .get(),
            1
        );
    }
}
