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

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use futures::{ready, Future};
use pin_project::{pin_project, pinned_drop};
use tower::{Layer, Service};

use crate::metrics::{
    new_counter_vec, new_gauge_vec, new_histogram_vec, HistogramVec, IntCounterVec, IntGaugeVec,
};

pub trait RpcName {
    fn rpc_name() -> &'static str;
}

#[derive(Clone)]
pub struct GrpcMetrics<S> {
    inner: S,
    requests_total: IntCounterVec<2>,
    requests_in_flight: IntGaugeVec<1>,
    request_duration_seconds: HistogramVec<2>,
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

        self.requests_in_flight.with_label_values([rpc_name]).inc();

        ResponseFuture {
            inner,
            start,
            rpc_name,
            status: "canceled",
            requests_total: self.requests_total.clone(),
            requests_in_flight: self.requests_in_flight.clone(),
            request_duration_seconds: self.request_duration_seconds.clone(),
        }
    }
}

#[derive(Clone)]
pub struct GrpcMetricsLayer {
    requests_total: IntCounterVec<2>,
    requests_in_flight: IntGaugeVec<1>,
    request_duration_seconds: HistogramVec<2>,
}

impl GrpcMetricsLayer {
    pub fn new(subsystem: &'static str, kind: &'static str) -> Self {
        Self {
            requests_total: new_counter_vec(
                "grpc_requests_total",
                "Total number of gRPC requests processed.",
                subsystem,
                &[("kind", kind)],
                ["rpc", "status"],
            ),
            requests_in_flight: new_gauge_vec(
                "grpc_requests_in_flight",
                "Number of gRPC requests in-flight.",
                subsystem,
                &[("kind", kind)],
                ["rpc"],
            ),
            request_duration_seconds: new_histogram_vec(
                "grpc_request_duration_seconds",
                "Duration of request in seconds.",
                subsystem,
                &[("kind", kind)],
                ["rpc", "status"],
            ),
        }
    }
}

impl<S> Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetrics<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetrics {
            inner,
            requests_total: self.requests_total.clone(),
            requests_in_flight: self.requests_in_flight.clone(),
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
    rpc_name: &'static str,
    status: &'static str,
    requests_total: IntCounterVec<2>,
    requests_in_flight: IntGaugeVec<1>,
    request_duration_seconds: HistogramVec<2>,
}

#[pinned_drop]
impl<F> PinnedDrop for ResponseFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let label_values = [self.rpc_name, self.status];

        self.requests_total.with_label_values(label_values).inc();
        self.request_duration_seconds
            .with_label_values(label_values)
            .observe(elapsed);
        self.requests_in_flight
            .with_label_values([self.rpc_name])
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
                .with_label_values(["hello", "success"])
                .get(),
            1
        );
        assert_eq!(
            layer
                .requests_total
                .with_label_values(["goodbye", "success"])
                .get(),
            0
        );

        goodbye_service.call(GoodbyeRequest).await.unwrap();

        assert_eq!(
            layer
                .requests_total
                .with_label_values(["goodbye", "success"])
                .get(),
            1
        );

        let hello_future = hello_service.call(HelloRequest);
        drop(hello_future);

        assert_eq!(
            layer
                .requests_total
                .with_label_values(["hello", "canceled"])
                .get(),
            1
        );
    }
}
