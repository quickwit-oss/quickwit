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

use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use futures::{Future, ready};
use http::{Request, Response};
use pin_project::{pin_project, pinned_drop};
use prometheus::exponential_buckets;
use tonic::body::Body;
use tower::{Layer, Service};

use crate::metrics::{
    HistogramVec, IntCounterVec, IntGaugeVec, new_counter_vec, new_gauge_vec, new_histogram_vec,
};

fn extract_rpc_name_from_path(path: &str) -> &str {
    // gRPC paths are typically: /package.Service/Method
    if let Some(last_slash_pos) = path.rfind('/') {
        &path[last_slash_pos + 1..]
    } else {
        "unknown"
    }
}

fn get_content_length(headers: &http::HeaderMap) -> u64 {
    headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

#[derive(Clone)]
pub struct GrpcMetrics<S> {
    pub inner: S,
    requests_total: IntCounterVec<2>,
    requests_in_flight: IntGaugeVec<1>,
    request_duration_seconds: HistogramVec<2>,
    request_bytes_total: IntCounterVec<1>,
    response_bytes_total: IntCounterVec<1>,
}

impl<S> Debug for GrpcMetrics<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcMetrics").finish_non_exhaustive()
    }
}

impl<S> Service<Request<Body>> for GrpcMetrics<S>
where S: Service<Request<Body>, Response = Response<Body>>
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let start = Instant::now();

        let rpc_name = extract_rpc_name_from_path(request.uri().path()).to_string();

        let request_size = get_content_length(request.headers());
        if request_size > 0 {
            self.request_bytes_total
                .with_label_values([&rpc_name])
                .inc_by(request_size);
        }

        let inner = self.inner.call(request);

        self.requests_in_flight.with_label_values([&rpc_name]).inc();

        ResponseFuture {
            inner,
            start,
            rpc_name,
            status: "cancelled",
            requests_total: self.requests_total.clone(),
            requests_in_flight: self.requests_in_flight.clone(),
            request_duration_seconds: self.request_duration_seconds.clone(),
            response_bytes_total: self.response_bytes_total.clone(),
        }
    }
}

#[derive(Clone)]
pub struct GrpcMetricsLayer {
    requests_total: IntCounterVec<2>,
    requests_in_flight: IntGaugeVec<1>,
    request_duration_seconds: HistogramVec<2>,
    request_bytes_total: IntCounterVec<1>,
    response_bytes_total: IntCounterVec<1>,
}

impl GrpcMetricsLayer {
    pub fn new(subsystem: &'static str) -> Self {
        Self::new_inner(subsystem)
    }

    fn new_inner(subsystem: &str) -> Self {
        Self {
            requests_total: new_counter_vec(
                "grpc_requests_total",
                "Total number of gRPC requests processed.",
                subsystem,
                &[("kind", "server")],
                ["rpc", "status"],
            ),
            requests_in_flight: new_gauge_vec(
                "grpc_requests_in_flight",
                "Number of gRPC requests in-flight.",
                subsystem,
                &[("kind", "server")],
                ["rpc"],
            ),
            request_duration_seconds: new_histogram_vec(
                "grpc_request_duration_seconds",
                "Duration of request in seconds.",
                subsystem,
                &[("kind", "server")],
                ["rpc", "status"],
                exponential_buckets(0.001, 2.0, 12).unwrap(),
            ),
            request_bytes_total: new_counter_vec(
                "grpc_request_bytes_total",
                "Total bytes sent in gRPC requests.",
                subsystem,
                &[("kind", "server")],
                ["rpc"],
            ),
            response_bytes_total: new_counter_vec(
                "grpc_response_bytes_total",
                "Total bytes received in gRPC responses.",
                subsystem,
                &[("kind", "server")],
                ["rpc"],
            ),
        }
    }
}

#[cfg(any(test, feature = "testsuite"))]
impl GrpcMetricsLayer {
    pub fn for_test() -> Self {
        use rand::Rng;
        use rand::distributions::Alphanumeric;

        let slug: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(6)
            .map(char::from)
            .collect();
        Self::new_inner(&format!("test_grpc_{slug}"))
    }

    pub fn requests_total(&self, rpc: &str, status: &str) -> u64 {
        self.requests_total.with_label_values([rpc, status]).get()
    }

    pub fn request_bytes_total(&self, rpc: &str) -> u64 {
        self.request_bytes_total.with_label_values([rpc]).get()
    }

    pub fn response_bytes_total(&self, rpc: &str) -> u64 {
        self.response_bytes_total.with_label_values([rpc]).get()
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
            request_bytes_total: self.request_bytes_total.clone(),
            response_bytes_total: self.response_bytes_total.clone(),
        }
    }
}

/// Response future for [`GrpcMetrics`].
#[pin_project(PinnedDrop)]
pub struct ResponseFuture<F> {
    #[pin]
    inner: F,
    start: Instant,
    rpc_name: String,
    status: &'static str,
    requests_total: IntCounterVec<2>,
    requests_in_flight: IntGaugeVec<1>,
    request_duration_seconds: HistogramVec<2>,
    response_bytes_total: IntCounterVec<1>,
}

#[pinned_drop]
impl<F> PinnedDrop for ResponseFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let label_values = [&self.rpc_name, self.status];

        self.requests_total.with_label_values(label_values).inc();
        self.request_duration_seconds
            .with_label_values(label_values)
            .observe(elapsed);
        self.requests_in_flight
            .with_label_values([&self.rpc_name])
            .dec();
    }
}

impl<F, E> Future for ResponseFuture<F>
where F: Future<Output = Result<Response<Body>, E>>
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response: Result<Response<Body>, E> = ready!(this.inner.poll(cx));

        match &response {
            Ok(response) => {
                *this.status = "success";

                let response_size = get_content_length(response.headers());
                if response_size > 0 {
                    this.response_bytes_total
                        .with_label_values([&this.rpc_name])
                        .inc_by(response_size);
                }
            }
            Err(_) => {
                *this.status = "error";
            }
        }

        Poll::Ready(response)
    }
}

// TODO tests
