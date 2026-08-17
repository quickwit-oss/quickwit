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
use quickwit_metrics::{
    Counter, Gauge, Histogram, Labels, LazyCounter, LazyGauge, LazyHistogram, counter, gauge,
    histogram, labels, lazy_counter, lazy_gauge, lazy_histogram,
};
use tower::{Layer, Service};

use crate::metrics::exponential_buckets;

pub trait RpcName {
    fn rpc_name() -> &'static str;
}

/// Returns the gRPC status code associated with a service error.
pub trait GrpcStatusCode {
    fn grpc_status_code(&self) -> tonic::Code;
}

impl GrpcStatusCode for tonic::Status {
    fn grpc_status_code(&self) -> tonic::Code {
        self.code()
    }
}

impl GrpcStatusCode for std::convert::Infallible {
    fn grpc_status_code(&self) -> tonic::Code {
        match *self {}
    }
}

fn grpc_code_label(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "ok",
        tonic::Code::Cancelled => "cancelled",
        tonic::Code::Unknown => "unknown",
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::DeadlineExceeded => "deadline_exceeded",
        tonic::Code::NotFound => "not_found",
        tonic::Code::AlreadyExists => "already_exists",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::FailedPrecondition => "failed_precondition",
        tonic::Code::Aborted => "aborted",
        tonic::Code::OutOfRange => "out_of_range",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Internal => "internal",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::DataLoss => "data_loss",
        tonic::Code::Unauthenticated => "unauthenticated",
    }
}

static GRPC_REQUESTS_TOTAL: LazyCounter = lazy_counter!(
        name: "requests_total",
        description: "Total number of gRPC requests processed.",
        subsystem: "grpc",
);

static GRPC_REQUESTS_IN_FLIGHT: LazyGauge = lazy_gauge!(
        name: "requests_in_flight",
        description: "Number of gRPC requests in-flight.",
        subsystem: "grpc",
);

static GRPC_REQUEST_DURATION_SECONDS: LazyHistogram = lazy_histogram!(
        name: "request_duration_seconds",
        description: "Duration of request in seconds.",
        subsystem: "grpc",
        buckets: exponential_buckets(0.001, 2.0, 12).unwrap(),
);

#[derive(Clone)]
pub struct GrpcMetrics<S> {
    inner: S,
    requests_total: Counter,
    requests_in_flight: Gauge,
    request_duration_seconds: Histogram,
}

impl<S, R> Service<R> for GrpcMetrics<S>
where
    S: Service<R>,
    S::Error: GrpcStatusCode,
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

        gauge!(
            parent: self.requests_in_flight,
            "rpc" => rpc_name,
        )
        .inc();

        ResponseFuture {
            inner,
            start,
            rpc_name,
            status: "cancelled",
            code: "cancelled",
            requests_total: self.requests_total.clone(),
            requests_in_flight: self.requests_in_flight.clone(),
            request_duration_seconds: self.request_duration_seconds.clone(),
        }
    }
}

#[derive(Clone)]
pub struct GrpcMetricsLayer {
    requests_total: Counter,
    requests_in_flight: Gauge,
    request_duration_seconds: Histogram,
}

impl GrpcMetricsLayer {
    pub fn new(subsystem: &'static str, kind: &'static str) -> Self {
        let labels = Self::default_labels(subsystem, kind);
        Self {
            requests_total: counter!(parent: GRPC_REQUESTS_TOTAL, labels: [labels]),
            requests_in_flight: gauge!(parent: GRPC_REQUESTS_IN_FLIGHT, labels: [labels]),
            request_duration_seconds: histogram!(parent: GRPC_REQUEST_DURATION_SECONDS, labels: [labels]),
        }
    }

    pub fn new_with_labels<const N: usize>(
        subsystem: &'static str,
        kind: &'static str,
        extra_labels: Labels<N>,
    ) -> Self {
        let labels = Self::default_labels(subsystem, kind);
        Self {
            requests_total: counter!(parent: GRPC_REQUESTS_TOTAL, labels: [labels, extra_labels]),
            requests_in_flight: gauge!(parent: GRPC_REQUESTS_IN_FLIGHT, labels: [labels, extra_labels]),
            request_duration_seconds: histogram!(parent: GRPC_REQUEST_DURATION_SECONDS, labels: [labels, extra_labels]),
        }
    }

    fn default_labels(subsystem: &'static str, kind: &'static str) -> Labels<3> {
        // `service` is kept for backward compatibility with existing consumers. Prefer
        // `grpc_service` for new consumers.
        // TODO: Remove `service` in a future breaking release.
        labels!("service" => subsystem, "grpc_service" => subsystem, "kind" => kind)
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

/// Response future for [`GrpcMetrics`].
#[pin_project(PinnedDrop)]
pub struct ResponseFuture<F> {
    #[pin]
    inner: F,
    start: Instant,
    rpc_name: &'static str,
    // Should have been called `result` or `outcome` but here we are.
    status: &'static str,
    code: &'static str,
    requests_total: Counter,
    requests_in_flight: Gauge,
    request_duration_seconds: Histogram,
}

#[pinned_drop]
impl<F> PinnedDrop for ResponseFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let rpc_label = labels!("rpc" => self.rpc_name);
        let status_label = labels!("status" => self.status);
        let code_label = labels!("code" => self.code);
        counter!(parent: self.requests_total, labels: [rpc_label, status_label, code_label]).inc();
        histogram!(parent: self.request_duration_seconds, labels: [rpc_label, status_label, code_label])
            .observe(elapsed);
        gauge!(parent: self.requests_in_flight, labels: [rpc_label]).dec();
    }
}

impl<F, T, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<T, E>>,
    E: GrpcStatusCode,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.inner.poll(cx));
        match &response {
            Ok(_) => {
                *this.status = "success";
                *this.code = "ok";
            }
            Err(error) => {
                *this.status = "error";
                *this.code = grpc_code_label(error.grpc_status_code());
            }
        }
        Poll::Ready(Ok(response?))
    }
}

#[cfg(test)]
mod tests {
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;

    #[derive(Debug)]
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

    #[test]
    fn test_grpc_metrics() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let primary_layer = GrpcMetricsLayer::new_with_labels(
                    "quickwit_test",
                    "server",
                    labels!("metastore_kind" => "primary", "test_label" => "test"),
                );
                let read_replica_layer = GrpcMetricsLayer::new_with_labels(
                    "quickwit_test",
                    "server",
                    labels!("metastore_kind" => "read_replica", "test_label" => "test"),
                );

                let mut hello_service = primary_layer.clone().layer(tower::service_fn(
                    |request: HelloRequest| async move { Ok::<_, tonic::Status>(request) },
                ));
                let mut goodbye_service = primary_layer.clone().layer(tower::service_fn(
                    |request: GoodbyeRequest| async move { Ok::<_, tonic::Status>(request) },
                ));
                let mut read_replica_service = read_replica_layer.layer(tower::service_fn(
                    |request: HelloRequest| async move { Ok::<_, tonic::Status>(request) },
                ));
                let mut failing_service =
                    primary_layer.layer(tower::service_fn(|_request: HelloRequest| async move {
                        Err::<HelloRequest, _>(tonic::Status::not_found("not found"))
                    }));

                hello_service.call(HelloRequest).await.unwrap();
                goodbye_service.call(GoodbyeRequest).await.unwrap();
                read_replica_service.call(HelloRequest).await.unwrap();
                failing_service.call(HelloRequest).await.unwrap_err();

                let hello_future = hello_service.call(HelloRequest);
                drop(hello_future);
            });
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let counter_value = |rpc: &str, status: &str, code: &str, metastore_kind: &str| {
            snapshot.iter().find_map(|(composite_key, _, _, value)| {
                let (_, key) = composite_key.clone().into_parts();
                let labels = key
                    .labels()
                    .map(|label| (label.key(), label.value()))
                    .collect::<Vec<_>>();
                if key.name() == "quickwit_grpc_requests_total"
                    && labels.contains(&("service", "quickwit_test"))
                    && labels.contains(&("kind", "server"))
                    && labels.contains(&("metastore_kind", metastore_kind))
                    && labels.contains(&("test_label", "test"))
                    && labels.contains(&("rpc", rpc))
                    && labels.contains(&("status", status))
                    && labels.contains(&("code", code))
                {
                    Some(value)
                } else {
                    None
                }
            })
        };
        assert_eq!(
            counter_value("hello", "success", "ok", "primary"),
            Some(&DebugValue::Counter(1))
        );
        assert_eq!(
            counter_value("goodbye", "success", "ok", "primary"),
            Some(&DebugValue::Counter(1))
        );
        assert_eq!(
            counter_value("hello", "cancelled", "cancelled", "primary"),
            Some(&DebugValue::Counter(1))
        );
        assert_eq!(
            counter_value("hello", "success", "ok", "read_replica"),
            Some(&DebugValue::Counter(1))
        );
        assert_eq!(
            counter_value("hello", "error", "not_found", "primary"),
            Some(&DebugValue::Counter(1))
        );
    }
}
