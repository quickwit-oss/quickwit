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

use std::any::type_name;
use std::fmt;

use quickwit_metrics::{Counter, counter, labels};
use tokio::time::Sleep;
use tower::Layer;
use tower::retry::{Policy, Retry};
use tracing::debug;

use super::metrics::{GrpcStatusCode, RpcName, grpc_code_label};
use crate::retry::{RetryParams, Retryable};

/// Retry layer copy/pasted from `tower::retry::RetryLayer`
/// but which implements `Clone`.
impl<P, S> Layer<S> for RetryLayer<P>
where P: Clone
{
    type Service = Retry<P, S>;

    fn layer(&self, service: S) -> Self::Service {
        let policy = self.policy.clone();
        Retry::new(policy, service)
    }
}

#[derive(Clone, Debug)]
pub struct RetryLayer<P> {
    policy: P,
}

impl<P> RetryLayer<P> {
    /// Create a new [`RetryLayer`] from a retry policy
    pub fn new(policy: P) -> Self {
        RetryLayer { policy }
    }
}

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    num_attempts: usize,
    retry_params: RetryParams,
    retry_metrics_counter_opt: Option<Counter>,
}

impl RetryPolicy {
    /// Records each failed attempt that this policy will retry as a transient request.
    pub fn with_retry_metrics(mut self, retry_metrics_counter: Counter) -> Self {
        self.retry_metrics_counter_opt = Some(retry_metrics_counter);
        self
    }
}

impl From<RetryParams> for RetryPolicy {
    fn from(retry_params: RetryParams) -> Self {
        Self {
            num_attempts: 0,
            retry_params,
            retry_metrics_counter_opt: None,
        }
    }
}

impl<R, T, E> Policy<R, T, E> for RetryPolicy
where
    R: Clone + RpcName,
    E: fmt::Debug + Retryable + GrpcStatusCode,
{
    type Future = Sleep;

    fn retry(&mut self, _request: &mut R, result: &mut Result<T, E>) -> Option<Self::Future> {
        match result {
            Ok(_) => None,
            Err(error) => {
                self.num_attempts += 1;

                if !error.is_retryable() || self.num_attempts >= self.retry_params.max_attempts {
                    None
                } else {
                    if let Some(retry_metrics_counter) = &self.retry_metrics_counter_opt {
                        counter!(
                            parent: retry_metrics_counter,
                            labels: [labels!(
                                "rpc" => R::rpc_name(),
                                "status" => "transient",
                                "code" => grpc_code_label(error.grpc_status_code()),
                            )],
                        )
                        .inc();
                    }
                    let delay = self.retry_params.compute_delay(self.num_attempts);
                    debug!(
                        num_attempts=%self.num_attempts,
                        delay_millis=%delay.as_millis(),
                        error=?error,
                        "{} request failed, retrying.", type_name::<R>()
                    );
                    let sleep_fut = tokio::time::sleep(delay);
                    Some(sleep_fut)
                }
            }
        }
    }

    fn clone_request(&mut self, request: &R) -> Option<R> {
        Some(request.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    use futures::future::{Ready, ready};
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tower::{Layer, Service, ServiceExt};

    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    pub enum Retry<E> {
        Permanent(E),
        Transient(E),
    }

    impl<E> Retryable for Retry<E> {
        fn is_retryable(&self) -> bool {
            match self {
                Retry::Permanent(_) => false,
                Retry::Transient(_) => true,
            }
        }
    }

    impl<E> GrpcStatusCode for Retry<E> {
        fn grpc_status_code(&self) -> tonic::Code {
            tonic::Code::Unavailable
        }
    }

    #[derive(Debug, Clone, Default)]
    struct HelloService;

    type HelloResults = Arc<Mutex<Vec<Result<(), Retry<()>>>>>;

    #[derive(Debug, Clone, Default)]
    struct HelloRequest {
        num_attempts: Arc<AtomicUsize>,
        results: HelloResults,
    }

    impl RpcName for HelloRequest {
        fn rpc_name() -> &'static str {
            "hello"
        }
    }

    impl Service<HelloRequest> for HelloService {
        type Response = ();
        type Error = Retry<()>;
        type Future = Ready<Result<(), Retry<()>>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: HelloRequest) -> Self::Future {
            request.num_attempts.fetch_add(1, Ordering::Relaxed);
            let result = request
                .results
                .lock()
                .expect("lock should not be poisoned")
                .pop()
                .unwrap_or(Err(Retry::Permanent(())));
            ready(result)
        }
    }

    #[tokio::test]
    async fn test_retry_policy_records_retryable_failures_as_transient() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        with_local_recorder(&recorder, || {
            let retry_metrics_counter = counter!(
                name: "requests_total",
                description: "test request count",
                subsystem: "grpc",
            );
            let mut retry_policy = RetryPolicy::from(RetryParams::for_test())
                .with_retry_metrics(retry_metrics_counter);
            let mut request = HelloRequest::default();
            let mut result: Result<(), Retry<()>> = Err(Retry::Transient(()));

            assert!(retry_policy.retry(&mut request, &mut result).is_some());
        });

        let snapshot = snapshotter.snapshot().into_vec();
        assert!(snapshot.iter().any(|(composite_key, _, _, value)| {
            let (_, key) = composite_key.clone().into_parts();
            let labels = key
                .labels()
                .map(|label| (label.key(), label.value()))
                .collect::<Vec<_>>();
            key.name() == "quickwit_grpc_requests_total"
                && labels.contains(&("rpc", "hello"))
                && labels.contains(&("status", "transient"))
                && labels.contains(&("code", "unavailable"))
                && value == &DebugValue::Counter(1)
        }));
    }

    #[tokio::test]
    async fn test_retry_policy() {
        let retry_policy = RetryPolicy::from(RetryParams::for_test());
        let retry_layer = RetryLayer::new(retry_policy);
        let mut retry_hello_service = retry_layer.layer(HelloService);

        let hello_request = HelloRequest {
            results: Arc::new(Mutex::new(vec![Ok(())])),
            ..Default::default()
        };
        retry_hello_service
            .ready()
            .await
            .unwrap()
            .call(hello_request.clone())
            .await
            .unwrap();
        assert_eq!(hello_request.num_attempts.load(Ordering::Relaxed), 1);

        let hello_request = HelloRequest {
            results: Arc::new(Mutex::new(vec![Ok(()), Err(Retry::Transient(()))])),
            ..Default::default()
        };
        retry_hello_service
            .ready()
            .await
            .unwrap()
            .call(hello_request.clone())
            .await
            .unwrap();
        assert_eq!(hello_request.num_attempts.load(Ordering::Relaxed), 2);

        let hello_request = HelloRequest {
            results: Arc::new(Mutex::new(vec![
                Err(Retry::Transient(())),
                Err(Retry::Transient(())),
                Err(Retry::Transient(())),
            ])),
            ..Default::default()
        };
        retry_hello_service
            .ready()
            .await
            .unwrap()
            .call(hello_request.clone())
            .await
            .unwrap_err();
        assert_eq!(hello_request.num_attempts.load(Ordering::Relaxed), 3);

        let hello_request = HelloRequest::default();
        retry_hello_service
            .ready()
            .await
            .unwrap()
            .call(hello_request.clone())
            .await
            .unwrap_err();
        assert_eq!(hello_request.num_attempts.load(Ordering::Relaxed), 1);
    }
}
