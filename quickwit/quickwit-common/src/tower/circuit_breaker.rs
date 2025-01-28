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
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use pin_project::pin_project;
use prometheus::IntCounter;
use tokio::time::Instant;
use tower::{Layer, Service};

/// The circuit breaker layer implements the [circuit breaker pattern](https://martinfowler.com/bliki/CircuitBreaker.html).
///
/// It counts the errors emitted by the inner service, and if the number of errors exceeds a certain
/// threshold within a certain time window, it will "open" the circuit.
///
/// Requests will then be rejected for a given timeout.
/// After this timeout, the circuit breaker ends up in a HalfOpen state. It will allow a single
/// request to pass through. Depending on the result of this request, the circuit breaker will
/// either close the circuit again or open it again.
///
/// Implementation detail:
///
/// A circuit breaker needs to have some logic to estimate the chances for the next request
/// to fail. In this implementation, we use a simple heuristic that does not take in account
/// successes. We simply count the number or errors which happened in the last window.
///
/// The circuit breaker does not attempt to measure accurately the error rate.
/// Instead, it counts errors, and check for the time window in which these errors occurred.
/// This approach is accurate enough, robust, very easy to code and avoids calling the
/// `Instant::now()` at every error in the open state.
#[derive(Debug, Clone)]
pub struct CircuitBreakerLayer<Evaluator> {
    max_error_count_per_time_window: u32,
    time_window: Duration,
    timeout: Duration,
    evaluator: Evaluator,
    circuit_break_total: prometheus::IntCounter,
}

pub trait CircuitBreakerEvaluator: Clone {
    type Response;
    type Error;
    fn is_circuit_breaker_error(&self, output: &Result<Self::Response, Self::Error>) -> bool;
    fn make_circuit_breaker_output(&self) -> Self::Error;
    fn make_layer(
        self,
        max_num_errors_per_secs: u32,
        timeout: Duration,
        circuit_break_total: prometheus::IntCounter,
    ) -> CircuitBreakerLayer<Self> {
        CircuitBreakerLayer {
            max_error_count_per_time_window: max_num_errors_per_secs,
            time_window: Duration::from_secs(1),
            timeout,
            evaluator: self,
            circuit_break_total,
        }
    }
}

impl<S, Evaluator: CircuitBreakerEvaluator> Layer<S> for CircuitBreakerLayer<Evaluator> {
    type Service = CircuitBreaker<S, Evaluator>;

    fn layer(&self, service: S) -> CircuitBreaker<S, Evaluator> {
        let time_window = Duration::from_millis(self.time_window.as_millis() as u64);
        let timeout = Duration::from_millis(self.timeout.as_millis() as u64);
        CircuitBreaker {
            underlying: service,
            circuit_breaker_inner: Arc::new(Mutex::new(CircuitBreakerInner {
                max_error_count_per_time_window: self.max_error_count_per_time_window,
                time_window,
                timeout,
                state: CircuitBreakerState::Closed(ClosedState {
                    error_counter: 0u32,
                    error_window_end: Instant::now() + time_window,
                }),
                evaluator: self.evaluator.clone(),
                circuit_break_total: self.circuit_break_total.clone(),
            })),
        }
    }
}

struct CircuitBreakerInner<Evaluator> {
    max_error_count_per_time_window: u32,
    time_window: Duration,
    timeout: Duration,
    evaluator: Evaluator,
    state: CircuitBreakerState,
    circuit_break_total: IntCounter,
}

impl<Evaluator> CircuitBreakerInner<Evaluator> {
    fn get_state(&mut self) -> CircuitBreakerState {
        let new_state = match self.state {
            CircuitBreakerState::Open { until } => {
                let now = Instant::now();
                if now < until {
                    CircuitBreakerState::Open { until }
                } else {
                    CircuitBreakerState::HalfOpen
                }
            }
            other => other,
        };
        self.state = new_state;
        new_state
    }

    fn receive_error(&mut self) {
        match self.state {
            CircuitBreakerState::HalfOpen => {
                self.circuit_break_total.inc();
                self.state = CircuitBreakerState::Open {
                    until: Instant::now() + self.timeout,
                }
            }
            CircuitBreakerState::Open { .. } => {}
            CircuitBreakerState::Closed(ClosedState {
                error_counter,
                error_window_end,
            }) => {
                if error_counter < self.max_error_count_per_time_window {
                    self.state = CircuitBreakerState::Closed(ClosedState {
                        error_counter: error_counter + 1,
                        error_window_end,
                    });
                    return;
                }
                let now = Instant::now();
                if now < error_window_end {
                    self.circuit_break_total.inc();
                    self.state = CircuitBreakerState::Open {
                        until: now + self.timeout,
                    };
                } else {
                    self.state = CircuitBreakerState::Closed(ClosedState {
                        error_counter: 0u32,
                        error_window_end: now + self.time_window,
                    });
                }
            }
        }
    }

    fn receive_success(&mut self) {
        match self.state {
            CircuitBreakerState::HalfOpen | CircuitBreakerState::Open { .. } => {
                self.state = CircuitBreakerState::Closed(ClosedState {
                    error_counter: 0u32,
                    error_window_end: Instant::now() + self.time_window,
                });
            }
            CircuitBreakerState::Closed { .. } => {
                // We could actually take that as a signal.
            }
        }
    }
}

#[derive(Clone)]
pub struct CircuitBreaker<S, Evaluator> {
    underlying: S,
    circuit_breaker_inner: Arc<Mutex<CircuitBreakerInner<Evaluator>>>,
}

impl<S, Evaluator> std::fmt::Debug for CircuitBreaker<S, Evaluator> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("CircuitBreaker").finish()
    }
}

#[derive(Debug, Clone, Copy)]
enum CircuitBreakerState {
    Open { until: Instant },
    HalfOpen,
    Closed(ClosedState),
}

#[derive(Debug, Clone, Copy)]
struct ClosedState {
    error_counter: u32,
    error_window_end: Instant,
}

impl<S, R, Evaluator> Service<R> for CircuitBreaker<S, Evaluator>
where
    S: Service<R>,
    Evaluator: CircuitBreakerEvaluator<Response = S::Response, Error = S::Error>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = CircuitBreakerFuture<S::Future, Evaluator>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut inner = self.circuit_breaker_inner.lock().unwrap();
        let state = inner.get_state();
        match state {
            CircuitBreakerState::Closed { .. } | CircuitBreakerState::HalfOpen => {
                self.underlying.poll_ready(cx)
            }
            CircuitBreakerState::Open { .. } => {
                let circuit_break_error = inner.evaluator.make_circuit_breaker_output();
                Poll::Ready(Err(circuit_break_error))
            }
        }
    }

    fn call(&mut self, request: R) -> Self::Future {
        CircuitBreakerFuture {
            underlying_fut: self.underlying.call(request),
            circuit_breaker_inner: self.circuit_breaker_inner.clone(),
        }
    }
}

#[pin_project]
pub struct CircuitBreakerFuture<F, Evaluator> {
    #[pin]
    underlying_fut: F,
    circuit_breaker_inner: Arc<Mutex<CircuitBreakerInner<Evaluator>>>,
}

impl<Response, Error, F, Evaluator> Future for CircuitBreakerFuture<F, Evaluator>
where
    F: Future<Output = Result<Response, Error>>,
    Evaluator: CircuitBreakerEvaluator<Response = Response, Error = Error>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let circuit_breaker_inner = self.circuit_breaker_inner.clone();
        let poll_res = self.project().underlying_fut.poll(cx);
        match poll_res {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                let mut circuit_breaker_inner_lock = circuit_breaker_inner.lock().unwrap();
                let is_circuit_breaker_error = circuit_breaker_inner_lock
                    .evaluator
                    .is_circuit_breaker_error(&result);
                if is_circuit_breaker_error {
                    circuit_breaker_inner_lock.receive_error();
                } else {
                    circuit_breaker_inner_lock.receive_success();
                }
                Poll::Ready(result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use tower::{ServiceBuilder, ServiceExt};

    use super::*;

    #[derive(Debug)]
    enum TestError {
        CircuitBreak,
        ServiceError,
    }

    #[derive(Debug, Clone, Copy)]
    struct TestCircuitBreakerEvaluator;

    impl CircuitBreakerEvaluator for TestCircuitBreakerEvaluator {
        type Response = ();
        type Error = TestError;

        fn is_circuit_breaker_error(&self, output: &Result<Self::Response, Self::Error>) -> bool {
            output.is_err()
        }

        fn make_circuit_breaker_output(&self) -> TestError {
            TestError::CircuitBreak
        }
    }

    #[tokio::test]
    async fn test_circuit_breaker() {
        tokio::time::pause();
        let test_switch: Arc<AtomicBool> = Arc::new(AtomicBool::new(true));

        const TIMEOUT: Duration = Duration::from_millis(500);

        let int_counter: prometheus::IntCounter =
            IntCounter::new("circuit_break_total_test", "test circuit breaker counter").unwrap();
        let mut service = ServiceBuilder::new()
            .layer(TestCircuitBreakerEvaluator.make_layer(10, TIMEOUT, int_counter))
            .service_fn(|_| async {
                if test_switch.load(Ordering::Relaxed) {
                    Ok(())
                } else {
                    Err(TestError::ServiceError)
                }
            });

        service.ready().await.unwrap().call(()).await.unwrap();

        for _ in 0..1_000 {
            service.ready().await.unwrap().call(()).await.unwrap();
        }

        test_switch.store(false, Ordering::Relaxed);

        let mut service_error_count = 0;
        let mut circuit_break_count = 0;
        for _ in 0..1_000 {
            match service.ready().await {
                Ok(service) => {
                    service.call(()).await.unwrap_err();
                    service_error_count += 1;
                }
                Err(_circuit_breaker_error) => {
                    circuit_break_count += 1;
                }
            }
        }

        assert_eq!(service_error_count + circuit_break_count, 1_000);
        assert_eq!(service_error_count, 11);

        tokio::time::advance(TIMEOUT).await;

        // The test request at half open fails.
        for _ in 0..1_000 {
            match service.ready().await {
                Ok(service) => {
                    service.call(()).await.unwrap_err();
                    service_error_count += 1;
                }
                Err(_circuit_breaker_error) => {
                    circuit_break_count += 1;
                }
            }
        }

        assert_eq!(service_error_count + circuit_break_count, 2_000);
        assert_eq!(service_error_count, 12);

        test_switch.store(true, Ordering::Relaxed);
        tokio::time::advance(TIMEOUT).await;

        // The test request at half open succeeds.
        for _ in 0..1_000 {
            service.ready().await.unwrap().call(()).await.unwrap();
        }
    }
}
