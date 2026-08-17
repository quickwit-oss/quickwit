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

use tokio::time::Sleep;
use tower::Layer;
use tower::retry::{Policy, Retry};
use tracing::debug;

use crate::retry::{RetryParams, Retryable};

/// Retry layer copy/pasted from `tower::retry::RetryLayer`
/// but which implements `Clone`.
impl<P, S> Layer<S> for RetryLayer<P>
where
    P: Clone,
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

/// Callbacks invoked after the retry policy classifies an attempt's result.
pub trait RetryCallbacks<R, T, E>: Clone {
    /// Called when an attempt failed and another attempt will be made.
    fn on_retry(&mut self, _request: &R, _error: &E, _num_attempts: usize) {}

    /// Called when the logical request completed with an error.
    fn on_error(&mut self, _request: &R, _error: &E, _num_attempts: usize) {}

    /// Called when the logical request completed successfully.
    fn on_success(&mut self, _request: &R, _response: &T, _num_attempts: usize) {}
}

/// Default callbacks that do nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopRetryCallbacks;

impl<R, T, E> RetryCallbacks<R, T, E> for NoopRetryCallbacks {}

#[derive(Clone, Debug)]
pub struct RetryPolicy<C = NoopRetryCallbacks> {
    num_attempts: usize,
    retry_params: RetryParams,
    callbacks: C,
}

impl<C> RetryPolicy<C> {
    /// Replaces the callbacks invoked when an attempt is classified.
    pub fn with_callbacks<D>(self, callbacks: D) -> RetryPolicy<D> {
        RetryPolicy {
            num_attempts: self.num_attempts,
            retry_params: self.retry_params,
            callbacks,
        }
    }
}

impl From<RetryParams> for RetryPolicy {
    fn from(retry_params: RetryParams) -> Self {
        Self {
            num_attempts: 0,
            retry_params,
            callbacks: NoopRetryCallbacks,
        }
    }
}

impl<R, T, E, C> Policy<R, T, E> for RetryPolicy<C>
where
    R: Clone,
    E: fmt::Debug + Retryable,
    C: RetryCallbacks<R, T, E>,
{
    type Future = Sleep;

    fn retry(&mut self, request: &mut R, result: &mut Result<T, E>) -> Option<Self::Future> {
        match result {
            Ok(response) => {
                self.callbacks
                    .on_success(request, response, self.num_attempts + 1);
                None
            }
            Err(error) => {
                self.num_attempts += 1;

                if !error.is_retryable() || self.num_attempts >= self.retry_params.max_attempts {
                    self.callbacks.on_error(request, error, self.num_attempts);
                    None
                } else {
                    self.callbacks.on_retry(request, error, self.num_attempts);
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

    #[derive(Debug, Clone, Default)]
    struct HelloService;

    type HelloResults = Arc<Mutex<Vec<Result<(), Retry<()>>>>>;

    #[derive(Debug, Clone, Default)]
    struct HelloRequest {
        num_attempts: Arc<AtomicUsize>,
        results: HelloResults,
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

    #[derive(Clone, Debug, Default)]
    struct TestRetryCallbacks {
        events: Arc<Mutex<Vec<(&'static str, usize)>>>,
    }

    impl RetryCallbacks<HelloRequest, (), Retry<()>> for TestRetryCallbacks {
        fn on_retry(&mut self, _request: &HelloRequest, _error: &Retry<()>, num_attempts: usize) {
            self.events.lock().unwrap().push(("retry", num_attempts));
        }

        fn on_error(&mut self, _request: &HelloRequest, _error: &Retry<()>, num_attempts: usize) {
            self.events.lock().unwrap().push(("error", num_attempts));
        }

        fn on_success(&mut self, _request: &HelloRequest, _response: &(), num_attempts: usize) {
            self.events.lock().unwrap().push(("success", num_attempts));
        }
    }

    #[tokio::test]
    async fn test_retry_policy_callbacks() {
        let callbacks = TestRetryCallbacks::default();
        let mut retry_policy =
            RetryPolicy::from(RetryParams::for_test()).with_callbacks(callbacks.clone());
        let mut request = HelloRequest::default();

        let mut transient_error = Err(Retry::Transient(()));
        assert!(
            retry_policy
                .retry(&mut request, &mut transient_error)
                .is_some()
        );
        let mut success = Ok(());
        assert!(retry_policy.retry(&mut request, &mut success).is_none());
        assert_eq!(
            callbacks.events.lock().unwrap().as_slice(),
            &[("retry", 1), ("success", 2)]
        );

        let callbacks = TestRetryCallbacks::default();
        let mut retry_policy =
            RetryPolicy::from(RetryParams::for_test()).with_callbacks(callbacks.clone());
        for num_attempts in 1..=3 {
            let mut transient_error = Err(Retry::Transient(()));
            let retry = retry_policy.retry(&mut request, &mut transient_error);
            assert_eq!(retry.is_some(), num_attempts < 3);
        }
        assert_eq!(
            callbacks.events.lock().unwrap().as_slice(),
            &[("retry", 1), ("retry", 2), ("error", 3)]
        );

        let callbacks = TestRetryCallbacks::default();
        let mut retry_policy =
            RetryPolicy::from(RetryParams::for_test()).with_callbacks(callbacks.clone());
        let mut permanent_error = Err(Retry::Permanent(()));
        assert!(retry_policy
            .retry(&mut request, &mut permanent_error)
            .is_none());
        assert_eq!(
            callbacks.events.lock().unwrap().as_slice(),
            &[("error", 1)]
        );
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
