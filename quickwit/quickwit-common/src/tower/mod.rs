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

mod box_layer;
mod box_service;
mod buffer;
mod change;
mod circuit_breaker;
mod delay;
mod estimate_rate;
mod event_listener;
mod load_shed;
mod metrics;
mod one_task_per_call_layer;
mod pool;
mod rate;
mod rate_estimator;
mod rate_limit;
mod retry;
mod timeout;
mod transport;

use std::error;
use std::pin::Pin;

pub use box_layer::BoxLayer;
pub use box_service::BoxService;
pub use buffer::{Buffer, BufferError, BufferLayer};
pub use change::Change;
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerEvaluator, CircuitBreakerLayer};
pub use delay::{Delay, DelayLayer};
pub use estimate_rate::{EstimateRate, EstimateRateLayer};
pub use event_listener::{EventListener, EventListenerLayer};
use futures::Future;
pub use load_shed::{LoadShed, LoadShedLayer, MakeLoadShedError};
pub use metrics::{GrpcMetrics, GrpcMetricsLayer, RpcName};
pub use one_task_per_call_layer::{OneTaskPerCallLayer, TaskCancelled};
pub use pool::Pool;
pub use rate::{ConstantRate, Rate};
pub use rate_estimator::{RateEstimator, SmaRateEstimator};
pub use rate_limit::{RateLimit, RateLimitLayer};
pub use retry::{RetryLayer, RetryPolicy};
pub use timeout::{Timeout, TimeoutExceeded, TimeoutLayer};
pub use transport::{
    BalanceChannel, ClientGrpcConfig, KeepAliveConfig, make_channel, warmup_channel,
};

pub type BoxError = Box<dyn error::Error + Send + Sync + 'static>;

pub type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

pub type BoxFutureInfaillible<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

pub trait Cost {
    fn cost(&self) -> u64;
}

#[cfg(test)]
mod tests {

    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures::FutureExt;
    use tokio::time::sleep;
    use tower::{Service, ServiceBuilder, ServiceExt};

    use super::*;
    use crate::retry::{RetryParams, Retryable};
    use crate::tower::{TimeoutExceeded, TimeoutLayer};

    #[derive(Debug, Eq, PartialEq)]
    pub enum Retry {
        Permanent,
        Transient,
    }

    impl Retryable for Retry {
        fn is_retryable(&self) -> bool {
            match self {
                Retry::Permanent => false,
                Retry::Transient => true,
            }
        }
    }

    impl From<TimeoutExceeded> for Retry {
        fn from(_: TimeoutExceeded) -> Self {
            Retry::Transient
        }
    }

    #[derive(Debug, Clone, Default)]
    struct HelloService;

    #[derive(Debug)]
    struct ExpectedBehavior {
        delay: Duration,
        result: Result<(), Retry>,
    }

    /// A request that specifies the delay and result for each retry attempt.
    #[derive(Debug, Clone, Default)]
    struct HelloRequest {
        num_attempts: Arc<AtomicUsize>,
        retry_behaviors: Arc<Mutex<VecDeque<ExpectedBehavior>>>,
    }

    impl HelloRequest {
        fn new(retry_behaviors: impl IntoIterator<Item = ExpectedBehavior>) -> Self {
            Self {
                num_attempts: Arc::new(AtomicUsize::new(0)),
                retry_behaviors: Arc::new(Mutex::new(VecDeque::from_iter(retry_behaviors))),
            }
        }
    }

    impl Service<HelloRequest> for HelloService {
        type Response = ();
        type Error = Retry;
        type Future = BoxFuture<(), Retry>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: HelloRequest) -> Self::Future {
            request.num_attempts.fetch_add(1, Ordering::Relaxed);
            let ExpectedBehavior { delay, result } = request
                .retry_behaviors
                .lock()
                .expect("lock should not be poisoned")
                .pop_front()
                .expect("more requests than expected");

            Box::pin(sleep(delay).map(|_| result))
        }
    }

    #[tokio::test]
    async fn test_combining_retry_and_timeout_layer() {
        let mut retry_hello_service = ServiceBuilder::new()
            .layer(RetryLayer::new(RetryPolicy::from(RetryParams {
                base_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
                max_attempts: 3,
            })))
            .layer(TimeoutLayer::new(Duration::from_millis(100)))
            .service(HelloService);

        {
            let hello_request = HelloRequest::new([ExpectedBehavior {
                delay: Duration::ZERO,
                result: Ok(()),
            }]);
            retry_hello_service
                .ready()
                .await
                .unwrap()
                .call(hello_request.clone())
                .await
                .unwrap();
            assert_eq!(hello_request.num_attempts.load(Ordering::Relaxed), 1);
        }
        {
            let hello_request = HelloRequest::new([
                ExpectedBehavior {
                    delay: Duration::from_secs(1),
                    result: Ok(()),
                },
                ExpectedBehavior {
                    delay: Duration::ZERO,
                    result: Ok(()),
                },
            ]);
            retry_hello_service
                .ready()
                .await
                .unwrap()
                .call(hello_request.clone())
                .await
                .unwrap();
            assert_eq!(hello_request.num_attempts.load(Ordering::Relaxed), 2);
        }
        {
            let hello_request = HelloRequest::new([
                ExpectedBehavior {
                    delay: Duration::from_secs(1),
                    result: Ok(()),
                },
                ExpectedBehavior {
                    delay: Duration::ZERO,
                    result: Err(Retry::Transient),
                },
                ExpectedBehavior {
                    delay: Duration::from_secs(1),
                    result: Ok(()),
                },
            ]);
            let err = retry_hello_service
                .ready()
                .await
                .unwrap()
                .call(hello_request.clone())
                .await
                .unwrap_err();
            assert!(matches!(err, Retry::Transient));
            assert_eq!(hello_request.num_attempts.load(Ordering::Relaxed), 3);
        }
        {
            let hello_request = HelloRequest::new([
                ExpectedBehavior {
                    delay: Duration::from_secs(1),
                    result: Ok(()),
                },
                ExpectedBehavior {
                    delay: Duration::ZERO,
                    result: Err(Retry::Permanent),
                },
            ]);
            let err = retry_hello_service
                .ready()
                .await
                .unwrap()
                .call(hello_request.clone())
                .await
                .unwrap_err();
            assert!(matches!(err, Retry::Permanent));
            assert_eq!(hello_request.num_attempts.load(Ordering::Relaxed), 2);
        }

        // using the layers in the reverse order doesn't work
        let mut retry_hello_service = ServiceBuilder::new()
            .layer(TimeoutLayer::new(Duration::from_millis(100)))
            .layer(RetryLayer::new(RetryPolicy::from(RetryParams {
                base_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
                max_attempts: 3,
            })))
            .service(HelloService);

        {
            let hello_request = HelloRequest::new([
                ExpectedBehavior {
                    delay: Duration::from_secs(1),
                    result: Ok(()),
                },
                ExpectedBehavior {
                    delay: Duration::ZERO,
                    result: Ok(()),
                },
            ]);
            let err = retry_hello_service
                .ready()
                .await
                .unwrap()
                .call(hello_request.clone())
                .await
                .unwrap_err();
            assert!(matches!(err, Retry::Transient));
            assert_eq!(hello_request.num_attempts.load(Ordering::Relaxed), 1);
        }
    }
}
