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
