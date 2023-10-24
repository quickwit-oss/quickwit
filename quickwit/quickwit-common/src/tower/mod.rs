// Copyright (C) 2023 Quickwit, Inc.
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

mod box_layer;
mod box_service;
mod buffer;
mod change;
mod estimate_rate;
mod event_listener;
mod metrics;
mod pool;
mod rate;
mod rate_estimator;
mod rate_limit;
mod retry;
mod transport;

use std::error;
use std::pin::Pin;

pub use box_layer::BoxLayer;
pub use box_service::BoxService;
pub use buffer::{Buffer, BufferError, BufferLayer};
pub use change::Change;
pub use estimate_rate::{EstimateRate, EstimateRateLayer};
pub use event_listener::{EventListener, EventListenerLayer};
use futures::Future;
pub use metrics::{PrometheusMetrics, PrometheusMetricsLayer};
pub use pool::Pool;
pub use rate::{ConstantRate, Rate};
pub use rate_estimator::{RateEstimator, SmaRateEstimator};
pub use rate_limit::{RateLimit, RateLimitLayer};
pub use retry::{RetryLayer, RetryPolicy};
pub use transport::{make_channel, warmup_channel, BalanceChannel};

pub type BoxError = Box<dyn error::Error + Send + Sync + 'static>;

pub type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

pub type BoxFutureInfaillible<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

pub trait Cost {
    fn cost(&self) -> u64;
}
