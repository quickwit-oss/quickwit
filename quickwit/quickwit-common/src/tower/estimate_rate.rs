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

use std::marker::PhantomData;
use std::task::{Context, Poll};
use std::time::Instant;

use tower::load::completion::TrackCompletionFuture;
use tower::load::CompleteOnResponse;
use tower::{Layer, Service};

use super::{Cost, RateEstimator};

pub struct Handle<T: RateEstimator> {
    started_at: Instant,
    work: u64,
    estimator: T,
}

impl<T> Drop for Handle<T>
where T: RateEstimator
{
    fn drop(&mut self) {
        let ended_at = Instant::now();
        self.estimator.update(self.started_at, ended_at, self.work);
    }
}

/// Estimates the quantity of work the underlying service can handle over a period of time.
///
/// Each request is decorated with a `Handle` that measures the time necessary to process the
/// request and, on drop, updates the rate estimator on which it holds a reference.
#[derive(Debug, Clone)]
pub struct EstimateRate<S, T> {
    service: S,
    estimator: T,
}

impl<S, T> EstimateRate<S, T>
where T: RateEstimator
{
    /// Creates a new rate estimator.
    pub fn new(service: S, estimator: T) -> Self {
        Self { service, estimator }
    }

    fn handle(&self, work: u64) -> Handle<T> {
        Handle {
            started_at: Instant::now(),
            work,
            estimator: self.estimator.clone(),
        }
    }
}

impl<S, R, T> Service<R> for EstimateRate<S, T>
where
    S: Service<R>,
    R: Cost,
    T: RateEstimator,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = TrackCompletionFuture<S::Future, CompleteOnResponse, Handle<T>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> Self::Future {
        let handle = self.handle(request.cost());
        TrackCompletionFuture::new(
            CompleteOnResponse::default(),
            handle,
            self.service.call(request),
        )
    }
}

/// Estimates the quantity of work the underlying
/// service can handle over a period of time.
#[derive(Debug, Clone)]
pub struct EstimateRateLayer<R, T> {
    estimator: T,
    _phantom: PhantomData<R>,
}

impl<R, T> EstimateRateLayer<R, T> {
    /// Creates new estimate rate layer.
    pub fn new(estimator: T) -> Self {
        Self {
            estimator,
            _phantom: PhantomData,
        }
    }
}

impl<S, R, T> Layer<S> for EstimateRateLayer<R, T>
where
    S: Service<R>,
    R: Cost,
    T: RateEstimator,
{
    type Service = EstimateRate<S, T>;

    fn layer(&self, service: S) -> Self::Service {
        EstimateRate::new(service, self.estimator.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tower::ServiceExt;

    use super::*;
    use crate::tower::Rate;

    struct Request;

    impl Cost for Request {
        fn cost(&self) -> u64 {
            42
        }
    }

    #[derive(Debug, Clone, Default)]
    struct DummyEstimator {
        work: Arc<AtomicU64>,
        duration_micros: Arc<AtomicU64>,
    }

    impl Rate for DummyEstimator {
        fn work(&self) -> u64 {
            self.work.load(Ordering::Relaxed)
        }

        fn period(&self) -> Duration {
            Duration::from_micros(self.duration_micros.load(Ordering::Relaxed))
        }
    }

    impl RateEstimator for DummyEstimator {
        fn update(&mut self, started_at: Instant, ended_at: Instant, work: u64) {
            self.work.store(work, Ordering::Relaxed);
            self.duration_micros.store(
                (ended_at - started_at).as_micros() as u64,
                Ordering::Relaxed,
            );
        }
    }

    #[tokio::test]
    async fn test_estimate_rate() {
        let estimator = DummyEstimator::default();
        let mut service = EstimateRate::new(
            tower::service_fn(|_: Request| async move { Ok::<_, ()>(()) }),
            estimator.clone(),
        );
        service.ready().await.unwrap().call(Request).await.unwrap();
        assert_eq!(service.estimator.work(), 42);
    }
}
