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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use tokio::time::{Instant, Sleep};
use tower::{Layer, Service};

use super::rate::Rate;
use super::Cost;

/// Enforces a rate limit on the quantity of work the underlying
/// service can handle over a period of time. This implementation is a generalization of
/// `tower::limit::RateLimit`, which is limited to a constant rate of requests over a period of
/// time.
#[derive(Debug)]
pub struct RateLimit<S, T> {
    inner: S,
    rate: T,
    state: State,
    sleep: Pin<Box<Sleep>>,
}

#[derive(Debug)]
enum State {
    // The service has hit its limit.
    Limited { debit: u64 },
    Ready { deadline: Instant, credit: u64 },
}

impl<S, T> RateLimit<S, T>
where T: Rate
{
    /// Creates a new rate limiter.
    pub fn new(inner: S, rate: T) -> Self {
        let deadline = Instant::now();
        let state = State::Ready {
            deadline,
            credit: rate.work(),
        };

        Self {
            inner,
            rate,
            state,
            // The sleep won't actually be used with this duration, but
            // we create it eagerly so that we can reset it in place rather than
            // `Box::pin`ning a new `Sleep` every time we need one.
            sleep: Box::pin(tokio::time::sleep_until(deadline)),
        }
    }

    /// Gets a reference to the inner service.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Gets a mutable reference to the inner service.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consumes `self`, returning the inner service
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S, R, T> Service<R> for RateLimit<S, T>
where
    S: Service<R>,
    R: Cost,
    T: Rate,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let debit = match self.state {
            State::Ready { .. } => return Poll::Ready(ready!(self.inner.poll_ready(cx))),
            State::Limited { debit } => {
                if Pin::new(&mut self.sleep).poll(cx).is_pending() {
                    return Poll::Pending;
                }
                debit
            }
        };
        let deposit = self.rate.work();

        if deposit >= debit {
            self.state = State::Ready {
                deadline: Instant::now() + self.rate.period(),
                credit: deposit - debit,
            };
            Poll::Ready(ready!(self.inner.poll_ready(cx)))
        } else {
            self.state = State::Limited {
                debit: debit - deposit,
            };
            self.sleep
                .as_mut()
                .reset(Instant::now() + self.rate.period());
            Poll::Pending
        }
    }

    fn call(&mut self, request: R) -> Self::Future {
        match self.state {
            State::Ready {
                mut deadline,
                mut credit,
            } => {
                let now = Instant::now();

                // If the period has elapsed, reset it.
                if now >= deadline {
                    deadline = now + self.rate.period();
                    credit = self.rate.work();
                }
                let withdrawal = request.cost();

                if credit >= withdrawal {
                    credit -= withdrawal;
                    self.state = State::Ready { deadline, credit };
                } else {
                    // The service is disabled until further notice
                    // Reset the sleep future in place, so that we don't have to
                    // deallocate the existing box and allocate a new one.
                    let debit = withdrawal - credit;
                    self.state = State::Limited { debit };
                    self.sleep.as_mut().reset(deadline);
                }

                // Call the inner future
                self.inner.call(request)
            }
            State::Limited { .. } => {
                panic!("Service not ready; `poll_ready` must be called first!")
            }
        }
    }
}

/// Enforces a rate limit on the quantity of work the underlying
/// service can handle over a period of time.
#[derive(Debug, Clone)]
pub struct RateLimitLayer<T> {
    rate: T,
}

impl<T> RateLimitLayer<T> {
    /// Creates new rate limit layer.
    pub fn new(rate: T) -> Self {
        Self { rate }
    }
}

impl<S, T> Layer<S> for RateLimitLayer<T>
where T: Rate
{
    type Service = RateLimit<S, T>;

    fn layer(&self, service: S) -> Self::Service {
        RateLimit::new(service, self.rate.clone())
    }
}

#[cfg(test)]
mod tests {

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use futures::future::join_all;
    use tower::{ServiceBuilder, ServiceExt};

    use super::*;
    use crate::tower::buffer::BufferError;
    use crate::tower::{BufferLayer, ConstantRate};

    struct Request {
        cost: u64,
    }

    impl Request {
        fn random() -> Self {
            Self {
                cost: rand::random::<u64>() % 100,
            }
        }
    }

    impl Cost for Request {
        fn cost(&self) -> u64 {
            self.cost
        }
    }

    #[derive(Debug, Clone, thiserror::Error)]
    #[error("Rate meter error")]
    struct RateMeterError;

    impl From<BufferError> for RateMeterError {
        fn from(_: BufferError) -> Self {
            Self
        }
    }

    #[derive(Debug, Clone)]
    struct RateMeter {
        cumulated_work: Arc<AtomicU64>,
    }

    impl RateMeter {
        fn new() -> Self {
            Self {
                cumulated_work: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl Service<Request> for RateMeter {
        type Response = ();
        type Error = RateMeterError;
        type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request) -> Self::Future {
            self.cumulated_work
                .fetch_add(request.cost, Ordering::Relaxed);
            futures::future::ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_rate_limit_over_multiple_periods() {
        let work = 1000;
        let period = 100;

        let rate = ConstantRate::new(work, Duration::from_millis(period));
        let meter = RateMeter::new();
        let mut service = ServiceBuilder::new()
            .layer(BufferLayer::new(10))
            .layer(RateLimitLayer::new(rate))
            .service(meter.clone());

        let now = Instant::now();
        service
            .ready()
            .await
            .unwrap()
            .call(Request { cost: 1 })
            .await
            .unwrap();
        assert!(now.elapsed() < Duration::from_millis(1));

        let now = Instant::now();
        // The first request goes through, but the second one is rate limited.
        service
            .ready()
            .await
            .unwrap()
            .call(Request { cost: 2 * work - 1 })
            .await
            .unwrap();
        service
            .ready()
            .await
            .unwrap()
            .call(Request { cost: 1 })
            .await
            .unwrap();
        assert!(now.elapsed() >= Duration::from_millis(period));
        assert!(now.elapsed() < Duration::from_millis(2 * period));
    }

    #[tokio::test]
    async fn test_rate_limit() {
        let work = 1000;
        let period = 100;
        let deadline = 500;
        let expected_cumulated_work = work * (deadline / period);

        let rate = ConstantRate::new(work, Duration::from_millis(period));
        let meter = RateMeter::new();
        let service = ServiceBuilder::new()
            .layer(BufferLayer::new(10))
            .layer(RateLimitLayer::new(rate))
            .service(meter.clone());

        let futures = (0..5).map(|_| {
            let mut service = service.clone();
            tokio::time::timeout(Duration::from_millis(deadline), async move {
                loop {
                    service
                        .ready()
                        .await
                        .unwrap()
                        .call(Request::random())
                        .await
                        .unwrap();
                }
            })
        });
        join_all(futures).await;
        let cumulated_work = meter.cumulated_work.load(Ordering::Relaxed);
        assert!(cumulated_work > expected_cumulated_work * 95 / 100);
        assert!(cumulated_work < expected_cumulated_work * 105 / 100)
    }
}
