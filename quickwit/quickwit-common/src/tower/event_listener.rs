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

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, Future};
use pin_project::pin_project;
use tower::{Layer, Service};

use crate::pubsub::{Event, EventBroker};

#[derive(Clone)]

pub struct EventListener<S> {
    inner: S,
    event_broker: EventBroker,
}

impl<S> EventListener<S> {
    pub fn new(inner: S, event_broker: EventBroker) -> Self {
        Self {
            inner,
            event_broker,
        }
    }
}

impl<S, R> Service<R> for EventListener<S>
where
    S: Service<R>,
    R: Event,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future, R>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> Self::Future {
        let inner = self.inner.call(request.clone());
        ResponseFuture {
            inner,
            event_broker: self.event_broker.clone(),
            request: Some(request),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventListenerLayer {
    event_broker: EventBroker,
}

impl EventListenerLayer {
    pub fn new(event_broker: EventBroker) -> Self {
        Self { event_broker }
    }
}

impl<S> Layer<S> for EventListenerLayer {
    type Service = EventListener<S>;

    fn layer(&self, service: S) -> Self::Service {
        EventListener::new(service, self.event_broker.clone())
    }
}

/// Response future for [`EventListener`].
#[pin_project]
pub struct ResponseFuture<F, R> {
    #[pin]
    inner: F,
    event_broker: EventBroker,
    request: Option<R>,
}

impl<R, F, T, E> Future for ResponseFuture<F, R>
where
    R: Event,
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.inner.poll(cx));

        if response.is_ok() {
            this.event_broker
                .publish(this.request.take().expect("request should be set"));
        }
        Poll::Ready(Ok(response?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;

    use super::*;
    use crate::pubsub::EventSubscriber;

    #[derive(Debug, Clone, Copy)]
    struct MyEvent {
        return_ok: bool,
    }

    impl Event for MyEvent {}

    #[derive(Debug, Clone)]
    struct MySubscriber {
        counter: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl EventSubscriber<MyEvent> for MySubscriber {
        async fn handle_event(&mut self, _event: MyEvent) {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn test_event_listener() {
        let event_broker = EventBroker::default();
        let counter = Arc::new(AtomicUsize::new(0));
        let subscriber = MySubscriber {
            counter: counter.clone(),
        };
        let _subscription_handle = event_broker.subscribe::<MyEvent>(subscriber);

        let layer = EventListenerLayer::new(event_broker);

        let mut service = layer.layer(tower::service_fn(|request: MyEvent| async move {
            if request.return_ok {
                Ok(())
            } else {
                Err(())
            }
        }));
        let request = MyEvent { return_ok: false };
        service.call(request).await.unwrap_err();

        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        let request = MyEvent { return_ok: true };
        service.call(request).await.unwrap();

        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
}
