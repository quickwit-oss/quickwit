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

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex as TokioMutex;

use crate::type_map::TypeMap;

pub trait Event: fmt::Debug + Clone + Send + Sync + 'static {}

#[async_trait]
pub trait EventSubscriber<E>: Send + Sync + 'static {
    async fn handle_event(&mut self, event: E);
}

#[async_trait]
impl<E, F> EventSubscriber<E> for F
where
    E: Event,
    F: Fn(E) + Send + Sync + 'static,
{
    async fn handle_event(&mut self, event: E) {
        (self)(event);
    }
}

type EventSubscriptions<E> = HashMap<usize, EventSubscription<E>>;

/// The event broker makes it possible to
/// - emit specific local events
/// - subscribe to these local events
/// The event broker is not distributed in itself. Only events emitted
/// locally will be received by the subscribers.
///
/// It is however possible to locally subscribe a handler to a kind of event,
/// that will in turn run a RPC to other nodes.
#[derive(Debug, Clone, Default)]
pub struct EventBroker {
    inner: Arc<InnerEventBroker>,
}

#[derive(Debug, Default)]
struct InnerEventBroker {
    subscription_sequence: AtomicUsize,
    subscriptions: Mutex<TypeMap>,
}

impl EventBroker {
    /// Subscribes to an event type.
    #[must_use]
    pub fn subscribe<E>(&self, subscriber: impl EventSubscriber<E>) -> EventSubscriptionHandle
    where E: Event {
        let mut subscriptions = self
            .inner
            .subscriptions
            .lock()
            .expect("lock should not be poisoned");

        if !subscriptions.contains::<EventSubscriptions<E>>() {
            subscriptions.insert::<EventSubscriptions<E>>(HashMap::new());
        }
        let subscription_id = self
            .inner
            .subscription_sequence
            .fetch_add(1, Ordering::Relaxed);

        let subscription = EventSubscription {
            subscriber: Arc::new(TokioMutex::new(Box::new(subscriber))),
        };
        let typed_subscriptions = subscriptions
            .get_mut::<EventSubscriptions<E>>()
            .expect("subscription map should exist");
        typed_subscriptions.insert(subscription_id, subscription);

        EventSubscriptionHandle {
            subscription_id,
            broker: Arc::downgrade(&self.inner),
            drop_me: |subscription_id, broker| {
                let mut subscriptions = broker
                    .subscriptions
                    .lock()
                    .expect("lock should not be poisoned");
                if let Some(typed_subscriptions) = subscriptions.get_mut::<EventSubscriptions<E>>()
                {
                    typed_subscriptions.remove(&subscription_id);
                }
            },
        }
    }

    /// Publishes an event.
    pub fn publish<E>(&self, event: E)
    where E: Event {
        let subscriptions = self
            .inner
            .subscriptions
            .lock()
            .expect("lock should not be poisoned");

        if let Some(typed_subscriptions) = subscriptions.get::<EventSubscriptions<E>>() {
            for subscription in typed_subscriptions.values() {
                let event = event.clone();
                let subscriber_clone = subscription.subscriber.clone();
                tokio::spawn(tokio::time::timeout(Duration::from_secs(600), async move {
                    let mut subscriber_lock = subscriber_clone.lock().await;
                    subscriber_lock.handle_event(event).await;
                }));
            }
        }
    }
}

struct EventSubscription<E> {
    subscriber: Arc<TokioMutex<Box<dyn EventSubscriber<E>>>>,
}

#[derive(Clone)]
pub struct EventSubscriptionHandle {
    subscription_id: usize,
    broker: Weak<InnerEventBroker>,
    drop_me: fn(usize, &InnerEventBroker),
}

impl EventSubscriptionHandle {
    pub fn cancel(self) {}

    /// By default, dropping a subscription handle cancels the subscription.
    /// `forever` consumes the handle and avoids cancelling the subscription on drop.
    pub fn forever(mut self) {
        self.broker = Weak::new();
    }
}

impl Drop for EventSubscriptionHandle {
    fn drop(&mut self) {
        if let Some(broker) = self.broker.upgrade() {
            (self.drop_me)(self.subscription_id, &broker);
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    #[derive(Debug, Clone)]
    struct MyEvent {
        value: usize,
    }

    impl Event for MyEvent {}

    #[derive(Debug, Clone)]
    struct MySubscriber {
        counter: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl EventSubscriber<MyEvent> for MySubscriber {
        async fn handle_event(&mut self, event: MyEvent) {
            self.counter.store(event.value, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn test_event_broker() {
        let event_broker = EventBroker::default();
        let counter = Arc::new(AtomicUsize::new(0));
        let subscriber = MySubscriber {
            counter: counter.clone(),
        };
        let subscription_handle = event_broker.subscribe(subscriber);

        let event = MyEvent { value: 42 };
        event_broker.publish(event);

        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 42);

        subscription_handle.cancel();

        let event = MyEvent { value: 1337 };
        event_broker.publish(event);

        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 42);
    }

    #[tokio::test]
    async fn test_event_broker_handle_drop() {
        let event_broker = EventBroker::default();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        drop(event_broker.subscribe(move |event: MyEvent| {
            tx.send(event.value).unwrap();
        }));
        event_broker.publish(MyEvent { value: 42 });
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_event_broker_handle_cancel() {
        let event_broker = EventBroker::default();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        event_broker
            .subscribe(move |event: MyEvent| {
                tx.send(event.value).unwrap();
            })
            .cancel();
        event_broker.publish(MyEvent { value: 42 });
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_event_broker_handle_forever() {
        let event_broker = EventBroker::default();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        event_broker
            .subscribe(move |event: MyEvent| {
                tx.send(event.value).unwrap();
            })
            .forever();
        event_broker.publish(MyEvent { value: 42 });
        assert_eq!(rx.recv().await, Some(42));
    }
}
