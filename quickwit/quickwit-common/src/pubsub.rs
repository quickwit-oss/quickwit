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
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;

use crate::type_map::TypeMap;

pub trait Event: fmt::Debug + Clone + Send + Sync + 'static {}

#[async_trait]
pub trait EventSubscriber<E>: fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    async fn handle_event(&mut self, event: E);
}

dyn_clone::clone_trait_object!(<E> EventSubscriber<E>);

type EventSubscriptions<E> = HashMap<usize, EventSubscription<E>>;

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
    pub fn subscribe<E>(&self, subscriber: impl EventSubscriber<E>) -> EventSubscriptionHandle<E>
    where E: Event {
        let mut subscriptions = self
            .inner
            .subscriptions
            .lock()
            .expect("The lock should never be poisoned.");

        if !subscriptions.contains::<EventSubscriptions<E>>() {
            subscriptions.insert::<EventSubscriptions<E>>(HashMap::new());
        }
        let subscription_id = self
            .inner
            .subscription_sequence
            .fetch_add(1, Ordering::Relaxed);

        let subscription = EventSubscription {
            subscription_id,
            subscriber: Box::new(subscriber),
        };
        let typed_subscriptions = subscriptions
            .get_mut::<EventSubscriptions<E>>()
            .expect("The subscription map should exist.");
        typed_subscriptions.insert(subscription_id, subscription);

        EventSubscriptionHandle {
            subscription_id,
            broker: Arc::downgrade(&self.inner),
            _phantom: PhantomData,
        }
    }

    /// Publishes an event.
    pub fn publish<E>(&self, event: E)
    where E: Event {
        let subscriptions = self
            .inner
            .subscriptions
            .lock()
            .expect("The lock should never be poisoned.");

        if let Some(typed_subscriptions) = subscriptions.get::<EventSubscriptions<E>>() {
            for subscription in typed_subscriptions.values() {
                let event = event.clone();
                let mut subscriber = subscription.subscriber.clone();
                tokio::spawn(tokio::time::timeout(Duration::from_secs(600), async move {
                    subscriber.handle_event(event).await;
                }));
            }
        }
    }
}

#[derive(Debug)]
struct EventSubscription<E> {
    #[allow(dead_code)]
    subscription_id: usize, // Used for the `Debug` implementation.
    subscriber: Box<dyn EventSubscriber<E>>,
}

#[derive(Debug)]
pub struct EventSubscriptionHandle<E: Event> {
    subscription_id: usize,
    broker: Weak<InnerEventBroker>,
    _phantom: PhantomData<E>,
}

impl<E> EventSubscriptionHandle<E>
where E: Event
{
    pub fn cancel(self) {}
}

impl<E> Drop for EventSubscriptionHandle<E>
where E: Event
{
    fn drop(&mut self) {
        if let Some(broker) = self.broker.upgrade() {
            let mut subscriptions = broker
                .subscriptions
                .lock()
                .expect("The lock should never be poisoned.");
            if let Some(typed_subscriptions) = subscriptions.get_mut::<EventSubscriptions<E>>() {
                typed_subscriptions.remove(&self.subscription_id);
            }
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
        let broker = EventBroker::default();
        let counter = Arc::new(AtomicUsize::new(0));
        let subscriber = MySubscriber {
            counter: counter.clone(),
        };
        let subscription = broker.subscribe(subscriber);
        let event = MyEvent { value: 42 };
        broker.publish(event);
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 42);

        subscription.cancel();
        let event = MyEvent { value: 1337 };
        broker.publish(event);
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 42);
    }
}
