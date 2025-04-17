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

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex as TokioMutex;

use crate::rate_limited_warn;
use crate::type_map::TypeMap;

const EVENT_SUBSCRIPTION_CALLBACK_TIMEOUT: Duration = Duration::from_secs(10);

pub trait Event: fmt::Debug + Clone + Send + Sync + 'static {}

#[async_trait]
pub trait EventSubscriber<E>: Send + Sync + 'static {
    async fn handle_event(&mut self, event: E);
}

#[async_trait]
impl<E, F> EventSubscriber<E> for F
where
    E: Event,
    F: FnMut(E) + Send + Sync + 'static,
{
    async fn handle_event(&mut self, event: E) {
        (self)(event);
    }
}

type EventSubscriptions<E> = HashMap<usize, EventSubscription<E>>;

/// The event broker makes it possible to
/// - emit specific local events
/// - subscribe to these local events
///
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
    // The point of this private method is to allow the public subscribe method to have only one
    // generic argument and avoid the ugly `::<E, _>` syntax.
    fn subscribe_aux<E, S>(&self, subscriber: S, with_timeout: bool) -> EventSubscriptionHandle
    where
        E: Event,
        S: EventSubscriber<E> + Send + Sync + 'static,
    {
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

        let subscriber_name = std::any::type_name::<S>();
        let subscription = EventSubscription {
            subscriber_name,
            subscriber: Arc::new(TokioMutex::new(Box::new(subscriber))),
            with_timeout,
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

    /// Subscribes to an event type.
    ///
    /// The callback should be as light as possible.
    ///
    /// # Disclaimer
    ///
    /// If the callback takes more than `EVENT_SUBSCRIPTION_CALLBACK_TIMEOUT` to execute,
    /// the callback future will be aborted.
    #[must_use]
    pub fn subscribe<E>(&self, subscriber: impl EventSubscriber<E>) -> EventSubscriptionHandle
    where E: Event {
        self.subscribe_aux(subscriber, true)
    }

    /// Subscribes to an event type.
    ///
    /// The callback should be as light as possible.
    #[must_use]
    pub fn subscribe_without_timeout<E>(
        &self,
        subscriber: impl EventSubscriber<E>,
    ) -> EventSubscriptionHandle
    where
        E: Event,
    {
        self.subscribe_aux(subscriber, false)
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
                subscription.trigger(event.clone());
            }
        }
    }
}

struct EventSubscription<E> {
    // We put that in the subscription in order to avoid having to take the lock
    // to access it.
    subscriber_name: &'static str,
    subscriber: Arc<TokioMutex<Box<dyn EventSubscriber<E>>>>,
    with_timeout: bool,
}

impl<E: Event> EventSubscription<E> {
    /// Call the callback associated with the subscription.
    fn trigger(&self, event: E) {
        if self.with_timeout {
            self.trigger_abort_on_timeout(event);
        } else {
            self.trigger_just_log_on_timeout(event)
        }
    }

    /// Spawns a task to run the given subscription.
    ///
    /// Just logs a warning if it took more than `EVENT_SUBSCRIPTION_CALLBACK_TIMEOUT`
    /// for the future to execute.
    fn trigger_just_log_on_timeout(&self, event: E) {
        let subscriber_name = self.subscriber_name;
        let subscriber = self.subscriber.clone();
        // This task is just here to log a warning if the callback takes too long to execute.
        let log_timeout_task_handle = tokio::task::spawn(async move {
            tokio::time::sleep(EVENT_SUBSCRIPTION_CALLBACK_TIMEOUT).await;
            let event_name = std::any::type_name::<E>();
            rate_limited_warn!(
                limit_per_min = 10,
                "{subscriber_name}'s handler for {event_name} did not finished within {}ms",
                EVENT_SUBSCRIPTION_CALLBACK_TIMEOUT.as_millis()
            );
        });
        tokio::task::spawn(async move {
            subscriber.lock().await.handle_event(event).await;
            // The callback has terminated, let's abort the timeout task.
            log_timeout_task_handle.abort();
        });
    }

    /// Spawns a task to run the given subscription.
    ///
    /// Aborts the future execution and logs a warning if it takes more than
    /// `EVENT_SUBSCRIPTION_CALLBACK_TIMEOUT`.
    fn trigger_abort_on_timeout(&self, event: E) {
        let subscriber_name = self.subscriber_name;
        let subscriber = self.subscriber.clone();
        let fut = async move {
            if tokio::time::timeout(EVENT_SUBSCRIPTION_CALLBACK_TIMEOUT, async {
                subscriber.lock().await.handle_event(event).await
            })
            .await
            .is_err()
            {
                let event_name = std::any::type_name::<E>();
                rate_limited_warn!(
                    limit_per_min = 10,
                    "{subscriber_name}'s handler for {event_name} timed out, abort"
                );
            }
        };
        tokio::task::spawn(fut);
    }
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

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
