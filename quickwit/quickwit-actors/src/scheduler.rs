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

use std::cmp::Reverse;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type Callback = Box<dyn FnOnce() + Sync + Send + 'static>;

struct TimeoutEvent {
    deadline: Instant,
    event_id: u64, //< only useful to break ties in a deterministic way.
    callback: Callback,
}

impl PartialEq for TimeoutEvent {
    fn eq(&self, other: &Self) -> bool {
        self.event_id == other.event_id
    }
}

impl Eq for TimeoutEvent {}

impl PartialOrd for TimeoutEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimeoutEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.event_id.cmp(&other.event_id))
    }
}

enum SchedulerMessage {
    ProcessTime,
    Schedule {
        callback: Callback,
        timeout: Duration,
    },
}

#[derive(Clone)]
pub struct SchedulerClient {
    inner: Arc<SchedulerClientInner>,
}

struct SchedulerClientInner {
    no_advance_time_guard_count: AtomicUsize,
    accelerate_time: AtomicBool,
    tx: flume::Sender<SchedulerMessage>,
}

impl SchedulerClient {
    /// Returns true if someone asked for the time to be accelerated.
    fn time_is_accelerated(&self) -> bool {
        self.inner.accelerate_time.load(Ordering::Relaxed)
    }

    /// Returns true if something is preventing for accelerating the time.
    fn is_advance_time_forbidden(&self) -> bool {
        self.inner
            .no_advance_time_guard_count
            .load(Ordering::SeqCst)
            > 0
    }

    /// Schedules a new event.
    /// Once `timeout` is elapsed, the future `fut` is
    /// executed.
    ///
    /// `fut` will be executed in the scheduler task, so it is
    /// required to be short.
    pub fn schedule_event<F: FnOnce() + Send + Sync + 'static>(
        &self,
        callback: F,
        timeout: Duration,
    ) {
        let _ = self.inner.tx.send(SchedulerMessage::Schedule {
            callback: Box::new(callback),
            timeout,
        });
    }

    // Increases the number of reasons to not simulate advance time.
    pub(crate) fn inc_no_advance_time(&self) {
        self.inner
            .no_advance_time_guard_count
            .fetch_add(1, Ordering::SeqCst);
    }

    // Decrease the number of reasons to not simulate advance time.
    //
    // If the number reaches 0, we trigger a `timeout`.
    pub(crate) fn dec_no_advance_time(&self) {
        let previous_count = self
            .inner
            .no_advance_time_guard_count
            .fetch_sub(1, Ordering::SeqCst);
        if previous_count == 1 {
            self.process_time();
        }
    }

    /// Switch accelerated time mode for the scheduler.
    ///
    /// The scheduler will jump in time whenever there are no more `NoAdvanceInTimeGuard`.
    pub fn accelerate_time(&self) {
        self.inner.accelerate_time.store(true, Ordering::Relaxed);
        self.process_time();
    }

    pub async fn sleep(&self, duration: Duration) {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        self.schedule_event(
            move || {
                let _ = oneshot_tx.send(());
            },
            duration,
        );
        let _ = oneshot_rx.await;
    }

    pub async fn timeout<O>(
        &self,
        duration: Duration,
        fut: impl Future<Output = O>,
    ) -> Result<O, ()> {
        tokio::select! {
            _ = self.sleep(duration) => {
                Err(())
            },
            future_output = fut => {
                Ok(future_output)
            }
        }
    }

    // Triggers an event, telling the Scheduler to process time,
    // checks whether some scheduled events have timed out, or whether we should
    // jump forward in time.
    pub(crate) fn process_time(&self) {
        let _ = self.inner.tx.send(SchedulerMessage::ProcessTime);
    }

    /// Returns a `NoAdvanceTimeGuard` which calls `inc_no_advance_time`
    /// on `NoAdvanceTimeGuard::new` and `dec_no_advance_time` when dropped.
    pub fn no_advance_time_guard(&self) -> NoAdvanceTimeGuard {
        NoAdvanceTimeGuard::new(self.clone())
    }
}

pub struct NoAdvanceTimeGuard {
    scheduler_client: SchedulerClient,
}

impl NoAdvanceTimeGuard {
    fn new(scheduler_client: SchedulerClient) -> Self {
        scheduler_client.inc_no_advance_time();
        NoAdvanceTimeGuard { scheduler_client }
    }
}

impl Drop for NoAdvanceTimeGuard {
    fn drop(&mut self) {
        self.scheduler_client.dec_no_advance_time();
    }
}

pub fn start_scheduler() -> SchedulerClient {
    let (tx, rx) = flume::unbounded::<SchedulerMessage>();
    let scheduler_client = SchedulerClient {
        inner: Arc::new(SchedulerClientInner {
            no_advance_time_guard_count: AtomicUsize::default(),
            accelerate_time: Default::default(),
            tx,
        }),
    };
    let mut scheduler = Scheduler::new(&scheduler_client);
    tokio::spawn(async move {
        while let Ok(scheduler_message) = rx.recv_async().await {
            match scheduler_message {
                SchedulerMessage::ProcessTime => scheduler.process_time(),
                SchedulerMessage::Schedule { callback, timeout } => {
                    scheduler.process_schedule(callback, timeout);
                }
            }
        }
    });
    scheduler_client
}

struct Scheduler {
    // We attribute an event_id to all event just to break ties
    // if two events are scheduled on the same time.
    event_id_generator: u64,
    // Simulated time shift which defines the scheduler time reference as `simulated_time =
    // Instant::now() + simulated_time_shift`. By default `simulated_time_shift` is set to 0
    // but it can be shifted when the scheduler has to process a simulate sleep event`.
    simulated_time_shift: Duration,
    future_events: BinaryHeap<Reverse<TimeoutEvent>>,
    next_timeout: Option<JoinHandle<()>>,
    weak_scheduler_client: Weak<SchedulerClientInner>,
}

impl Scheduler {
    /// Processes "time".
    ///
    /// This :
    /// - identifies all events that are elapsed and execute their callback,
    /// - advance time if necessary
    /// - schedule a message to make sure process_time is called in time for the next event.
    fn process_time(&mut self) {
        let now = self.simulated_now();
        // Pops all elapsed events and executes the associated callback.
        while let Some(next_event_peek) = self.future_events.peek_mut() {
            if next_event_peek.0.deadline > now {
                // The next event is out of scope.
                break;
            }
            let next_event = PeekMut::pop(next_event_peek);
            (next_event.0.callback)();
        }

        // If the condition to accelerate time are met, we can
        // advance time and jump straight to the next timeout.
        self.advance_time_if_necessary();
        self.schedule_next_timeout();
    }

    /// Schedules a new event.
    fn process_schedule(&mut self, callback: Callback, timeout: Duration) {
        let new_evt_deadline = self.simulated_now() + timeout;
        let timeout_event = self.timeout_event(new_evt_deadline, callback);
        self.future_events.push(Reverse(timeout_event));
        self.process_time();
    }

    fn scheduler_client(&self) -> Option<SchedulerClient> {
        let scheduler_client = SchedulerClient {
            inner: self.weak_scheduler_client.upgrade()?,
        };
        Some(scheduler_client)
    }

    /// Schedules a Timeout event callback if necessary.
    fn schedule_next_timeout(&mut self) {
        let Some(scheduler_client) = self.scheduler_client() else { return };
        let simulated_now = self.simulated_now();
        let Some(next_deadline) = self.next_event_deadline() else { return; };
        let timeout: Duration = if next_deadline <= simulated_now {
            // This should almost never happen, because we supposedly triggered
            // all pending events.
            //
            // But time has advanced as we were calling the different callbacks
            // so it is actually possible.
            Duration::default()
        } else {
            next_deadline - simulated_now
        };
        if let Some(previous_join_handle) = self.next_timeout.take() {
            // The next event timeout is about to change. Let's cancel the previous
            // scheduled event.
            previous_join_handle.abort();
        }
        let new_join_handle: JoinHandle<()> = tokio::task::spawn(async move {
            if timeout.is_zero() {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(timeout).await;
            }
            scheduler_client.process_time();
        });
        self.next_timeout = Some(new_join_handle);
    }
}

impl Scheduler {
    pub fn new(scheduler_client: &SchedulerClient) -> Self {
        Scheduler {
            event_id_generator: 0u64,
            simulated_time_shift: Duration::default(),
            future_events: Default::default(),
            next_timeout: None,
            weak_scheduler_client: Arc::downgrade(&scheduler_client.inner),
        }
    }

    /// Updates the simulated time shift, if appropriate.
    ///
    /// We advance time if:
    /// - someone is actually requesting for a simulated fast forward in time.
    /// (if Universe::simulate_time_shift(..) has been called).
    /// - no message is queued for processing, no initialize or no finalize
    /// is being processed.
    fn advance_time_if_necessary(&mut self) {
        let Some(scheduler_client) = self.scheduler_client() else { return; };
        if !scheduler_client.time_is_accelerated() {
            return;
        }
        if scheduler_client.is_advance_time_forbidden() {
            return;
        }
        let Some(advance_to_instant) = self.next_event_deadline() else { return; };
        let now = self.simulated_now();
        if let Some(time_shift) = advance_to_instant.checked_duration_since(now) {
            self.simulated_time_shift += time_shift;
        }
    }

    fn next_event_deadline(&self) -> Option<Instant> {
        self.future_events.peek().map(|rev| rev.0.deadline)
    }

    fn simulated_now(&self) -> Instant {
        Instant::now() + self.simulated_time_shift
    }

    fn timeout_event(&mut self, deadline: Instant, callback: Callback) -> TimeoutEvent {
        let event_id = self.event_id_generator;
        self.event_id_generator += 1;
        TimeoutEvent {
            deadline,
            event_id,
            callback,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use async_trait::async_trait;

    use crate::{Actor, ActorContext, ActorExitStatus, Handler, Universe};

    struct ClockActor {
        count: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    struct Tick;

    #[async_trait]
    impl Actor for ClockActor {
        type ObservableState = ();
        fn observable_state(&self) -> Self::ObservableState {}

        async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
            self.handle(Tick, ctx).await
        }
    }

    #[async_trait]
    impl Handler<Tick> for ClockActor {
        type Reply = ();

        async fn handle(
            &mut self,
            _tick: Tick,
            ctx: &ActorContext<Self>,
        ) -> Result<(), ActorExitStatus> {
            self.count.fetch_add(1, Ordering::SeqCst);
            ctx.schedule_self_msg(Duration::from_secs(1), Tick).await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_scheduler_advance_time_fast_forward_initialize() {
        quickwit_common::setup_logging_for_tests();
        let count: Arc<AtomicUsize> = Default::default();
        let simple_actor = ClockActor {
            count: count.clone(),
        };
        let universe = Universe::with_accelerated_time();
        universe.spawn_builder().spawn(simple_actor);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        universe.sleep(Duration::from_millis(15)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_scheduler_advance_time_fast_forward_scheduled_message() {
        let start = Instant::now();
        quickwit_common::setup_logging_for_tests();
        let count: Arc<AtomicUsize> = Default::default();
        let simple_actor = ClockActor {
            count: count.clone(),
        };
        let universe = Universe::with_accelerated_time();
        universe.spawn_builder().spawn(simple_actor);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        universe.sleep(Duration::from_secs(10)).await;
        assert_eq!(count.load(Ordering::SeqCst), 10);
        let elapsed = start.elapsed();
        // The whole point is to accelerate time.
        assert!(elapsed.as_millis() < 50);
        universe.assert_quit().await;
    }
}
