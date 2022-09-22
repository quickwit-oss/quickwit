// Copyright (C) 2022 Quickwit, Inc.
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

use core::fmt;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::Future;
use tokio::sync::oneshot::{self, Sender};
use tokio::task::JoinHandle;
use tracing::info;

use crate::{Actor, ActorContext, ActorExitStatus, Handler};

pub(crate) struct Callback(pub Pin<Box<dyn Future<Output = ()> + Sync + Send + 'static>>);

// A bug in the rustc requires wrapping Box<...> in order to use it as an argument in an async
// method. pub(crate) struct Callback(pub BoxFuture<'static, ()>);

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
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.event_id.cmp(&other.event_id))
    }
}

#[derive(Debug)]
pub enum TimeShift {
    ToInstant(Instant),
    ByDuration(Duration),
}

/// Schedules a callback to be executed after `timeout` is elapsed.
///
/// The callback is required to not block.
pub(crate) struct ScheduleEvent {
    pub(crate) timeout: Duration,
    pub(crate) callback: Callback,
}

/// Identifies elapsed events and triggers their associated callback.
#[derive(Debug)]
struct Timeout;

/// Advance through time.
/// `tx` is called once `time_shift` is entirely elapsed.
#[derive(Debug)]
pub(crate) struct SimulateAdvanceTime {
    pub(crate) time_shift: TimeShift,
    pub(crate) tx: oneshot::Sender<()>,
}

impl fmt::Debug for ScheduleEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ScheduleEvent")
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerCounters {
    num_pending_events: usize,
    total_num_events: u64,
}

#[derive(Default)]
pub(crate) struct Scheduler {
    event_id_generator: u64,
    simulated_time_shift: Duration,
    future_events: BinaryHeap<Reverse<TimeoutEvent>>,
    next_timeout: Option<JoinHandle<()>>,
}

impl Actor for Scheduler {
    type ObservableState = SchedulerCounters;

    fn yield_after_each_message(&self) -> bool {
        false
    }

    fn observable_state(&self) -> Self::ObservableState {
        SchedulerCounters {
            num_pending_events: self.future_events.len(),
            total_num_events: self.event_id_generator,
        }
    }

    fn name(&self) -> String {
        "Scheduler".to_string()
    }
}

#[async_trait]
impl Handler<SimulateAdvanceTime> for Scheduler {
    type Reply = ();

    async fn handle(
        &mut self,
        message: SimulateAdvanceTime,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.process_simulate_advance_time(message.time_shift, message.tx, ctx)
            .await;
        Ok(())
    }
}

#[async_trait]
impl Handler<ScheduleEvent> for Scheduler {
    type Reply = ();

    async fn handle(
        &mut self,
        message: ScheduleEvent,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.process_schedule_event(message.timeout, message.callback, ctx)
            .await;
        Ok(())
    }
}

#[async_trait]
impl Handler<Timeout> for Scheduler {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: Timeout,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.process_timeout(ctx).await;
        Ok(())
    }
}

impl Scheduler {
    async fn process_timeout(&mut self, ctx: &ActorContext<Self>) {
        let now = self.simulated_now();
        while let Some(next_evt) = self.find_next_event_before_now(now) {
            next_evt.0.await;
        }
        self.schedule_next_timeout(ctx);
    }

    async fn process_schedule_event(
        &mut self,
        timeout: Duration,
        callback: Callback,
        ctx: &ActorContext<Self>,
    ) {
        let new_evt_deadline = self.simulated_now() + timeout;
        let current_next_deadline = self.future_events.peek().map(|evt| evt.0.deadline);
        let is_new_next_deadline = current_next_deadline
            .map(|next_evt_deadline| new_evt_deadline < next_evt_deadline)
            .unwrap_or(true);
        let timeout_event = self.timeout_event(new_evt_deadline, callback);
        self.future_events.push(Reverse(timeout_event));
        if is_new_next_deadline {
            self.schedule_next_timeout(ctx);
        }
    }

    async fn process_simulate_advance_time(
        &mut self,
        time_shift: TimeShift,
        tx: Sender<()>,
        ctx: &ActorContext<Self>,
    ) {
        let now = self.simulated_now();
        let deadline = match time_shift {
            TimeShift::ToInstant(instant) => instant,
            TimeShift::ByDuration(duration) => now + duration,
        };
        if now > deadline {
            let _ = tx.send(());
            return;
        }
        if let Some(next_evt_before_deadline) = self.next_event_deadline().filter(|t| t < &deadline)
        {
            self.advance_by_duration(next_evt_before_deadline - now, ctx)
                .await;
            // We leave 100ms for actors to process their messages. A callback on process would not
            // work here, as callbacks might create extra messages in turn.
            // A good way could be to wait for the overall actors in the universe to be idle.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = ctx
                .send_self_message(SimulateAdvanceTime {
                    time_shift: TimeShift::ToInstant(deadline),
                    tx,
                })
                .await;
        } else {
            self.advance_by_duration(deadline - now, ctx).await;
            let _ = tx.send(());
        }
    }

    async fn advance_by_duration(&mut self, time_shift: Duration, ctx: &ActorContext<Self>) {
        info!(time_shift=?time_shift, "advance-time");
        self.simulated_time_shift += time_shift;
        self.process_timeout(ctx).await;
    }

    fn next_event_deadline(&self) -> Option<Instant> {
        self.future_events.peek().map(|rev| rev.0.deadline)
    }
    fn find_next_event_before_now(&mut self, simulated_now: Instant) -> Option<Callback> {
        let next_event_deadline = self.next_event_deadline()?;
        if next_event_deadline < simulated_now {
            self.future_events.pop().map(|rev| rev.0.callback)
        } else {
            None
        }
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

    fn schedule_next_timeout(&mut self, ctx: &ActorContext<Self>) {
        let simulated_now = self.simulated_now();
        let next_deadline_opt = self.future_events.peek().map(|evt| evt.0.deadline);
        let timeout = match next_deadline_opt {
            Some(next_deadline) if next_deadline <= simulated_now => Duration::default(),
            Some(next_deadline) => next_deadline - simulated_now,
            None => {
                // No event to schedule
                return;
            }
        };
        // The next event timeout is about to change. Let's cancel the previous scheduled event.
        if let Some(previous_join_handle) = self.next_timeout.take() {
            previous_join_handle.abort();
        }
        let self_mailbox = ctx.mailbox().clone();
        let new_join_handle: JoinHandle<()> = tokio::task::spawn(async move {
            if timeout.is_zero() {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(timeout).await;
            }
            // We ignore the send error here. The scheduler was just terminated
            let _ = self_mailbox.send_message(Timeout).await;
        });
        self.next_timeout = Some(new_join_handle);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::{Callback, Scheduler};
    use crate::scheduler::{ScheduleEvent, SchedulerCounters, SimulateAdvanceTime, TimeShift};
    use crate::Universe;

    fn create_test_callback() -> (Arc<AtomicBool>, Callback) {
        let cb_called = Arc::new(AtomicBool::default());
        let cb_called_clone = cb_called.clone();
        let callback = Callback(Box::pin(async move {
            cb_called_clone.store(true, Ordering::SeqCst);
        }));
        (cb_called, callback)
    }

    #[tokio::test]
    async fn test_scheduler_advance_time() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        // It might be a bit confusing. We spawn a scheduler like a regular actor to test it.
        // The scheduler is usually spawned from within the universe.
        let (scheduler_mailbox, scheduler_handler) =
            universe.spawn_builder().spawn(Scheduler::default());
        let (cb_called, callback) = create_test_callback();
        scheduler_mailbox
            .send_message(ScheduleEvent {
                timeout: Duration::from_secs(30),
                callback,
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!cb_called.load(Ordering::SeqCst));
        let (tx, _rx) = oneshot::channel();
        scheduler_mailbox
            .send_message(SimulateAdvanceTime {
                time_shift: TimeShift::ByDuration(Duration::from_secs(31)),
                tx,
            })
            .await
            .unwrap();
        let scheduler_counters: SchedulerCounters =
            scheduler_handler.process_pending_and_observe().await.state;
        assert_eq!(
            scheduler_counters,
            SchedulerCounters {
                total_num_events: 1,
                num_pending_events: 0
            }
        );
        assert!(cb_called.load(Ordering::SeqCst));
        scheduler_handler.quit().await;
    }

    #[tokio::test]
    async fn test_scheduler_simple() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (scheduler_mailbox, scheduler_handler) =
            universe.spawn_builder().spawn(Scheduler::default());
        let (cb_called1, callback1) = create_test_callback();
        let (cb_called2, callback2) = create_test_callback();
        scheduler_mailbox
            .send_message(ScheduleEvent {
                timeout: Duration::from_secs(20),
                callback: callback2,
            })
            .await
            .unwrap();
        scheduler_mailbox
            .send_message(ScheduleEvent {
                timeout: Duration::from_millis(2),
                callback: callback1,
            })
            .await
            .unwrap();
        let scheduler_counters = scheduler_handler.process_pending_and_observe().await.state;
        assert_eq!(
            scheduler_counters,
            SchedulerCounters {
                total_num_events: 2,
                num_pending_events: 2
            }
        );
        assert!(!cb_called1.load(Ordering::SeqCst));
        assert!(!cb_called2.load(Ordering::SeqCst));
        tokio::time::sleep(Duration::from_millis(10)).await;
        let scheduler_counters = scheduler_handler.process_pending_and_observe().await.state;
        assert_eq!(
            scheduler_counters,
            SchedulerCounters {
                total_num_events: 2,
                num_pending_events: 1
            }
        );
        assert!(cb_called1.load(Ordering::SeqCst));
        assert!(!cb_called2.load(Ordering::SeqCst));
        let (tx, _rx) = oneshot::channel::<()>();
        scheduler_mailbox
            .send_message(SimulateAdvanceTime {
                time_shift: TimeShift::ByDuration(Duration::from_secs(10)),
                tx,
            })
            .await
            .unwrap();
        assert!(cb_called1.load(Ordering::SeqCst));
        assert!(!cb_called2.load(Ordering::SeqCst));
        let (tx, _rx) = oneshot::channel::<()>();
        scheduler_mailbox
            .send_message(SimulateAdvanceTime {
                time_shift: TimeShift::ByDuration(Duration::from_secs(10)),
                tx,
            })
            .await
            .unwrap();
        let scheduler_counters: SchedulerCounters =
            scheduler_handler.process_pending_and_observe().await.state;
        assert!(cb_called1.load(Ordering::SeqCst));
        assert!(cb_called2.load(Ordering::SeqCst));
        assert_eq!(
            scheduler_counters,
            SchedulerCounters {
                total_num_events: 2,
                num_pending_events: 0
            }
        );
        scheduler_handler.quit().await;
    }
}
