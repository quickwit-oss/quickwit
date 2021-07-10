/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
use std::mem;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::Interval;
use tracing::info;

use crate::payload::{ClientInformation, EventWithTimestamp, TelemetryEvent, TelemetryPayload};
use crate::sink::HttpClient;
use crate::sink::Sink;

/// At most 1 Request per minutes.
const TELEMETRY_PUSH_COOLDOWN: Duration = Duration::from_secs(60);

/// Upon termination of the program, we send one last telemetry request with pending events.
/// This duration is the amount of time we wait for at most to send that last telemetry request.
const LAST_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

const DISABLE_TELEMETRY_ENV_KEY: &str = "DISABLE_QUICKWIT_TELEMETRY";

const MAX_NUM_EVENTS_IN_QUEUE: usize = 10;

#[cfg(test)]
struct ClockButton(Sender<()>);

#[cfg(test)]
impl ClockButton {
    async fn tick(&self) {
        let _ = self.0.send(()).await;
    }
}

enum Clock {
    Periodical(Mutex<Interval>),
    #[cfg(test)]
    Manual(Mutex<Receiver<()>>),
}

impl Clock {
    pub fn periodical(period: Duration) -> Clock {
        let interval = tokio::time::interval(period);
        Clock::Periodical(Mutex::new(interval))
    }

    #[cfg(test)]
    pub async fn manual() -> (ClockButton, Clock) {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let _ = tx.send(()).await;
        let button = ClockButton(tx);
        (button, Clock::Manual(Mutex::new(rx)))
    }

    async fn tick(&self) {
        match self {
            Clock::Periodical(interval) => {
                interval.lock().await.tick().await;
            }
            #[cfg(test)]
            Clock::Manual(channel) => {
                channel.lock().await.recv().await;
            }
        }
    }
}

#[derive(Default)]
struct EventsState {
    events: Vec<EventWithTimestamp>,
    num_dropped_events: usize,
}

impl EventsState {
    fn drain_events(&mut self) -> EventsState {
        mem::replace(
            self,
            EventsState {
                events: Vec::new(),
                num_dropped_events: 0,
            },
        )
    }

    /// Adds an event.
    /// If the queue is already saturated, (ie. it has reached the len `MAX_NUM_EVENTS_IN_QUEUE`)
    // Returns true iff it was the first event in the queue.
    fn push_event(&mut self, event: TelemetryEvent) -> bool {
        if self.events.len() >= MAX_NUM_EVENTS_IN_QUEUE {
            self.num_dropped_events += 1;
            return false;
        }
        let events_was_empty = self.events.is_empty();
        self.events.push(EventWithTimestamp::from(event));
        events_was_empty
    }
}

struct Events {
    state: RwLock<EventsState>,
    items_available_tx: Sender<()>,
    items_available_rx: RwLock<Receiver<()>>,
}

impl Default for Events {
    fn default() -> Self {
        let (items_available_tx, items_available_rx) = tokio::sync::mpsc::channel(1);
        Events {
            state: RwLock::new(EventsState::default()),
            items_available_tx,
            items_available_rx: RwLock::new(items_available_rx),
        }
    }
}

impl Events {
    /// Wait for events to be available (if there are pending events, then do not wait)
    /// and then send them to the PushAPI server.
    async fn drain_events(&self) -> EventsState {
        self.items_available_rx.write().await.recv().await;
        self.state.write().await.drain_events()
    }

    async fn push_event(&self, event: TelemetryEvent) {
        let is_first_event = self.state.write().await.push_event(event);
        if is_first_event {
            let _ = self.items_available_tx.send(()).await;
        }
    }
}

pub(crate) struct Inner {
    sink: Option<Box<dyn Sink>>,
    client_information: ClientInformation,
    /// This channel is just used to signal there are new items available.
    events: Events,
    clock: Clock,
    is_started: AtomicBool,
}

impl Inner {
    pub fn is_disabled(&self) -> bool {
        self.sink.is_none()
    }

    async fn create_telemetry_payload(&self) -> TelemetryPayload {
        let events_state = self.events.drain_events().await;
        TelemetryPayload {
            client_information: self.client_information.clone(),
            events: events_state.events,
            num_dropped_events: events_state.num_dropped_events,
        }
    }

    /// Wait for events to be available (if there are pending events, then do not wait)
    /// and then send them to the PushAPI server.
    ///
    /// If the requests fails, it fails silently.
    async fn send_pending_events(&self) {
        if let Some(sink) = self.sink.as_ref() {
            let payload = self.create_telemetry_payload().await;
            sink.send_payload(payload).await;
        }
    }

    async fn send(&self, event: TelemetryEvent) {
        if self.is_disabled() {
            return;
        }
        self.events.push_event(event).await;
    }
}

pub struct TelemetrySender {
    pub(crate) inner: Arc<Inner>,
}

pub enum TelemetryLoopHandle {
    NoLoop,
    WithLoop {
        join_handle: JoinHandle<()>,
        terminate_command_tx: oneshot::Sender<()>,
    },
}

impl TelemetryLoopHandle {
    /// Terminate telemetry will exit the telemetry loop
    /// and possibly send the last request, possibly ignoring the
    /// telemetry cooldown.
    pub async fn terminate_telemetry(self) {
        if let Self::WithLoop {
            join_handle,
            terminate_command_tx,
        } = self
        {
            let _ = terminate_command_tx.send(());
            let _ = tokio::time::timeout(LAST_REQUEST_TIMEOUT, join_handle).await;
        }
    }
}

impl TelemetrySender {
    fn new<S: Sink>(sink_opt: Option<S>, clock: Clock) -> TelemetrySender {
        let sink_opt: Option<Box<dyn Sink>> = if let Some(sink) = sink_opt {
            Some(Box::new(sink))
        } else {
            None
        };
        TelemetrySender {
            inner: Arc::new(Inner {
                sink: sink_opt,
                client_information: ClientInformation::default(),
                events: Events::default(),
                clock,
                is_started: AtomicBool::new(false),
            }),
        }
    }

    pub fn start_loop(&self) -> TelemetryLoopHandle {
        let (terminate_command_tx, mut terminate_command_rx) = oneshot::channel();
        if self.inner.is_disabled() {
            return TelemetryLoopHandle::NoLoop;
        }

        assert!(
            self.inner
                .is_started
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
            "The telemetry loop is already started."
        );

        let inner = self.inner.clone();
        let join_handle = tokio::task::spawn(async move {
            // This channel is used to send the command to terminate telemetry.
            loop {
                let quit_loop = tokio::select! {
                    _ = (&mut terminate_command_rx) => { true }
                    _ = inner.clock.tick() => { false }
                };
                let _ = inner.send_pending_events().await;
                if quit_loop {
                    break;
                }
            }
        });
        TelemetryLoopHandle::WithLoop {
            join_handle,
            terminate_command_tx,
        }
    }

    pub async fn send(&self, event: TelemetryEvent) {
        self.inner.send(event).await;
    }
}

/// Check to see if telemetry is enabled.
pub fn is_telemetry_enabled() -> bool {
    std::env::var_os(DISABLE_TELEMETRY_ENV_KEY).is_none()
}

fn create_http_client() -> Option<HttpClient> {
    // TODO add telemetry URL.
    let client_opt = if is_telemetry_enabled() {
        HttpClient::try_new()
    } else {
        None
    };
    if let Some(client) = client_opt.as_ref() {
        info!("telemetry to {} is enabled.", client.endpoint());
    } else {
        info!("telemetry to quickwit is disabled.");
    }
    client_opt
}

impl Default for TelemetrySender {
    fn default() -> Self {
        let http_client = create_http_client();
        TelemetrySender::new(http_client, Clock::periodical(TELEMETRY_PUSH_COOLDOWN))
    }
}

#[cfg(test)]
mod tests {

    use std::env;

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_enabling_and_disabling_telemetry() {
        // We group the two in a single test to ensure it happens on the same thread.
        env::set_var(super::DISABLE_TELEMETRY_ENV_KEY, "");
        assert_eq!(TelemetrySender::default().inner.is_disabled(), true);
        env::remove_var(super::DISABLE_TELEMETRY_ENV_KEY);
        assert_eq!(TelemetrySender::default().inner.is_disabled(), false);
    }

    #[tokio::test]
    async fn test_telemetry_no_wait_for_first_event() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (_clock_btn, clock) = Clock::manual().await;
        let telemetry_sender = TelemetrySender::new(Some(tx), clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::Create).await;
        let payload_opt = rx.recv().await;
        assert!(payload_opt.is_some());
        let payload = payload_opt.unwrap();
        assert_eq!(payload.events.len(), 1);
        loop_handler.terminate_telemetry().await;
    }

    #[tokio::test]
    async fn test_telemetry_two_events() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (clock_btn, clock) = Clock::manual().await;
        let telemetry_sender = TelemetrySender::new(Some(tx), clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::Create).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        clock_btn.tick().await;
        telemetry_sender.send(TelemetryEvent::Create).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        loop_handler.terminate_telemetry().await;
    }

    #[tokio::test]
    async fn test_telemetry_cooldown_observed() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (clock_btn, clock) = Clock::manual().await;
        let telemetry_sender = TelemetrySender::new(Some(tx), clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::Create).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        tokio::task::yield_now().await;
        telemetry_sender.send(TelemetryEvent::Create).await;

        let timeout_res = tokio::time::timeout(Duration::from_millis(1), rx.recv()).await;
        assert!(timeout_res.is_err());

        telemetry_sender.send(TelemetryEvent::Create).await;
        clock_btn.tick().await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 2);
        }
        loop_handler.terminate_telemetry().await;
    }

    #[tokio::test]
    async fn test_terminate_telemetry_sends_pending_events() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (_clock_btn, clock) = Clock::manual().await;
        let telemetry_sender = TelemetrySender::new(Some(tx), clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::Create).await;
        let payload = rx.recv().await.unwrap();
        assert_eq!(payload.events.len(), 1);
        telemetry_sender
            .send(TelemetryEvent::EndCommand { return_code: 2i32 })
            .await;
        loop_handler.terminate_telemetry().await;
        let payload = rx.recv().await.unwrap();
        assert_eq!(payload.events.len(), 1);
        assert!(matches!(
            &payload.events[0].event,
            &TelemetryEvent::EndCommand { .. }
        ));
    }
}
