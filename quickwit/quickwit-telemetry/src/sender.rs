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

use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::Interval;
use tracing::info;

use crate::payload::{
    ClientInfo, EventWithTimestamp, QuickwitTelemetryInfo, TelemetryEvent, TelemetryPayload,
};
use crate::sink::{HttpClient, Sink};

/// At most 1 Request per minutes.
const TELEMETRY_PUSH_COOLDOWN: Duration = Duration::from_secs(60);

/// Interval at which to send telemetry `Running` event.
const TELEMETRY_RUNNING_EVENT_INTERVAL: Duration =
    Duration::from_secs(if cfg!(test) { 3 } else { 60 * 60 * 12 }); // 12h

/// Upon termination of the program, we send one last telemetry request with pending events.
/// This duration is the amount of time we wait for at most to send that last telemetry request.
const LAST_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

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
    /// and then send them to the ingest API server.
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
    client_info: ClientInfo,
    quickwit_info: QuickwitTelemetryInfo,
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
            client_info: self.client_info.clone(),
            quickwit_info: self.quickwit_info.clone(),
            events: events_state.events,
            num_dropped_events: events_state.num_dropped_events,
        }
    }

    /// Wait for events to be available (if there are pending events, then do not wait)
    /// and then send them to the ingest API server.
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
    pub fn from_quickwit_info(quickwit_info: QuickwitTelemetryInfo) -> Self {
        let http_client = create_http_client();
        TelemetrySender::new(
            quickwit_info,
            http_client,
            Clock::periodical(TELEMETRY_PUSH_COOLDOWN),
        )
    }

    fn new<S: Sink>(
        quickwit_info: QuickwitTelemetryInfo,
        sink_opt: Option<S>,
        clock: Clock,
    ) -> Self {
        let sink_opt: Option<Box<dyn Sink>> = if let Some(sink) = sink_opt {
            Some(Box::new(sink))
        } else {
            None
        };
        Self {
            inner: Arc::new(Inner {
                sink: sink_opt,
                client_info: ClientInfo::default(),
                quickwit_info,
                events: Events::default(),
                clock,
                is_started: AtomicBool::new(false),
            }),
        }
    }

    pub fn loop_started(&self) -> bool {
        self.inner.is_started.load(Ordering::Relaxed)
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
        start_monitor_if_server_running_task(inner.clone());
        let join_handle = tokio::task::spawn(async move {
            // This channel is used to send the command to terminate telemetry.
            loop {
                let quit_loop = tokio::select! {
                    _ = (&mut terminate_command_rx) => { true }
                    _ = inner.clock.tick() => { false }
                };
                inner.send_pending_events().await;
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

/// telemetry is disabled in tests.
#[cfg(test)]
pub fn is_telemetry_disabled() -> bool {
    true
}
/// Check to see if telemetry is enabled.
#[cfg(not(test))]
pub fn is_telemetry_disabled() -> bool {
    quickwit_common::get_bool_from_env(crate::DISABLE_TELEMETRY_ENV_KEY, false)
}

fn start_monitor_if_server_running_task(telemetry_sender: Arc<Inner>) {
    let mut clock = tokio::time::interval(TELEMETRY_RUNNING_EVENT_INTERVAL);
    tokio::spawn(async move {
        // Drop the first immediate tick.
        clock.tick().await;
        loop {
            clock.tick().await;
            telemetry_sender.send(TelemetryEvent::Running).await;
        }
    });
}

fn create_http_client() -> Option<HttpClient> {
    if is_telemetry_disabled() {
        info!("telemetry to quickwit is disabled");
        return None;
    }
    let client = HttpClient::try_new()?;
    info!("telemetry to {} is enabled", client.endpoint());
    Some(client)
}

#[cfg(test)]
mod tests {

    use std::env;

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_enabling_and_disabling_telemetry() {
        // We group the two in a single test to ensure it happens on the same thread.
        env::set_var(crate::DISABLE_TELEMETRY_ENV_KEY, "");
        assert_eq!(
            TelemetrySender::from_quickwit_info(QuickwitTelemetryInfo::default())
                .inner
                .is_disabled(),
            true
        );
        env::remove_var(crate::DISABLE_TELEMETRY_ENV_KEY);
        assert_eq!(
            TelemetrySender::from_quickwit_info(QuickwitTelemetryInfo::default())
                .inner
                .is_disabled(),
            false
        );
    }

    #[tokio::test]
    async fn test_telemetry_no_wait_for_first_event() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (_clock_btn, clock) = Clock::manual().await;
        let telemetry_sender =
            TelemetrySender::new(QuickwitTelemetryInfo::default(), Some(tx), clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
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
        let telemetry_sender =
            TelemetrySender::new(QuickwitTelemetryInfo::default(), Some(tx), clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        clock_btn.tick().await;
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        loop_handler.terminate_telemetry().await;
    }

    #[tokio::test]
    async fn test_telemetry_uptime_events() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (clock_btn, clock) = Clock::manual().await;
        let telemetry_sender =
            TelemetrySender::new(QuickwitTelemetryInfo::default(), Some(tx), clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        clock_btn.tick().await;
        tokio::time::sleep(TELEMETRY_RUNNING_EVENT_INTERVAL + Duration::from_secs(1)).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
            assert_eq!(payload.events[0].event, TelemetryEvent::Running);
        }
        loop_handler.terminate_telemetry().await;
    }

    #[tokio::test]
    async fn test_telemetry_cooldown_observed() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (clock_btn, clock) = Clock::manual().await;
        let telemetry_sender =
            TelemetrySender::new(QuickwitTelemetryInfo::default(), Some(tx), clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        tokio::task::yield_now().await;
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;

        let timeout_res = tokio::time::timeout(Duration::from_millis(1), rx.recv()).await;
        assert!(timeout_res.is_err());

        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
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
        let telemetry_sender =
            TelemetrySender::new(QuickwitTelemetryInfo::default(), Some(tx), clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
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
