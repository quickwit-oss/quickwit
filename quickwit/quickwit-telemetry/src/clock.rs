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

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[cfg(test)]
use tokio::sync::Mutex;
#[cfg(test)]
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
pub(crate) struct ClockButton(Sender<()>);

#[cfg(test)]
impl ClockButton {
    pub(crate) async fn tick(&self) {
        let _ = self.0.send(()).await;
    }
}

pub(crate) enum Clock {
    /// The first tick is immediate. Every later tick waits for the cooldown from the moment it is
    /// awaited. The telemetry loop awaits a tick right after each request, so requests are at
    /// least a cooldown apart, however long the loop waited for events in between.
    Cooldown {
        cooldown: Duration,
        is_first_tick: AtomicBool,
    },
    #[cfg(test)]
    Manual(Mutex<Receiver<()>>),
}

impl Clock {
    pub(crate) fn cooldown(cooldown: Duration) -> Clock {
        Clock::Cooldown {
            cooldown,
            is_first_tick: AtomicBool::new(true),
        }
    }

    #[cfg(test)]
    pub(crate) async fn manual() -> (ClockButton, Clock) {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let _ = tx.send(()).await;
        let button = ClockButton(tx);
        (button, Clock::Manual(Mutex::new(rx)))
    }

    pub(crate) async fn tick(&self) {
        match self {
            Clock::Cooldown {
                cooldown,
                is_first_tick,
            } => {
                if !is_first_tick.swap(false, Ordering::Relaxed) {
                    tokio::time::sleep(*cooldown).await;
                }
            }
            #[cfg(test)]
            Clock::Manual(channel) => {
                channel.lock().await.recv().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COOLDOWN: Duration = Duration::from_secs(60);

    #[tokio::test(start_paused = true)]
    async fn test_cooldown_clock_waits_for_cooldown_after_idle_time() {
        let clock = Clock::cooldown(COOLDOWN);
        let first_tick_start = tokio::time::Instant::now();
        clock.tick().await;
        assert_eq!(first_tick_start.elapsed(), Duration::ZERO);

        tokio::time::sleep(10 * COOLDOWN).await;
        let tick_start = tokio::time::Instant::now();
        clock.tick().await;
        assert!(tick_start.elapsed() >= COOLDOWN);
    }
}
