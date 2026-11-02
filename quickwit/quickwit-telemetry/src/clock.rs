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
    /// Each tick waits for the cooldown. The telemetry loop ticks right after each request, so
    /// requests are at least a cooldown apart.
    Cooldown(Duration),
    #[cfg(test)]
    Manual(Mutex<Receiver<()>>),
}

impl Clock {
    #[cfg(test)]
    pub(crate) fn manual() -> (ClockButton, Clock) {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        (ClockButton(tx), Clock::Manual(Mutex::new(rx)))
    }

    pub(crate) async fn tick(&self) {
        match self {
            Clock::Cooldown(cooldown) => tokio::time::sleep(*cooldown).await,
            #[cfg(test)]
            Clock::Manual(channel) => {
                channel.lock().await.recv().await;
            }
        }
    }
}
