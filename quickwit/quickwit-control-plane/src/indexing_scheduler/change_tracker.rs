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

use std::sync::Arc;

use tokio::sync::watch;

/// This object makes it possible to track for the completion of the next rebuild.
pub struct RebuildNotifier {
    generation_processed_tx: watch::Sender<usize>,
    generation_processed_rx: watch::Receiver<usize>,
    generation: usize,
}

impl Default for RebuildNotifier {
    fn default() -> Self {
        let (generation_processed_tx, generation_processed_rx) = watch::channel(0);

        Self {
            generation_processed_tx,
            generation_processed_rx,
            generation: 1,
        }
    }
}

impl RebuildNotifier {
    /// Returns a future that resolves when the next rebuild is completed.
    ///
    /// If an ongoing build T exists, it will not resolve upon build T's completion.
    /// It will only be resolved upon build T+1's completion, or any subsequent build.
    pub fn next_rebuild_waiter(&mut self) -> impl std::future::Future<Output = ()> + use<> {
        let mut generation_processed_rx = self.generation_processed_rx.clone();
        let current_generation = self.generation;
        async move {
            loop {
                if *generation_processed_rx.borrow() >= current_generation {
                    return;
                }
                if generation_processed_rx.changed().await.is_err() {
                    return;
                }
            }
        }
    }

    /// Starts a new rebuild.
    pub fn start_rebuild(&mut self) -> Arc<NotifyChangeOnDrop> {
        let generation = self.generation;
        self.generation += 1;
        Arc::new(NotifyChangeOnDrop {
            generation,
            generation_processed_tx: self.generation_processed_tx.clone(),
        })
    }
}

pub struct NotifyChangeOnDrop {
    generation: usize,
    generation_processed_tx: watch::Sender<usize>,
}

impl Drop for NotifyChangeOnDrop {
    fn drop(&mut self) {
        if self.generation < *self.generation_processed_tx.borrow() {
            return;
        }
        let _ = self.generation_processed_tx.send(self.generation);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_change_tracker() {
        let mut change_tracker = RebuildNotifier::default();
        let waiter = change_tracker.next_rebuild_waiter();
        let change_notifier = change_tracker.start_rebuild();
        drop(change_notifier);
        waiter.await;
    }

    #[tokio::test]
    async fn test_change_tracker_ongoing_is_not_good() {
        let mut change_tracker = RebuildNotifier::default();
        let change_notifier = change_tracker.start_rebuild();
        let waiter = change_tracker.next_rebuild_waiter();
        let waiter2 = change_tracker.next_rebuild_waiter();
        drop(change_notifier);
        let change_notifier2 = change_tracker.start_rebuild();
        let timeout_res = tokio::time::timeout(Duration::from_millis(100), waiter).await;
        assert!(timeout_res.is_err());
        drop(change_notifier2);
        waiter2.await;
    }

    #[tokio::test]
    async fn test_change_tracker_all_waiters_are_notified() {
        let mut change_tracker = RebuildNotifier::default();
        let waiter = change_tracker.next_rebuild_waiter();
        let waiter2 = change_tracker.next_rebuild_waiter();
        let change_notifier = change_tracker.start_rebuild();
        drop(change_notifier);
        waiter.await;
        waiter2.await;
    }
}
