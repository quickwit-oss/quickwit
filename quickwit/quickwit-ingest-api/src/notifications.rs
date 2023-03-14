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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::sync::Mutex;

/// Registry for the index positions that are waiting to be notified when index commit occurs.
#[derive(Clone, Default)]
pub struct Notifications {
    notifications: Arc<Mutex<HashMap<String, VecDeque<Position>>>>,
}

impl Notifications {
    /// Create a new notification registry
    pub fn new() -> Self {
        Self {
            notifications: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register index positions
    pub async fn register(
        &self,
        index_positions: Vec<(String, u64)>,
        notify: impl FnOnce() + Send + Sync + 'static,
    ) {
        let mut guard = self.notifications.lock().await;
        let notification = Arc::new(Notification::new(notify));
        for index_position in index_positions {
            let positions = guard
                .entry(index_position.0.clone())
                .or_insert_with(VecDeque::new);
            positions.push_back(Position {
                position: index_position.1,
                notification: notification.clone(),
            });
        }
    }

    /// Notify positions
    pub async fn notify(&self, index: &String, max_position: u64) {
        let mut map = self.notifications.lock().await;
        if let Some(positions) = map.get_mut(index) {
            while let Some(position) = positions.front() {
                if position.position <= max_position {
                    positions
                        .pop_front()
                        .unwrap()
                        .decrement_count_and_notify_if_last();
                } else {
                    break;
                }
            }
            if positions.is_empty() {
                map.remove(index);
            }
        }
    }
}

impl Notification {
    fn new(notify: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            notify: Box::new(notify),
        }
    }
}

struct Position {
    position: u64,
    notification: Arc<Notification>,
}

impl Position {
    /// Reduces the notification's Arc count and notifies when if self has the only pointer.
    fn decrement_count_and_notify_if_last(self) {
        // Errors are allowed here, it simply means theare are still some positions that
        // were not notified
        let _ = Arc::try_unwrap(self.notification).map(|notification| notification.notify());
    }
}

struct Notification {
    notify: Box<dyn FnOnce() + Send + Sync + 'static>,
}

impl Notification {
    fn notify(self) {
        (self.notify)();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use crate::notifications::Notifications;

    #[tokio::test]
    async fn test_notifications() {
        let notifications = Notifications::new();
        let cleared = Arc::new(AtomicUsize::default());
        let cleared_clone = cleared.clone();
        notifications
            .register(vec![("index1".to_string(), 10)], move || {
                assert_eq!(
                    cleared_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    0
                );
            })
            .await;
        let cleared_clone = cleared.clone();
        notifications
            .register(vec![("index2".to_string(), 10)], move || {
                assert_eq!(
                    cleared_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    1
                );
            })
            .await;
        let cleared_clone = cleared.clone();
        notifications
            .register(
                vec![("index1".to_string(), 20), ("index1".to_string(), 30)],
                move || {
                    assert_eq!(
                        cleared_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                        2
                    );
                },
            )
            .await;
        assert_eq!(cleared.load(std::sync::atomic::Ordering::Relaxed), 0);
        notifications.notify(&"index1".to_string(), 20).await;
        assert_eq!(cleared.load(std::sync::atomic::Ordering::Relaxed), 1);
        notifications.notify(&"index2".to_string(), 100).await;
        assert_eq!(cleared.load(std::sync::atomic::Ordering::Relaxed), 2);
        notifications.notify(&"index1".to_string(), 100).await;
        assert_eq!(cleared.load(std::sync::atomic::Ordering::Relaxed), 3);
    }
}
