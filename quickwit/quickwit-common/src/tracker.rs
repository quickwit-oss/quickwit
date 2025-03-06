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

use std::ops::Deref;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

use census::{Inventory, TrackedObject as InventoredObject};

/// A ressource tracker
///
/// This is used to track whether an object is alive (still in use), or if it's dead (no longer
/// used, but not acknowledged). It does not keep any traces of object that were alive, but were
/// since acknowledged.
#[derive(Clone)]
pub struct Tracker<T: Clone> {
    inner_inventory: Inventory<T>,
    unacknowledged_drop_receiver: Arc<Mutex<Receiver<T>>>,
    return_channel: Sender<T>,
}

/// A single tracked object
#[derive(Debug)]
pub struct TrackedObject<T: Clone> {
    inner: Option<InventoredObject<T>>,
    return_channel: Sender<T>,
}

impl<T: Clone> TrackedObject<T> {
    /// acknoledge an object
    pub fn acknowledge(mut self) {
        self.inner.take();
    }

    /// Create an untracked object mostly for tests
    pub fn untracked(value: T) -> Self {
        Tracker::new().track(value)
    }

    /// Create an object which is tracked only as long as it's alive,
    /// but not once it's dead.
    /// The object is tracked through the provided census inventory
    pub fn track_alive_in(value: T, inventory: &Inventory<T>) -> Self {
        TrackedObject {
            inner: Some(inventory.track(value)),
            return_channel: channel().0,
        }
    }
}

impl<T: Clone> AsRef<T> for TrackedObject<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T: Clone> Deref for TrackedObject<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.inner
            .as_ref()
            .expect("inner should only be None during drop")
    }
}

impl<T: Clone> Drop for TrackedObject<T> {
    fn drop(&mut self) {
        if let Some(item) = self.inner.take() {
            // if send fails, no one cared about getting that notification, it's fine to
            // drop item
            let _ = self.return_channel.send(item.as_ref().clone());
        }
    }
}

impl<T: Clone> Default for Tracker<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> Tracker<T> {
    /// Create a new tracker
    pub fn new() -> Self {
        let (sender, receiver) = channel();
        Tracker {
            inner_inventory: Inventory::new(),
            unacknowledged_drop_receiver: Arc::new(Mutex::new(receiver)),
            return_channel: sender,
        }
    }

    /// Return whether it is safe to recreate this tracker.
    ///
    /// A tracker is considered safe to recreate if this is the only instance left,
    /// and it contains no alive object (it may contain dead objects though).
    ///
    /// Once this return true, it will stay that way until [Tracker::track] or [Tracker::clone] are
    /// called.
    pub fn safe_to_recreate(&self) -> bool {
        Arc::strong_count(&self.unacknowledged_drop_receiver) == 1
            && self.inner_inventory.len() == 0
    }

    /// List object which are considered alive
    pub fn list_ongoing(&self) -> Vec<InventoredObject<T>> {
        self.inner_inventory.list()
    }

    /// Take away the list of object considered dead
    pub fn take_dead(&self) -> Vec<T> {
        let mut res = Vec::new();
        let receiver = self.unacknowledged_drop_receiver.lock().unwrap();
        while let Ok(dead_entry) = receiver.try_recv() {
            res.push(dead_entry);
        }
        res
    }

    /// Track a new object.
    pub fn track(&self, value: T) -> TrackedObject<T> {
        TrackedObject {
            inner: Some(self.inner_inventory.track(value)),
            return_channel: self.return_channel.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InventoredObject, Tracker};

    #[track_caller]
    fn assert_tracked_eq<T: PartialEq + std::fmt::Debug>(
        got: Vec<InventoredObject<T>>,
        expected: Vec<T>,
    ) {
        assert_eq!(
            got.len(),
            expected.len(),
            "expected vec of same lenght, {} != {}",
            got.len(),
            expected.len()
        );
        for (got_item, expected_item) in got.into_iter().zip(expected) {
            assert_eq!(*got_item, expected_item);
        }
    }

    #[test]
    fn test_single_tracker() {
        let tracker = Tracker::<u32>::new();

        assert!(tracker.list_ongoing().is_empty());
        assert!(tracker.take_dead().is_empty());
        assert!(tracker.safe_to_recreate());

        {
            let tracked_1 = tracker.track(1);
            assert_tracked_eq(tracker.list_ongoing(), vec![1]);
            assert!(tracker.take_dead().is_empty());
            assert!(!tracker.safe_to_recreate());
            std::mem::drop(tracked_1); // done for clarity and silence unused var warn
        }

        assert!(tracker.list_ongoing().is_empty());
        assert!(tracker.safe_to_recreate());
        assert_eq!(tracker.take_dead(), vec![1]);
        assert!(tracker.safe_to_recreate());
    }

    #[test]
    fn test_two_tracker() {
        let tracker = Tracker::<u32>::new();
        let tracker2 = tracker.clone();

        assert!(tracker.list_ongoing().is_empty());
        assert!(tracker.take_dead().is_empty());
        assert!(!tracker.safe_to_recreate());

        {
            let tracked_1 = tracker.track(1);
            assert_tracked_eq(tracker.list_ongoing(), vec![1]);
            assert_tracked_eq(tracker2.list_ongoing(), vec![1]);
            assert!(tracker.take_dead().is_empty());
            assert!(tracker2.take_dead().is_empty());
            assert!(!tracker.safe_to_recreate());
            std::mem::drop(tracked_1); // done for clarity and silence unused var warn
        }

        assert!(tracker.list_ongoing().is_empty());
        assert!(tracker2.list_ongoing().is_empty());
        assert_eq!(tracker2.take_dead(), vec![1]);
        // we took awai the dead from tracker2, so they don't show up in tracker
        assert!(tracker.take_dead().is_empty());
    }
}
