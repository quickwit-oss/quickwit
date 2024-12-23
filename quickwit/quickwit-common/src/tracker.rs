use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

use census::{Inventory, TrackedObject as InventoredObject};

pub type TrackedObject<T> = InventoredObject<RecordUnacknoledgedDrop<T>>;

#[derive(Clone)]
pub struct Tracker<T> {
    inner_inventory: Inventory<RecordUnacknoledgedDrop<T>>,
    unacknoledged_drop_receiver: Arc<Mutex<Receiver<T>>>,
    return_channel: Sender<T>,
}

#[derive(Debug)]
pub struct RecordUnacknoledgedDrop<T> {
    // safety: this is always kept initialized except after Self::drop, where we move that
    // that value away to either send it through the return channel, or drop it manually
    inner: MaybeUninit<T>,
    acknoledged: AtomicBool,
    return_channel: Sender<T>,
}

impl<T> RecordUnacknoledgedDrop<T> {
    pub fn acknoledge(&self) {
        self.acknoledged.store(true, Ordering::Relaxed);
    }

    pub fn untracked(value: T) -> Self {
        let (sender, _receiver) = channel();
        RecordUnacknoledgedDrop {
            inner: MaybeUninit::new(value),
            acknoledged: true.into(),
            return_channel: sender,
        }
    }
}

impl<T> Deref for RecordUnacknoledgedDrop<T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe {
            // safety: see struct definition, this operation is valid except after drop.
            self.inner.assume_init_ref()
        }
    }
}

impl<T> Drop for RecordUnacknoledgedDrop<T> {
    fn drop(&mut self) {
        let item = unsafe {
            // safety: see struct definition. Additionally, we don't touch to self.inner
            // after this point so there is no risk of making a 2nd copy and cause a
            // double-free
            self.inner.assume_init_read()
        };
        if !*self.acknoledged.get_mut() {
            // if send fails, no one cared about getting that notification, it's fine to
            // drop item
            let _ = self.return_channel.send(item);
        }
    }
}

impl<T> Default for Tracker<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Tracker<T> {
    pub fn new() -> Self {
        let (sender, receiver) = channel();
        Tracker {
            inner_inventory: Inventory::new(),
            unacknoledged_drop_receiver: Arc::new(Mutex::new(receiver)),
            return_channel: sender,
        }
    }

    pub fn rebuildable_from_the_void(&self) -> bool {
        Arc::strong_count(&self.unacknoledged_drop_receiver) == 1 && self.inner_inventory.len() == 0
    }

    pub fn list_ongoing(&self) -> Vec<TrackedObject<T>> {
        self.inner_inventory.list()
    }

    pub fn take_dead(&self) -> Vec<T> {
        let mut res = Vec::new();
        let receiver = self.unacknoledged_drop_receiver.lock().unwrap();
        while let Ok(dead_entry) = receiver.try_recv() {
            res.push(dead_entry);
        }
        res
    }

    pub fn track(&self, value: T) -> TrackedObject<T> {
        self.inner_inventory.track(RecordUnacknoledgedDrop {
            inner: MaybeUninit::new(value),
            acknoledged: false.into(),
            return_channel: self.return_channel.clone(),
        })
    }
}
