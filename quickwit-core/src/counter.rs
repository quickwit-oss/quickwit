
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;


#[derive(Debug, Default)]
pub struct AtomicCounter(AtomicUsize);

impl AtomicCounter {
    pub fn inc(&self) -> usize {
        self.add(1)
    }

    pub fn add(&self, amount: usize) -> usize {
        self.0.fetch_add(amount, Ordering::Relaxed)
    }

    pub fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    pub fn reset(&self) -> usize {
        self.0.swap(0, Ordering::Relaxed)
    }
}
