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
use std::sync::Mutex;

use flume::TryRecvError;
use thiserror::Error;

#[derive(Default)]
struct LockedOption<T> {
    opt: Mutex<Option<T>>,
    has_val: AtomicBool,
}

impl<T> LockedOption<T> {
    pub fn none() -> Self {
        LockedOption {
            opt: Mutex::new(None),
            has_val: AtomicBool::new(false),
        }
    }

    pub fn is_some(&self) -> bool {
        self.has_val.load(Ordering::Acquire)
    }

    pub fn is_none(&self) -> bool {
        !self.is_some()
    }

    pub fn take(&self) -> Option<T> {
        if !self.has_val.load(Ordering::Acquire) {
            return None;
        }
        let mut lock = self.opt.lock().unwrap();
        let val_opt = lock.take();
        self.has_val.store(false, Ordering::Release);
        val_opt
    }

    pub fn place(&self, val: T) {
        let mut lock = self.opt.lock().unwrap();
        self.has_val.store(true, Ordering::Release);
        *lock = Some(val);
    }
}

#[derive(Debug, Error)]
pub enum SendError {
    #[error("the channel is closed")]
    Disconnected,
    #[error("the channel is full")]
    Full,
}

#[derive(Debug, Error)]
pub enum TrySendError<M> {
    #[error("the channel is closed")]
    Disconnected,
    #[error("the channel is full")]
    Full(M),
}

impl<M> From<flume::TrySendError<M>> for TrySendError<M> {
    fn from(err: flume::TrySendError<M>) -> Self {
        match err {
            flume::TrySendError::Full(msg) => TrySendError::Full(msg),
            flume::TrySendError::Disconnected(_) => TrySendError::Disconnected,
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RecvError {
    #[error("no message are currently available")]
    NoMessageAvailable,
    #[error("all senders were dropped and no pending messages are in the channel")]
    Disconnected,
}

impl From<flume::RecvTimeoutError> for RecvError {
    fn from(flume_err: flume::RecvTimeoutError) -> Self {
        match flume_err {
            flume::RecvTimeoutError::Timeout => Self::NoMessageAvailable,
            flume::RecvTimeoutError::Disconnected => Self::Disconnected,
        }
    }
}

impl<T> From<flume::SendError<T>> for SendError {
    fn from(_send_error: flume::SendError<T>) -> Self {
        SendError::Disconnected
    }
}

impl<T> From<flume::TrySendError<T>> for SendError {
    fn from(try_send_error: flume::TrySendError<T>) -> Self {
        match try_send_error {
            flume::TrySendError::Full(_) => SendError::Full,
            flume::TrySendError::Disconnected(_) => SendError::Disconnected,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum QueueCapacity {
    Bounded(usize),
    Unbounded,
}

/// Creates a channel with the ability to send high priority messages.
///
/// A high priority message is guaranteed to be consumed before any
/// low priority message sent after it.
pub fn channel<T>(queue_capacity: QueueCapacity) -> (Sender<T>, Receiver<T>) {
    let (high_priority_tx, high_priority_rx) = flume::unbounded();
    let (low_priority_tx, low_priority_rx) = match queue_capacity {
        QueueCapacity::Bounded(cap) => flume::bounded(cap),
        QueueCapacity::Unbounded => flume::unbounded(),
    };
    let receiver = Receiver {
        low_priority_rx,
        high_priority_rx,
        _high_priority_tx: high_priority_tx.clone(),
        pending_low_priority_message: LockedOption::none(),
        _clone_is_forbidden: CloneIsForbidden,
    };
    let sender = Sender {
        low_priority_tx,
        high_priority_tx,
    };
    (sender, receiver)
}

pub struct Sender<T> {
    low_priority_tx: flume::Sender<T>,
    high_priority_tx: flume::Sender<T>,
}

impl<T> Sender<T> {
    pub fn is_disconnected(&self) -> bool {
        self.low_priority_tx.is_disconnected()
    }

    pub fn try_send_low_priority(&self, msg: T) -> Result<(), TrySendError<T>> {
        self.low_priority_tx.try_send(msg)?;
        Ok(())
    }

    pub async fn send_low_priority(&self, msg: T) -> Result<(), SendError> {
        self.low_priority_tx.send_async(msg).await?;
        Ok(())
    }

    pub fn send_high_priority(&self, msg: T) -> Result<(), SendError> {
        self.high_priority_tx.send(msg)?;
        Ok(())
    }
}

// Message to future generations. I created this flag to prevent you
// from naively making a struct cloneable.
// The drop implementation drains the elements in the channel.
struct CloneIsForbidden;

pub struct Receiver<T> {
    low_priority_rx: flume::Receiver<T>,
    high_priority_rx: flume::Receiver<T>,
    _high_priority_tx: flume::Sender<T>,
    pending_low_priority_message: LockedOption<T>,
    _clone_is_forbidden: CloneIsForbidden,
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // Flume strangely (tokio::mpsc does not behave like this for instance)
        // does not drop the message in the channel when all receiver are dropped.
        //
        // They are only dropped when both the receivers AND the sender are dropped.
        // We fix this behavior by drainng the channel upon drop.
        self.high_priority_rx.drain();
        self.low_priority_rx.drain();
    }
}

impl<T> Receiver<T> {
    pub fn is_empty(&self) -> bool {
        self.low_priority_rx.is_empty()
            && self.pending_low_priority_message.is_none()
            && self.high_priority_rx.is_empty()
    }

    pub fn try_recv_high_priority_message(&self) -> Result<T, RecvError> {
        match self.high_priority_rx.try_recv() {
            Ok(msg) => Ok(msg),
            Err(TryRecvError::Disconnected) => {
                unreachable!(
                    "This can never happen, as the high priority Sender is owned by the Receiver."
                );
            }
            Err(TryRecvError::Empty) => {
                if self.low_priority_rx.is_disconnected() {
                    // We check that no new high priority message were sent
                    // in between.
                    if let Ok(msg) = self.high_priority_rx.try_recv() {
                        Ok(msg)
                    } else {
                        Err(RecvError::Disconnected)
                    }
                } else {
                    Err(RecvError::NoMessageAvailable)
                }
            }
        }
    }

    pub fn try_recv(&self) -> Result<T, RecvError> {
        if let Ok(msg) = self.high_priority_rx.try_recv() {
            return Ok(msg);
        }
        if let Some(pending_msg) = self.pending_low_priority_message.take() {
            return Ok(pending_msg);
        }
        match self.low_priority_rx.try_recv() {
            Ok(low_msg) => {
                if let Ok(high_msg) = self.high_priority_rx.try_recv() {
                    self.pending_low_priority_message.place(low_msg);
                    Ok(high_msg)
                } else {
                    Ok(low_msg)
                }
            }
            Err(TryRecvError::Disconnected) => {
                if let Ok(high_msg) = self.high_priority_rx.try_recv() {
                    Ok(high_msg)
                } else {
                    Err(RecvError::Disconnected)
                }
            }
            Err(TryRecvError::Empty) => Err(RecvError::NoMessageAvailable),
        }
    }

    pub async fn recv_high_priority(&self) -> T {
        self.high_priority_rx
            .recv_async()
            .await
            .expect("The Receiver owns the high priority Sender to avoid any disconnection.")
    }

    pub async fn recv(&self) -> Result<T, RecvError> {
        if let Ok(msg) = self.try_recv_high_priority_message() {
            return Ok(msg);
        }
        if let Some(pending_msg) = self.pending_low_priority_message.take() {
            return Ok(pending_msg);
        }
        tokio::select! {
            // We don't really care about fairness here.
            // We will double check if there is a command or not anyway.
            biased;
            high_priority_msg_res = self.high_priority_rx.recv_async() => {
                match high_priority_msg_res {
                    Ok(high_priority_msg) => {
                        Ok(high_priority_msg)
                    },
                    Err(_) => {
                        unreachable!("The Receiver owns the high priority Sender to avoid any disconnection.")
                    },
                }
            }
            low_priority_msg_res = self.low_priority_rx.recv_async() => {
                match low_priority_msg_res {
                    Ok(low_priority_msg) => {
                        if let Ok(high_priority_msg) = self.try_recv_high_priority_message() {
                            self.pending_low_priority_message.place(low_priority_msg);
                            Ok(high_priority_msg)
                        } else {
                            Ok(low_priority_msg)
                        }
                    },
                    Err(flume::RecvError::Disconnected) => {
                        if let Ok(high_priority_msg) = self.try_recv_high_priority_message() {
                            Ok(high_priority_msg)
                        } else {
                            Err(RecvError::Disconnected)
                        }
                    }
                }
           }
        }
    }

    /// Drain all of the pending low priority messages and return them.
    pub fn drain_low_priority(&self) -> Vec<T> {
        let mut messages = Vec::new();
        while let Ok(msg) = self.low_priority_rx.try_recv() {
            messages.push(msg);
        }
        messages
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_channel_with_priority_drop_receiver_drop_messages() {
        let arc_high = Arc::new(());
        let arc_low = Arc::new(());
        let (tx, rx) = super::channel(QueueCapacity::Bounded(2));
        tx.send_high_priority(arc_high.clone()).unwrap();
        tx.send_low_priority(arc_low.clone()).await.unwrap();
        assert_eq!(Arc::strong_count(&arc_high), 2);
        assert_eq!(Arc::strong_count(&arc_low), 2);
        drop(rx);
        assert_eq!(Arc::strong_count(&arc_high), 1);
        assert_eq!(Arc::strong_count(&arc_low), 1);
    }

    #[test]
    fn test_locked_option_new_empty() {
        let locked_option: LockedOption<usize> = LockedOption::none();
        assert_eq!(locked_option.take(), None);
    }

    #[test]
    fn test_locked_option_place() {
        let locked_option = LockedOption::none();
        locked_option.place(1);
        assert_eq!(locked_option.take(), Some(1));
    }

    #[test]
    fn test_locked_option_place_twice_keep_last() {
        let locked_option = LockedOption::none();
        locked_option.place(1);
        locked_option.place(2);
        assert_eq!(locked_option.take(), Some(2));
    }

    #[test]
    fn test_locked_option_place_take_twice() {
        let locked_option = LockedOption::none();
        locked_option.place(1);
        assert_eq!(locked_option.take(), Some(1));
        assert_eq!(locked_option.take(), None);
    }

    #[tokio::test]
    async fn test_recv_priority() -> anyhow::Result<()> {
        let (sender, receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        sender.send_low_priority(1).await?;
        sender.send_high_priority(2)?;
        assert_eq!(receiver.recv().await, Ok(2));
        assert_eq!(receiver.recv().await, Ok(1));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), receiver.recv())
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_try_recv() -> anyhow::Result<()> {
        let (sender, receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        sender.send_low_priority(1).await?;
        assert_eq!(receiver.try_recv(), Ok(1));
        assert_eq!(receiver.try_recv(), Err(RecvError::NoMessageAvailable));
        Ok(())
    }

    #[tokio::test]
    async fn test_try_recv_high_priority() -> anyhow::Result<()> {
        let (sender, receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        sender.send_low_priority(1).await?;
        assert_eq!(
            receiver.try_recv_high_priority_message(),
            Err(RecvError::NoMessageAvailable)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_recv_high_priority_ignore_disconnection() -> anyhow::Result<()> {
        let (sender, receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        std::mem::drop(sender);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv_high_priority())
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_recv_disconnect() -> anyhow::Result<()> {
        let (sender, receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        std::mem::drop(sender);
        assert_eq!(receiver.recv().await, Err(RecvError::Disconnected));
        Ok(())
    }

    #[tokio::test]
    async fn test_recv_timeout_simple() -> anyhow::Result<()> {
        let (_sender, receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        assert!(matches!(
            receiver.try_recv(),
            Err(RecvError::NoMessageAvailable)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_try_recv_priority_corner_case() -> anyhow::Result<()> {
        let (sender, receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            sender.send_high_priority(1)?;
            sender.send_low_priority(2).await?;
            Result::<(), SendError>::Ok(())
        });
        assert_eq!(receiver.recv().await, Ok(1));
        assert_eq!(receiver.try_recv(), Ok(2));
        assert!(matches!(receiver.try_recv(), Err(RecvError::Disconnected)));
        Ok(())
    }

    #[tokio::test]
    async fn test_try_recv_high_low() {
        let (tx, rx) = super::channel::<usize>(QueueCapacity::Unbounded);
        tx.send_low_priority(1).await.unwrap();
        tx.send_high_priority(2).unwrap();
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Err(RecvError::NoMessageAvailable));
    }

    #[tokio::test]
    async fn test_try_recv_high() {
        let (tx, rx) = super::channel::<usize>(QueueCapacity::Unbounded);
        tx.send_low_priority(1).await.unwrap();
        tx.send_high_priority(2).unwrap();
        assert_eq!(rx.try_recv_high_priority_message(), Ok(2));
        assert_eq!(
            rx.try_recv_high_priority_message(),
            Err(RecvError::NoMessageAvailable)
        );
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Err(RecvError::NoMessageAvailable));
    }
}
