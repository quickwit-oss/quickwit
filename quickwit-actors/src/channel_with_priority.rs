// Copyright (C) 2022 Quickwit, Inc.
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

use std::time::Duration;

use flume::TryRecvError;
use thiserror::Error;
use tokio::time::timeout;

#[derive(Debug, Error)]
pub enum SendError {
    #[error("The channel is closed.")]
    Disconnected,
    #[error("The channel is full.")]
    Full,
}

#[derive(Debug, Error, Copy, Clone, PartialEq, Eq)]
pub enum RecvError {
    #[error("No message are currently available.")]
    NoMessageAvailable,
    #[error("All sender were dropped and no pending messages are in the channel.")]
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Priority {
    High,
    Low,
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
        pending_low_priority_message: None,
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
    fn channel(&self, priority: Priority) -> &flume::Sender<T> {
        match priority {
            Priority::High => &self.high_priority_tx,
            Priority::Low => &self.low_priority_tx,
        }
    }

    pub async fn send(&self, msg: T, priority: Priority) -> Result<(), SendError> {
        self.channel(priority).send_async(msg).await?;
        Ok(())
    }
}

pub struct Receiver<T> {
    low_priority_rx: flume::Receiver<T>,
    high_priority_rx: flume::Receiver<T>,
    _high_priority_tx: flume::Sender<T>,
    pending_low_priority_message: Option<T>,
}

impl<T> Receiver<T> {
    pub fn try_recv_high_priority_message(&mut self) -> Result<T, RecvError> {
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

    #[allow(dead_code)] // temporary
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        if let Ok(msg) = self.high_priority_rx.try_recv() {
            return Ok(msg);
        }
        if let Some(pending_msg) = self.pending_low_priority_message.take() {
            return Ok(pending_msg);
        }
        match self.low_priority_rx.try_recv() {
            Ok(low_msg) => {
                if let Ok(high_msg) = self.high_priority_rx.try_recv() {
                    self.pending_low_priority_message = Some(low_msg);
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

    pub async fn recv_high_priority_timeout(&mut self, duration: Duration) -> Result<T, RecvError> {
        match timeout(duration, self.high_priority_rx.recv_async()).await {
            Ok(Ok(high_priority_msg)) => Ok(high_priority_msg),
            Ok(Err(flume::RecvError::Disconnected)) => {
                panic!("The Receiver owns the high priority Sender to avoid any disconnection.");
            }
            Err(_elapsed) => Err(RecvError::NoMessageAvailable),
        }
    }

    pub async fn recv_timeout(&mut self, duration: Duration) -> Result<T, RecvError> {
        if let Ok(msg) = self.try_recv_high_priority_message() {
            return Ok(msg);
        }
        if let Some(pending_msg) = self.pending_low_priority_message.take() {
            return Ok(pending_msg);
        }
        tokio::select! {
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
                            self.pending_low_priority_message = Some(low_priority_msg);
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
            _ = tokio::time::sleep(duration) => {
                Err(RecvError::NoMessageAvailable)
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
    use std::time::{Duration, Instant};

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_millis(100);

    #[tokio::test]
    async fn test_recv_timeout_prority() -> anyhow::Result<()> {
        let (sender, mut receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        sender.send(1, Priority::Low).await?;
        sender.send(2, Priority::High).await?;
        assert_eq!(receiver.recv_timeout(TEST_TIMEOUT).await, Ok(2));
        assert_eq!(receiver.recv_timeout(TEST_TIMEOUT).await, Ok(1));
        assert_eq!(
            receiver.recv_timeout(TEST_TIMEOUT).await,
            Err(RecvError::NoMessageAvailable)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_recv_high_priority_timeout() -> anyhow::Result<()> {
        let (sender, mut receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        sender.send(1, Priority::Low).await?;
        assert_eq!(
            receiver.recv_high_priority_timeout(TEST_TIMEOUT).await,
            Err(RecvError::NoMessageAvailable)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_recv_high_priority_ignore_disconnection() -> anyhow::Result<()> {
        let (sender, mut receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        std::mem::drop(sender);
        assert_eq!(
            receiver.recv_high_priority_timeout(TEST_TIMEOUT).await,
            Err(RecvError::NoMessageAvailable)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_recv_disconnect() -> anyhow::Result<()> {
        let (sender, mut receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        std::mem::drop(sender);
        assert_eq!(
            receiver.recv_timeout(TEST_TIMEOUT).await,
            Err(RecvError::Disconnected)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_recv_timeout_simple() -> anyhow::Result<()> {
        let (_sender, mut receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        let start_time = Instant::now();
        assert_eq!(
            receiver.recv_timeout(TEST_TIMEOUT).await,
            Err(RecvError::NoMessageAvailable)
        );
        let elapsed = start_time.elapsed();
        assert!(elapsed < crate::HEARTBEAT);
        Ok(())
    }

    #[tokio::test]
    async fn test_try_recv_prority_corner_case() -> anyhow::Result<()> {
        let (sender, mut receiver) = super::channel::<usize>(QueueCapacity::Unbounded);
        tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            sender.send(1, Priority::High).await?;
            sender.send(2, Priority::Low).await?;
            Result::<(), SendError>::Ok(())
        });
        assert_eq!(receiver.recv_timeout(TEST_TIMEOUT).await, Ok(1));
        assert_eq!(receiver.recv_timeout(TEST_TIMEOUT).await, Ok(2));
        assert_eq!(
            receiver.recv_timeout(TEST_TIMEOUT).await,
            Err(RecvError::Disconnected)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_try_recv() {
        let (tx, mut rx) = super::channel::<usize>(QueueCapacity::Unbounded);
        tx.send(1, Priority::Low).await.unwrap();
        tx.send(2, Priority::High).await.unwrap();
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Err(RecvError::NoMessageAvailable));
    }

    #[tokio::test]
    async fn test_try_recv_high() {
        let (tx, mut rx) = super::channel::<usize>(QueueCapacity::Unbounded);
        tx.send(1, Priority::Low).await.unwrap();
        tx.send(2, Priority::High).await.unwrap();
        assert_eq!(rx.try_recv_high_priority_message(), Ok(2));
        assert_eq!(
            rx.try_recv_high_priority_message(),
            Err(RecvError::NoMessageAvailable)
        );
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Err(RecvError::NoMessageAvailable));
    }
}
