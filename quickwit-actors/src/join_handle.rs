// Copyright (C) 2021 Quickwit, Inc.
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

use tokio::sync::oneshot;

pub struct JoinHandle {
    inner: Inner,
}

impl JoinHandle {
    pub fn create_for_thread() -> (JoinHandle, oneshot::Sender<JoinOutcome>) {
        let (tx, rx) = oneshot::channel();
        let join_handle = JoinHandle {
            inner: Inner::ThreadJoinHandle(rx),
        };
        (join_handle, tx)
    }

    pub fn create_for_task(join_handle: tokio::task::JoinHandle<()>) -> JoinHandle {
        JoinHandle {
            inner: Inner::TokioTaskJoinHandle(join_handle),
        }
    }

    pub async fn join(&mut self) -> JoinOutcome {
        let inner = std::mem::replace(&mut self.inner, Inner::Done(JoinOutcome::Ok));
        let outcome = match inner {
            Inner::ThreadJoinHandle(rx) => rx.await.unwrap_or(JoinOutcome::Panic),
            Inner::TokioTaskJoinHandle(join_handle) => match join_handle.await {
                Ok(()) => JoinOutcome::Ok,
                Err(join_err) => {
                    if join_err.is_panic() {
                        JoinOutcome::Panic
                    } else {
                        JoinOutcome::Error
                    }
                }
            },
            Inner::Done(outcome) => outcome,
        };
        self.inner = Inner::Done(outcome);
        outcome
    }
}

enum Inner {
    ThreadJoinHandle(oneshot::Receiver<JoinOutcome>),
    TokioTaskJoinHandle(tokio::task::JoinHandle<()>),
    Done(JoinOutcome),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JoinOutcome {
    Ok,
    Error,
    Panic,
}

#[cfg(test)]
mod tests {
    use super::JoinHandle;
    use crate::join_handle::JoinOutcome;

    #[tokio::test]
    async fn test_join_handle_ok() {
        let (mut join_handle, tx) = JoinHandle::create_for_thread();
        tx.send(JoinOutcome::Ok).unwrap();
        assert_eq!(join_handle.join().await, JoinOutcome::Ok);
        assert_eq!(join_handle.join().await, JoinOutcome::Ok);
    }

    #[tokio::test]
    async fn test_join_handle_err() {
        let (mut join_handle, tx) = JoinHandle::create_for_thread();
        tx.send(JoinOutcome::Error).unwrap();
        assert_eq!(join_handle.join().await, JoinOutcome::Error);
    }

    #[tokio::test]
    async fn test_join_handle_panic() {
        let (mut join_handle, tx) = JoinHandle::create_for_thread();
        std::mem::drop(tx);
        assert_eq!(join_handle.join().await, JoinOutcome::Panic);
    }
}
