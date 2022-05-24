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

use crate::ActorExitStatus;

pub enum JoinHandle {
    ThreadJoinHandle(oneshot::Receiver<ActorExitStatus>),
    TokioTaskJoinHandle(tokio::task::JoinHandle<ActorExitStatus>),
}

impl JoinHandle {
    pub fn create_for_thread() -> (JoinHandle, oneshot::Sender<ActorExitStatus>) {
        let (tx, rx) = oneshot::channel();
        let join_handle = JoinHandle::ThreadJoinHandle(rx);
        (join_handle, tx)
    }

    pub fn create_for_task(join_handle: tokio::task::JoinHandle<ActorExitStatus>) -> JoinHandle {
        JoinHandle::TokioTaskJoinHandle(join_handle)
    }

    pub async fn join(self) -> ActorExitStatus {
        match self {
            Self::ThreadJoinHandle(rx) => rx.await.unwrap_or(ActorExitStatus::Panicked),
            Self::TokioTaskJoinHandle(join_handle) => match join_handle.await {
                Ok(exit_status) => exit_status,
                Err(join_err) => {
                    if join_err.is_panic() {
                        ActorExitStatus::Panicked
                    } else {
                        ActorExitStatus::Killed
                    }
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JoinHandle;
    use crate::ActorExitStatus;

    #[tokio::test]
    async fn test_join_handle_ok() {
        let (join_handle, tx) = JoinHandle::create_for_thread();
        tx.send(ActorExitStatus::Success).unwrap();
        assert!(matches!(join_handle.join().await, ActorExitStatus::Success));
    }

    #[tokio::test]
    async fn test_join_handle_panic() {
        let (join_handle, tx) = JoinHandle::create_for_thread();
        std::mem::drop(tx);
        assert!(matches!(
            join_handle.join().await,
            ActorExitStatus::Panicked
        ));
    }
}
