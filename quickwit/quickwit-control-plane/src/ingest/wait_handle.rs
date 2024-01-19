// Copyright (C) 2024 Quickwit, Inc.
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

pub struct WaitHandle {
    rx: oneshot::Receiver<()>,
}

impl WaitHandle {
    pub fn new() -> (WaitDropGuard, WaitHandle) {
        let (tx, rx) = oneshot::channel();
        let wait_drop_guard = WaitDropGuard(tx);
        let wait_handle = WaitHandle { rx };
        (wait_drop_guard, wait_handle)
    }

    pub async fn wait(self) {
        let _ = self.rx.await;
    }
}

pub struct WaitDropGuard(oneshot::Sender<()>);

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot::error::TryRecvError;
    #[tokio::test]
    async fn test_wait_handle_simple() {
        let (wait_drop_handle, mut wait_handle) = super::WaitHandle::new();
        assert!(matches!(
            wait_handle.rx.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));
        drop(wait_drop_handle);
        wait_handle.wait().await;
    }
}
