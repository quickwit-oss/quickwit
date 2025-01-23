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

pub struct WaitDropGuard(#[allow(dead_code)] oneshot::Sender<()>);

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
