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

use std::net::SocketAddr;

use quickwit_proto::tonic;
use tokio::net::TcpListener;
use tonic::async_trait;

/// Resolve `SocketAddr` into `TcpListener` instances.
///
/// This trait can be used to inject existing [`TcpListener`] instances to the
/// Quickwit REST and gRPC servers when running them in tests.
#[async_trait]
pub trait TcpListenerResolver: Clone + Send + 'static {
    async fn resolve(&self, addr: SocketAddr) -> anyhow::Result<TcpListener>;
}

#[derive(Clone)]
pub struct DefaultTcpListenerResolver;

#[async_trait]
impl TcpListenerResolver for DefaultTcpListenerResolver {
    async fn resolve(&self, addr: SocketAddr) -> anyhow::Result<TcpListener> {
        TcpListener::bind(addr)
            .await
            .map_err(|err| anyhow::anyhow!(err))
    }
}

#[cfg(any(test, feature = "testsuite"))]
pub mod for_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use anyhow::Context;
    use tokio::sync::Mutex;

    use super::*;

    #[derive(Clone, Default)]
    pub struct TestTcpListenerResolver {
        listeners: Arc<Mutex<HashMap<SocketAddr, TcpListener>>>,
    }

    #[async_trait]
    impl TcpListenerResolver for TestTcpListenerResolver {
        async fn resolve(&self, addr: SocketAddr) -> anyhow::Result<TcpListener> {
            self.listeners
                .lock()
                .await
                .remove(&addr)
                .context(format!("No listener found for address {addr}"))
        }
    }

    impl TestTcpListenerResolver {
        pub async fn add_listener(&self, listener: TcpListener) {
            self.listeners
                .lock()
                .await
                .insert(listener.local_addr().unwrap(), listener);
        }
    }
}
