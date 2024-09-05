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
                .context(format!("No listener found for address {}", addr))
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
