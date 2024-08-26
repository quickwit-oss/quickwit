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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// A [`TcpListener`] constructor that can re-use existing listeners in tests.
///
/// When building a local test cluster we want to reserve all the ports of all
/// the nodes before starting the first one.
#[derive(Clone)]
pub enum TcpListenerResolver {
    Default,
    #[cfg(any(test, feature = "testsuite"))]
    Test(Arc<Mutex<HashMap<SocketAddr, TcpListener>>>),
}

impl Default for TcpListenerResolver {
    fn default() -> Self {
        Self::Default
    }
}

#[cfg(any(test, feature = "testsuite"))]
impl TcpListenerResolver {
    pub fn for_test() -> Self {
        Self::Test(Arc::new(Mutex::new(HashMap::new())))
    }
}

impl TcpListenerResolver {
    pub async fn resolve(&self, addr: SocketAddr) -> anyhow::Result<TcpListener> {
        match self {
            TcpListenerResolver::Default => TcpListener::bind(addr).await.map_err(|e| e.into()),
            #[cfg(any(test, feature = "testsuite"))]
            TcpListenerResolver::Test(listeners) => listeners
                .lock()
                .await
                .remove(&addr)
                .context(format!("No listener found for address {}", addr)),
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub async fn add_listener(&self, listener: TcpListener) {
        match self {
            TcpListenerResolver::Default => {
                panic!("Cannot add listener in default mode.");
            }
            TcpListenerResolver::Test(listeners) => listeners
                .lock()
                .await
                .insert(listener.local_addr().unwrap(), listener),
        };
    }
}
