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

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use crate::connection::ConnStream;
use crate::endpoint::Endpoint;

/// Default per-host idle connection cap.
pub const DEFAULT_MAX_IDLE_PER_HOST: usize = 32;
/// Default idle timeout: an idle connection older than this is dropped on the
/// next acquire/release rather than reused.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

/// An idle connection and the instant it entered the pool.
struct IdleConn {
    conn: ConnStream,
    idle_since: Instant,
}

struct PoolInner {
    idle: Mutex<HashMap<Endpoint, VecDeque<IdleConn>>>,
    max_idle_per_host: usize,
    idle_timeout: Duration,
}

/// A connection pool keyed by [`Endpoint`].
#[derive(Clone)]
pub struct ConnectionPool {
    inner: Arc<PoolInner>,
}

impl ConnectionPool {
    /// Creates a pool with the given per-host idle cap and idle timeout.
    ///
    /// `max_idle_per_host = 0` disables pooling entirely: [`Self::release`]
    /// drops every connection and [`Self::acquire`] always returns `None`.
    pub fn new(max_idle_per_host: usize, idle_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                idle: Mutex::new(HashMap::new()),
                max_idle_per_host,
                idle_timeout,
            }),
        }
    }

    /// Creates a pool with [`DEFAULT_MAX_IDLE_PER_HOST`] and
    /// [`DEFAULT_IDLE_TIMEOUT`].
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_IDLE_PER_HOST, DEFAULT_IDLE_TIMEOUT)
    }

    /// Takes an idle connection for `endpoint` out of the pool, or returns
    /// `None` when none is available.
    pub fn acquire(&self, endpoint: &Endpoint) -> Option<ConnStream> {
        let mut idle = self.inner.idle.lock().unwrap();
        let queue = idle.get_mut(endpoint)?;
        purge_expired(queue, self.inner.idle_timeout);
        queue.pop_front().map(|entry| entry.conn)
    }

    /// Returns a connection to the pool for later reuse.
    pub fn release(&self, endpoint: &Endpoint, conn: ConnStream) {
        if self.inner.max_idle_per_host == 0 {
            return;
        }
        let mut idle = self.inner.idle.lock().unwrap();
        let queue = idle.entry(endpoint.clone()).or_default();
        purge_expired(queue, self.inner.idle_timeout);
        if queue.len() >= self.inner.max_idle_per_host {
            return;
        }
        queue.push_back(IdleConn {
            conn,
            idle_since: Instant::now(),
        });
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Drops entries from the front of `queue` whose idle age has reached
/// `idle_timeout`.
fn purge_expired(queue: &mut VecDeque<IdleConn>, idle_timeout: Duration) {
    let now = Instant::now();
    while let Some(front) = queue.front() {
        if now.duration_since(front.idle_since) >= idle_timeout {
            queue.pop_front();
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;

    async fn holding_server_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            // Hold every accepted connection so the client side stays open.
            let mut held: Vec<_> = Vec::new();
            while let Ok((conn, _)) = listener.accept().await {
                held.push(conn);
            }
        });
        port
    }

    fn endpoint(port: u16) -> Endpoint {
        Endpoint {
            tls: false,
            host: "127.0.0.1".to_string(),
            port,
        }
    }

    async fn make_conn(port: u16) -> ConnStream {
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let _ = tcp.set_nodelay(true);
        ConnStream::Plain(tcp)
    }

    // Local ephemeral port of a connection, used as a stable identity to
    // check that acquire returns the same connection that was released.
    fn local_port(conn: &ConnStream) -> u16 {
        match conn {
            ConnStream::Plain(tcp) => tcp.local_addr().unwrap().port(),
            ConnStream::Tls(tls) => tls.get_ref().0.local_addr().unwrap().port(),
        }
    }

    #[tokio::test]
    async fn acquire_returns_none_when_empty() {
        let pool = ConnectionPool::with_defaults();
        let port = holding_server_port().await;
        let ep = endpoint(port);
        assert!(pool.acquire(&ep).is_none());
    }

    #[tokio::test]
    async fn release_then_acquire_reuses_same_connection() {
        let pool = ConnectionPool::with_defaults();
        let port = holding_server_port().await;
        let ep = endpoint(port);
        let conn = make_conn(port).await;
        let expected_port = local_port(&conn);
        pool.release(&ep, conn);
        let reused = pool.acquire(&ep).expect("pooled connection");
        assert_eq!(local_port(&reused), expected_port);
        // The pool is drained after the acquire.
        assert!(pool.acquire(&ep).is_none());
    }

    #[tokio::test]
    async fn one_connection_serves_sequential_acquires() {
        // The point of the pool: a single connection, repeatedly released and
        // reacquired, serves a sequence of requests without opening new ones.
        let pool = ConnectionPool::with_defaults();
        let port = holding_server_port().await;
        let ep = endpoint(port);
        let mut conn = make_conn(port).await;
        let identity = local_port(&conn);

        for _ in 0..3 {
            pool.release(&ep, conn);
            conn = pool.acquire(&ep).expect("reused the pooled connection");
            assert_eq!(local_port(&conn), identity, "reused the same connection");
        }
        // After the loop the pool holds one connection again.
        assert!(pool.acquire(&ep).is_none(), "acquire drained the pool");
    }

    #[tokio::test(start_paused = true)]
    async fn idle_connection_expires_after_idle_timeout() {
        let idle_timeout = Duration::from_secs(10);
        let pool = ConnectionPool::new(8, idle_timeout);
        let port = holding_server_port().await;
        let ep = endpoint(port);
        let conn = make_conn(port).await;
        pool.release(&ep, conn);
        // Still fresh immediately.
        assert!(pool.acquire(&ep).is_some(), "should reuse before timeout");
        // Put it back and advance past the idle timeout.
        let conn = make_conn(port).await;
        pool.release(&ep, conn);
        tokio::time::sleep(idle_timeout + Duration::from_secs(1)).await;
        assert!(pool.acquire(&ep).is_none(), "should have expired");
    }

    #[tokio::test(start_paused = true)]
    async fn idle_cap_drops_excess_connections() {
        let pool = ConnectionPool::new(2, Duration::from_secs(90));
        let port = holding_server_port().await;
        let ep = endpoint(port);

        let c1 = make_conn(port).await;
        let c2 = make_conn(port).await;
        let c3 = make_conn(port).await;
        let p1 = local_port(&c1);
        let p2 = local_port(&c2);

        pool.release(&ep, c1);
        pool.release(&ep, c2);
        // Queue is at the cap (2); the third release will be dropped.
        pool.release(&ep, c3);

        let got1 = pool.acquire(&ep).expect("first pooled");
        let got2 = pool.acquire(&ep).expect("second pooled");
        assert!(pool.acquire(&ep).is_none(), "third was dropped");
        assert_eq!(local_port(&got1), p1);
        assert_eq!(local_port(&got2), p2);
    }

    #[tokio::test]
    async fn max_idle_zero_disables_pooling() {
        let pool = ConnectionPool::new(0, Duration::from_secs(90));
        let port = holding_server_port().await;
        let ep = endpoint(port);
        let conn = make_conn(port).await;
        pool.release(&ep, conn);
        assert!(pool.acquire(&ep).is_none());
    }
}
