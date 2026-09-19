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
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::connection::ConnStream;
use crate::endpoint::Endpoint;
use crate::error::HttpError;

/// Probes a connection with a single non-blocking poll_read.
///
/// We don't await, Pending means the connection looks healty,
/// anything else means it's not:
/// - Ready(Err): there's an error
/// - Ready(Ok(0)): end of stream (the connection is half closed)
/// - Ready(Ok(n)): protocol desync
///
/// This costs a non-blocking syscall
fn probe_healthy(conn: &mut ConnStream) -> bool {
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut buf = [0u8; 1];
    let mut read_buf = ReadBuf::new(&mut buf);
    match Pin::new(conn).poll_read(&mut context, &mut read_buf) {
        Poll::Pending => true,
        Poll::Ready(Ok(())) => false,
        Poll::Ready(Err(_)) => false,
    }
}

/// Default per-host idle connection cap.
pub const DEFAULT_MAX_IDLE_PER_HOST: usize = 32;
/// Default idle timeout: an idle connection older than this is dropped on the
/// next acquire/release (and by the background reaper) rather than reused.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

/// An idle connection and the instant it entered the pool.
struct IdleConn {
    conn: ConnStream,
    idle_since: Instant,
}

/// Per-host pool state
#[derive(Default)]
struct HostState {
    /// Idle connections, ordered oldest (front) to newest (back).
    idle: VecDeque<IdleConn>,
    /// Checkouts waiting for a connection to be returned.
    waiters: VecDeque<oneshot::Sender<ConnStream>>,
}

impl HostState {
    fn purge_expired(&mut self, idle_timeout: Duration) {
        let now = Instant::now();
        if let Some(newest) = self.idle.back()
            && now.duration_since(newest.idle_since) >= idle_timeout
        {
            self.idle.clear();
        } else {
            while let Some(oldest) = self.idle.front() {
                if now.duration_since(oldest.idle_since) >= idle_timeout {
                    self.idle.pop_front();
                } else {
                    break;
                }
            }
        }
        while let Some(front) = self.waiters.front() {
            if front.is_closed() {
                self.waiters.pop_front();
            } else {
                break;
            }
        }
    }
}

/// A per-host entry, shared via `Arc` so the top-level map only needs a brief
/// lookup lock; the deque work happens under this per-host lock.
struct HostEntry {
    state: Mutex<HostState>,
}

struct PoolInner {
    hosts: Mutex<HashMap<Endpoint, Arc<HostEntry>>>,
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
    /// drops every connection and [`Self::acquire`] always connects fresh.
    ///
    /// Must be called within a Tokio runtime: this spawns a background reaper
    /// task that periodically evicts expired idle connections. The task holds
    /// a [`Weak`] handle to this pool and exits when the pool is dropped.
    /// Lazy eviction on [`Self::acquire`] and [`Self::release`] keeps the
    /// pool correct even if the reaper never runs.
    pub fn new(max_idle_per_host: usize, idle_timeout: Duration) -> Self {
        Self::new_with_reaper_handle(max_idle_per_host, idle_timeout).0
    }

    /// Like [`Self::new`], but also returns the background reaper task's
    /// [`JoinHandle`].
    pub fn new_with_reaper_handle(
        max_idle_per_host: usize,
        idle_timeout: Duration,
    ) -> (Self, JoinHandle<()>) {
        let inner = Arc::new(PoolInner {
            hosts: Mutex::new(HashMap::new()),
            max_idle_per_host,
            idle_timeout,
        });
        let reaper = spawn_reaper(Arc::downgrade(&inner), idle_timeout);
        (Self { inner }, reaper)
    }

    /// Creates a pool with [`DEFAULT_MAX_IDLE_PER_HOST`] and
    /// [`DEFAULT_IDLE_TIMEOUT`].
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_IDLE_PER_HOST, DEFAULT_IDLE_TIMEOUT)
    }

    fn entry(&self, endpoint: &Endpoint) -> Arc<HostEntry> {
        let mut hosts = self.inner.hosts.lock().unwrap();
        hosts
            .entry(endpoint.clone())
            .or_insert_with(|| {
                Arc::new(HostEntry {
                    state: Mutex::new(HostState::default()),
                })
            })
            .clone()
    }

    /// Takes an idle connection for `endpoint` out of the pool (MRU: the
    /// most recently returned one first), or races a fresh `connect` against a
    /// connection being returned by a concurrent [`Self::release`].
    ///
    /// Returns the connection along with `was_reused`: `true` when it came
    /// from the pool (either an idle entry or a waiter hand-off). Reused connections
    /// might have died without it being noticed yet, one failing early should
    /// cause a retry rather than a query failure.
    pub async fn acquire<F>(
        &self,
        endpoint: &Endpoint,
        connect: F,
    ) -> Result<(ConnStream, bool), HttpError>
    where
        F: Future<Output = Result<ConnStream, HttpError>> + Send,
    {
        let entry = self.entry(endpoint);
        let mut rx = {
            let mut state = entry.state.lock().unwrap();
            while let Some(mut conn) = state.idle.pop_back().map(|entry| entry.conn) {
                if probe_healthy(&mut conn) {
                    return Ok((conn, true));
                } else {
                    drop(conn);
                }
            }
            let (tx, rx) = oneshot::channel();
            state.waiters.push_back(tx);
            rx
        };
        let mut connect = Box::pin(connect);
        tokio::select! {
            conn = &mut rx => match conn {
                Ok(conn) => Ok((conn, true)),
                // The sender was dropped without sending. This shouldn't happen.
                // Fall back to connecting.
                Err(_) => {
                    let conn = connect.as_mut().await?;
                    Ok((conn, false))
                }
            },
            conn = connect.as_mut() => {
                // leave our oneshot sender alone, next call to release() that tries to send it a
                // connection will clean it up
                Ok((conn?, false))
            }
        }
    }

    /// Returns a connection to the pool for later reuse, handing it to the
    /// oldest live waiter first if one is waiting.
    pub fn release(&self, endpoint: &Endpoint, conn: ConnStream) {
        if self.inner.max_idle_per_host == 0 {
            return;
        }
        let entry = self.entry(endpoint);
        let mut state = entry.state.lock().unwrap();
        // Hand the connection to the oldest live waiter, or park the connection.
        let mut conn = conn;
        while let Some(sender) = state.waiters.pop_front() {
            match sender.send(conn) {
                Ok(()) => return,
                Err(returned) => conn = returned,
            }
        }
        state.idle.push_back(IdleConn {
            conn,
            idle_since: Instant::now(),
        });
        if state.idle.len() > self.inner.max_idle_per_host {
            state.idle.pop_front();
        }
    }

    #[cfg(test)]
    pub(crate) fn idle_count(&self, endpoint: &Endpoint) -> usize {
        let entry = self.entry(endpoint);
        entry.state.lock().unwrap().idle.len()
    }

    #[cfg(test)]
    pub(crate) fn waiter_count(&self, endpoint: &Endpoint) -> usize {
        let entry = self.entry(endpoint);
        entry.state.lock().unwrap().waiters.len()
    }

    #[cfg(test)]
    pub(crate) fn host_count(&self) -> usize {
        self.inner.hosts.lock().unwrap().len()
    }
}

/// Background task that periodically evicts expired idle connections and
/// reclaims stale waiters across all hosts, keeping the idle set honest
/// without an acquire happening. Holds a [`Weak`] handle so it exits as soon
/// as the pool is dropped.
fn spawn_reaper(weak: Weak<PoolInner>, idle_timeout: Duration) -> tokio::task::JoinHandle<()> {
    let tick = (idle_timeout / 2).max(Duration::from_secs(2));
    // enforce this is run only from a runtime so we can actually spawn the task
    let handle = tokio::runtime::Handle::try_current()
        .expect("ConnectionPool::new must be called within a Tokio runtime");
    handle.spawn(async move {
        loop {
            tokio::time::sleep(tick).await;
            let Some(inner) = weak.upgrade() else {
                return;
            };
            // Collect handles under the top-level lock, then purge each host
            // under its own lock so the map lock is held as little as possible.
            let entries: Vec<(Endpoint, Arc<HostEntry>)> = inner
                .hosts
                .lock()
                .unwrap()
                .iter()
                .map(|(ep, entry)| (ep.clone(), entry.clone()))
                .collect();
            for (endpoint, entry) in entries {
                let mut state = entry.state.lock().unwrap();
                state.purge_expired(inner.idle_timeout);
                // remove empty entries so they don't accumulate
                // there are races where we end up dropping a state that's being interacted with,
                // so that someone adds an idle conn or a waiter, just after we removed it from
                // the map. This only means we sometime don't reuse a connection when we could,
                // not ideal, not horrible
                if state.idle.is_empty() && state.waiters.is_empty() {
                    drop(state);
                    let mut hosts = inner.hosts.lock().unwrap();
                    if let Some(stale) = hosts.get(&endpoint) {
                        let stale_state = stale.state.lock().unwrap();
                        if stale_state.idle.is_empty() && stale_state.waiters.is_empty() {
                            drop(stale_state);
                            hosts.remove(&endpoint);
                        }
                    }
                }
            }
        }
    })
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

    fn pending_connect() -> std::future::Pending<Result<ConnStream, HttpError>> {
        std::future::pending()
    }

    async fn fresh_connect(port: u16) -> Result<ConnStream, HttpError> {
        Ok(make_conn(port).await)
    }

    #[tokio::test]
    async fn acquire_on_empty_pool_connects_fresh() {
        let pool = ConnectionPool::with_defaults();
        let port = holding_server_port().await;
        let ep = endpoint(port);
        let (_conn, was_reused) = pool.acquire(&ep, fresh_connect(port)).await.unwrap();
        assert!(!was_reused, "empty pool must connect fresh");
    }

    #[tokio::test]
    async fn release_then_acquire_reuses_same_connection() {
        let pool = ConnectionPool::with_defaults();
        let port = holding_server_port().await;
        let ep = endpoint(port);
        let conn = make_conn(port).await;
        let expected_port = local_port(&conn);
        pool.release(&ep, conn);
        let (reused, was_reused) = pool.acquire(&ep, pending_connect()).await.unwrap();
        assert!(was_reused, "should reuse the pooled connection");
        assert_eq!(local_port(&reused), expected_port);
        assert_eq!(pool.idle_count(&ep), 0, "acquire drained the pool");
    }

    #[tokio::test]
    async fn one_connection_serves_sequential_acquires() {
        let pool = ConnectionPool::with_defaults();
        let port = holding_server_port().await;
        let ep = endpoint(port);
        let mut conn = make_conn(port).await;
        let identity = local_port(&conn);

        for _ in 0..3 {
            pool.release(&ep, conn);
            let (c, was_reused) = pool.acquire(&ep, pending_connect()).await.unwrap();
            assert!(was_reused, "should reuse the pooled connection");
            conn = c;
            assert_eq!(local_port(&conn), identity, "reused the same connection");
        }
        // After the loop the pool holds no connection (the last acquire took it).
        assert_eq!(pool.idle_count(&ep), 0, "acquire drained the pool");
    }

    #[tokio::test]
    async fn acquire_is_mru_most_recent_first() {
        let pool = ConnectionPool::with_defaults();
        let port = holding_server_port().await;
        let ep = endpoint(port);

        let c1 = make_conn(port).await;
        let c2 = make_conn(port).await;
        let p1 = local_port(&c1);
        let p2 = local_port(&c2);

        pool.release(&ep, c1);
        pool.release(&ep, c2);

        let (first, _) = pool
            .acquire(&ep, pending_connect())
            .await
            .expect("first pooled");
        let (second, _) = pool
            .acquire(&ep, pending_connect())
            .await
            .expect("second pooled");
        assert_eq!(local_port(&first), p2, "most-recently-returned first");
        assert_eq!(local_port(&second), p1, "then the older one");
        assert_eq!(pool.idle_count(&ep), 0, "pool drained");
    }

    #[tokio::test(start_paused = true)]
    async fn idle_cap_evicts_oldest_connection() {
        let pool = ConnectionPool::new(2, Duration::from_secs(90));
        let port = holding_server_port().await;
        let ep = endpoint(port);

        let c1 = make_conn(port).await;
        let c2 = make_conn(port).await;
        let c3 = make_conn(port).await;
        let p2 = local_port(&c2);
        let p3 = local_port(&c3);

        pool.release(&ep, c1);
        pool.release(&ep, c2);
        // Queue is at the cap (2); the third release evicts c1 (oldest).
        pool.release(&ep, c3);

        let (got1, _) = pool
            .acquire(&ep, pending_connect())
            .await
            .expect("first pooled");
        let (got2, _) = pool
            .acquire(&ep, pending_connect())
            .await
            .expect("second pooled");
        // c1 (the oldest) was evicted when c3 was released.
        assert_eq!(pool.idle_count(&ep), 0, "pool drained");
        assert_eq!(local_port(&got1), p3, "MRU: c3 (newest) first");
        assert_eq!(local_port(&got2), p2, "then c2");
    }

    #[tokio::test]
    async fn max_idle_zero_disables_pooling() {
        let pool = ConnectionPool::new(0, Duration::from_secs(90));
        let port = holding_server_port().await;
        let ep = endpoint(port);
        let conn = make_conn(port).await;
        pool.release(&ep, conn);
        let (_, was_reused) = pool.acquire(&ep, fresh_connect(port)).await.unwrap();
        assert!(!was_reused, "pooling disabled: must connect fresh");
        assert_eq!(pool.idle_count(&ep), 0, "nothing parked");
    }

    #[tokio::test]
    async fn distinct_endpoints_do_not_share_connections() {
        let pool = ConnectionPool::with_defaults();
        let port_a = holding_server_port().await;
        let port_b = holding_server_port().await;
        let ep_a = endpoint(port_a);
        let ep_b = endpoint(port_b);

        let conn_a = make_conn(port_a).await;
        let p_a = local_port(&conn_a);
        pool.release(&ep_a, conn_a);
        let (_, was_reused_b) = pool.acquire(&ep_b, fresh_connect(port_b)).await.unwrap();
        assert!(!was_reused_b, "ep_b must connect fresh");
        let (got_a, was_reused_a) = pool.acquire(&ep_a, pending_connect()).await.unwrap();
        assert!(was_reused_a, "ep_a still has its connection");
        assert_eq!(local_port(&got_a), p_a);
    }

    #[tokio::test]
    async fn acquire_probes_and_drops_fully_closed_connection() {
        let pool = ConnectionPool::with_defaults();
        let port = holding_server_port().await;
        let ep = endpoint(port);

        let closing_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closing_port = closing_listener.local_addr().unwrap().port();
        // The server accepts, and drops it immediately.
        let server = tokio::spawn(async move {
            let (sock, _) = closing_listener.accept().await.unwrap();
            drop(sock);
        });
        let dead_conn = make_conn(closing_port).await;
        // Wait for the server to accept and close its end.
        server.await.unwrap();

        pool.release(&ep, dead_conn);
        let (_got, was_reused) = pool.acquire(&ep, fresh_connect(port)).await.unwrap();
        assert!(
            !was_reused,
            "fully-closed conn should be dropped, not reused"
        );
    }

    #[tokio::test]
    async fn acquire_probes_and_drops_half_closed_connection() {
        let pool = ConnectionPool::with_defaults();
        let port = holding_server_port().await;
        let ep = endpoint(port);

        let half_close_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let half_close_port = half_close_listener.local_addr().unwrap().port();
        // The server accepts, shuts down its write side, and keeps the
        // socket alive to keep the connection half-open.
        let server = tokio::spawn(async move {
            let (mut sock, _) = half_close_listener.accept().await.unwrap();
            use tokio::io::AsyncWriteExt;
            sock.shutdown().await.unwrap();
            std::future::pending::<()>().await;
        });
        let half_closed_conn = make_conn(half_close_port).await;
        // Give the server time to accept and shut down its write side.
        tokio::time::sleep(Duration::from_millis(50)).await;

        pool.release(&ep, half_closed_conn);
        let (_got, was_reused) = pool.acquire(&ep, fresh_connect(port)).await.unwrap();
        assert!(
            !was_reused,
            "half-closed conn should be dropped, not reused"
        );
        server.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_races_connect_and_hands_off_to_waiter() {
        let pool = ConnectionPool::with_defaults();
        let port = holding_server_port().await;
        let ep = endpoint(port);

        let mut acquire_fut = std::pin::pin!(pool.acquire(&ep, pending_connect()));

        // Poll the acquire once: it must register its waiter and then park
        // on the race (Pending), since neither the pending connect nor a
        // hand-off has resolved.
        let waker = futures::task::noop_waker();
        let mut context = std::task::Context::from_waker(&waker);
        assert!(
            matches!(
                acquire_fut.as_mut().poll(&mut context),
                std::task::Poll::Pending
            ),
            "acquire should park waiting for a connection"
        );
        assert_eq!(pool.waiter_count(&ep), 1, "acquire should be waiting");

        // A separate connection is returned; release must hand it to the
        // waiter rather than parking it idle.
        let donated = make_conn(port).await;
        let donated_port = local_port(&donated);
        pool.release(&ep, donated);
        assert_eq!(pool.idle_count(&ep), 0, "released conn went to the waiter");

        let (got, was_reused) = acquire_fut.await.expect("acquire ok");
        assert!(was_reused, "should reuse the donated connection");
        assert_eq!(local_port(&got), donated_port);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_waiter_is_cleaned_up_by_release() {
        let pool = ConnectionPool::with_defaults();
        let port = holding_server_port().await;
        let ep = endpoint(port);

        let (_fresh, was_reused) = pool
            .acquire(&ep, fresh_connect(port))
            .await
            .expect("acquire ok");
        assert!(!was_reused);
        // The stale sender is still in the waiters queue until release touches it.
        assert_eq!(pool.waiter_count(&ep), 1, "stale sender not cleaned yet");

        let conn = make_conn(port).await;
        let identity = local_port(&conn);
        pool.release(&ep, conn);
        // The stale sender was popped (send failed), and the connection was
        // parked idle rather than lost.
        assert_eq!(pool.waiter_count(&ep), 0, "stale sender cleaned up");
        assert_eq!(pool.idle_count(&ep), 1, "connection parked idle");
        let (reused, _) = pool
            .acquire(&ep, pending_connect())
            .await
            .expect("parked connection");
        assert_eq!(local_port(&reused), identity);
    }

    #[tokio::test(start_paused = true)]
    async fn reaper_evicts_expired_idle_connections_without_an_acquire() {
        let idle_timeout = Duration::from_secs(10);
        let pool = ConnectionPool::new(8, idle_timeout);
        let port = holding_server_port().await;
        let ep = endpoint(port);

        let conn = make_conn(port).await;
        pool.release(&ep, conn);
        assert_eq!(pool.idle_count(&ep), 1, "parked");

        // Advance past the idle timeout; the connection is now expired.
        tokio::time::sleep(idle_timeout + Duration::from_millis(50)).await;
        let mut evicted = false;
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if pool.idle_count(&ep) == 0 {
                evicted = true;
                break;
            }
            // Nudge time forward past the next tick if needed.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(evicted, "reaper should have evicted the expired connection");
    }

    #[tokio::test(start_paused = true)]
    async fn reaper_reclaims_stale_waiters_for_unpooled_host() {
        let idle_timeout = Duration::from_secs(10);
        let port = holding_server_port().await;
        let ep = endpoint(port);

        // Establish all connections before starting the reaper to make the test more
        // deterministic. Mixing real socket I/O with paused tokio leads to skipped time.
        let mut connections = Vec::new();
        for _ in 0..3 {
            connections.push(make_conn(port).await);
        }

        let pool = ConnectionPool::new(8, idle_timeout);
        for conn in connections {
            let connect = std::future::ready(Ok::<ConnStream, HttpError>(conn));
            let (_conn, was_reused) = pool.acquire(&ep, connect).await.unwrap();
            assert!(!was_reused);
        }
        assert_eq!(
            pool.waiter_count(&ep),
            3,
            "each connect-won acquire leaves a stale waiter"
        );

        tokio::time::sleep(idle_timeout + Duration::from_millis(50)).await;
        let mut reclaimed = false;
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if pool.waiter_count(&ep) == 0 {
                reclaimed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(reclaimed, "reaper should have reclaimed the stale waiters");
    }

    #[tokio::test(start_paused = true)]
    async fn reaper_evicts_empty_host_entries() {
        // A host whose connections all expire (or whose waiters all get
        // reclaimed) should have its `HostEntry` removed from the map, not
        // linger forever. Without this, the map accumulates one entry per
        // endpoint ever contacted.
        let idle_timeout = Duration::from_secs(10);
        let (pool, _reaper) = ConnectionPool::new_with_reaper_handle(8, idle_timeout);
        let port = holding_server_port().await;
        let ep = endpoint(port);

        let conn = make_conn(port).await;
        pool.release(&ep, conn);
        assert_eq!(pool.host_count(), 1, "host entry created on release");

        // Advance past the idle timeout; the reaper purges the expired
        // connection and then evicts the now-empty host entry.
        tokio::time::sleep(idle_timeout + Duration::from_millis(50)).await;
        let mut evicted = false;
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if pool.host_count() == 0 {
                evicted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(evicted, "reaper should have evicted the empty host entry");
    }

    #[tokio::test(start_paused = true)]
    async fn reaper_keeps_fresh_connections() {
        let idle_timeout = Duration::from_secs(10);
        let pool = ConnectionPool::new(8, idle_timeout);
        let port = holding_server_port().await;
        let ep = endpoint(port);

        let conn = make_conn(port).await;
        pool.release(&ep, conn);
        // Advance less than the idle timeout, then let the reaper tick: the
        // connection is still fresh and must be retained.
        tokio::time::sleep(idle_timeout / 2 + Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(pool.idle_count(&ep), 1, "fresh connection retained");
    }

    #[tokio::test(start_paused = true)]
    async fn reaper_exits_when_pool_is_dropped() {
        let idle_timeout = Duration::from_secs(10);
        let (pool, reaper) = ConnectionPool::new_with_reaper_handle(8, idle_timeout);
        let port = holding_server_port().await;
        let ep = endpoint(port);
        let conn = make_conn(port).await;
        pool.release(&ep, conn);
        assert!(!reaper.is_finished(), "reaper runs while the pool lives");

        drop(pool);
        // Advance past one reaper tick so the task wakes
        let tick = idle_timeout / 2;
        tokio::time::sleep(tick + Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        assert!(
            reaper.is_finished(),
            "reaper should have exited after the pool was dropped"
        );
    }
}
