// Copyright (C) 2023 Quickwit, Inc.
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

// This file contains code copied from the Resource trait
// in async-speed-limit from the TiKV project.
// https://github.com/tikv/async-speed-limit/blob/master/src/io.rs
//
// Copyright 2019 TiKV Project Authors. Licensed under MIT or Apache-2.0.

// We are simply porting the logic to tokio here and adding the functionality to
// plug some metrics.

use std::future::Future;
use std::io;
use std::io::IoSlice;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_speed_limit::clock::StandardClock;
use async_speed_limit::limiter::Consume;
use async_speed_limit::Limiter;
use once_cell::sync::Lazy;
use pin_project::pin_project;
use prometheus::IntCounter;
use tokio::io::AsyncWrite;

use crate::metrics::{new_counter_vec, IntCounterVec};
use crate::{KillSwitch, Progress, ProtectedZoneGuard};

// Max 1MB at a time.
const MAX_NUM_BYTES_WRITTEN_AT_ONCE: usize = 1 << 20;

fn truncate_bytes(bytes: &[u8]) -> &[u8] {
    let num_bytes = bytes.len().min(MAX_NUM_BYTES_WRITTEN_AT_ONCE);
    &bytes[..num_bytes]
}

struct IoMetrics {
    write_bytes: IntCounterVec<2>,
}

impl Default for IoMetrics {
    fn default() -> Self {
        let write_bytes = new_counter_vec(
            "write_bytes",
            "Number of bytes written by a given component in [indexer, merger, deleter, \
             split_downloader_{merge,delete}]",
            "quickwit",
            ["index", "component"],
        );
        Self { write_bytes }
    }
}

static IO_METRICS: Lazy<IoMetrics> = Lazy::new(IoMetrics::default);

/// Parameter used in `async_speed_limit`.
///
/// The default value is good and does not need to be tweaked.
/// We use a smaller value in unit test to get reasonably accurate throttling one very
/// short period of times.
///
/// For more details, please refer to `async_speed_limit` documentation.
const REFILL_DURATION: Duration = if cfg!(test) {
    Duration::from_millis(10)
} else {
    // Default value in async_speed_limit
    Duration::from_millis(100)
};

#[derive(Clone)]
pub struct IoControls {
    throughput_limiter: Limiter,
    bytes_counter: IntCounter,
    progress: Progress,
    kill_switch: KillSwitch,
}

impl Default for IoControls {
    fn default() -> Self {
        let default_bytes_counter =
            IntCounter::new("default_write_num_bytes", "Default write counter.").unwrap();
        IoControls {
            throughput_limiter: Limiter::new(f64::INFINITY),
            progress: Progress::default(),
            kill_switch: KillSwitch::default(),
            bytes_counter: default_bytes_counter,
        }
    }
}

impl IoControls {
    #[must_use]
    pub fn progress(&self) -> &Progress {
        &self.progress
    }

    pub fn kill(&self) {
        self.kill_switch.kill();
    }

    pub fn num_bytes(&self) -> u64 {
        self.bytes_counter.get()
    }

    pub fn check_if_alive(&self) -> io::Result<ProtectedZoneGuard> {
        if self.kill_switch.is_dead() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Directory kill switch was activated.",
            ));
        }
        let guard = self.progress.protect_zone();
        Ok(guard)
    }

    pub fn set_index_and_component(mut self, index: &str, component: &str) -> Self {
        self.bytes_counter = IO_METRICS.write_bytes.with_label_values([index, component]);
        self
    }

    pub fn set_throughput_limit(mut self, throughput: f64) -> Self {
        self.throughput_limiter = Limiter::builder(throughput).refill(REFILL_DURATION).build();
        self
    }

    pub fn set_bytes_counter(mut self, bytes_counter: IntCounter) -> Self {
        self.bytes_counter = bytes_counter;
        self
    }

    pub fn set_progress(mut self, progress: Progress) -> Self {
        self.progress = progress;
        self
    }

    pub fn set_kill_switch(mut self, kill_switch: KillSwitch) -> Self {
        self.kill_switch = kill_switch;
        self
    }
    fn consume_blocking(&self, num_bytes: usize) -> io::Result<()> {
        let _guard = self.check_if_alive()?;
        self.throughput_limiter.blocking_consume(num_bytes);
        self.bytes_counter.inc_by(num_bytes as u64);
        Ok(())
    }
}

#[pin_project]
pub struct ControlledWrite<A: IoControlsAccess, W> {
    #[pin]
    underlying_wrt: W,
    waiter: Option<Consume<StandardClock, ()>>,
    io_controls_access: A,
}

impl<A: IoControlsAccess, W: AsyncWrite> ControlledWrite<A, W> {
    // This function was copied from TiKV's `async-speed-limit`.
    // Copyright 2019 TiKV Project Authors. Licensed under MIT or Apache-2.0.
    /// Wraps a poll function with a delay after it.
    ///
    /// This method calls the given `poll` function until it is fulfilled. After
    /// that, the result is saved into this `Resource` instance (therefore
    /// different `poll_***` calls should not be interleaving), while returning
    /// `Pending` until the limiter has completely consumed the result.
    #[allow(dead_code)]
    pub(crate) fn poll_limited(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        poll: impl FnOnce(Pin<&mut W>, &mut Context<'_>) -> Poll<io::Result<usize>>,
    ) -> Poll<io::Result<usize>> {
        let this = self.project();

        let _protect_guard = match this
            .io_controls_access
            .apply(|io_controls| io_controls.check_if_alive())
        {
            Ok(protect_guard) => protect_guard,
            Err(io_err) => {
                return Poll::Ready(Err(io_err));
            }
        };

        if let Some(waiter) = this.waiter {
            let res = Pin::new(waiter).poll(cx);
            if res.is_pending() {
                return Poll::Pending;
            }
            *this.waiter = None;
        }

        let res: Poll<io::Result<usize>> = poll(this.underlying_wrt, cx);
        if let Poll::Ready(obj) = &res {
            let len = *obj.as_ref().unwrap_or(&0);
            if len > 0 {
                let waiter = this.io_controls_access.apply(|io_controls| {
                    io_controls.bytes_counter.inc_by(len as u64);
                    io_controls.throughput_limiter.consume(len)
                });
                *this.waiter = Some(waiter)
            }
        }
        res
    }
}

/// Quirky spec: truncates the list of bufs, and keep as many leftmost elements
/// as possible, within the constraint of not exceeding `max_len` bytes.
///
/// Please keep this function private
fn quirky_truncate_slices<'a, 'b>(bufs: &'b [IoSlice<'a>], max_len: usize) -> &'b [IoSlice<'a>] {
    if bufs.is_empty() {
        return bufs;
    }
    let mut cumulated_len = bufs[0].len();
    for (i, buf) in bufs.iter().enumerate().skip(1) {
        cumulated_len += buf.len();
        if cumulated_len > max_len {
            return &bufs[..i];
        }
    }
    bufs
}

impl<A: IoControlsAccess, W: AsyncWrite> AsyncWrite for ControlledWrite<A, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let buf = truncate_bytes(buf);
        // The shadowing is on purpose.
        self.poll_limited(cx, |r, cx| r.poll_write(cx, buf))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() {
            return Poll::Ready(Ok(0));
        }
        // The shadowing is on purpose.
        let bufs = quirky_truncate_slices(bufs, MAX_NUM_BYTES_WRITTEN_AT_ONCE);
        self.poll_limited(cx, |r, cx| r.poll_write_vectored(cx, bufs))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().underlying_wrt.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().underlying_wrt.poll_shutdown(cx)
    }
}

pub trait IoControlsAccess: Sized {
    fn wrap_write<W>(self, wrt: W) -> ControlledWrite<Self, W> {
        ControlledWrite {
            underlying_wrt: wrt,
            waiter: None,
            io_controls_access: self,
        }
    }

    fn apply<F, R>(&self, f: F) -> R
    where F: Fn(&IoControls) -> R;
}

impl IoControlsAccess for IoControls {
    fn apply<F, R>(&self, f: F) -> R
    where F: Fn(&IoControls) -> R {
        f(self)
    }
}

impl<A, W> ControlledWrite<A, W>
where A: IoControlsAccess
{
    pub fn underlying_wrt(&mut self) -> &mut W {
        &mut self.underlying_wrt
    }

    fn check_if_alive(&self) -> io::Result<ProtectedZoneGuard> {
        self.io_controls_access
            .apply(|io_controls| io_controls.check_if_alive())
    }
}

impl<A, W: io::Write> io::Write for ControlledWrite<A, W>
where A: IoControlsAccess
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let buf = truncate_bytes(buf);
        let written_num_bytes = self.underlying_wrt.write(buf)?;
        self.io_controls_access
            .apply(|io_controls| io_controls.consume_blocking(written_num_bytes))?;
        Ok(written_num_bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        // We voluntarily avoid to check the kill switch on flush.
        // This is because the `RAMDirectory` currently panics if flush
        // is not called before `Drop`.
        let _guard = self.check_if_alive();
        self.underlying_wrt.flush()
    }
}

#[cfg(test)]
mod tests {
    use std::io::{IoSlice, Write};
    use std::time::Duration;

    use tokio::io::{sink, AsyncWriteExt};
    use tokio::time::Instant;

    use crate::io::{IoControls, IoControlsAccess};

    #[tokio::test]
    async fn test_controlled_writer_limited_async() {
        let io_controls = IoControls::default().set_throughput_limit(2_000_000f64);
        let mut controlled_write = io_controls.clone().wrap_write(sink());
        let buf = vec![44u8; 1_000];
        let start = Instant::now();
        // We write 200 KB
        for _ in 0..200 {
            controlled_write.write_all(&buf).await.unwrap();
        }
        controlled_write.flush().await.unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(50));
        assert!(elapsed <= Duration::from_millis(150));
        assert_eq!(io_controls.num_bytes(), 200_000u64);
    }

    #[tokio::test]
    async fn test_controlled_writer_no_limit_async() {
        let io_controls = IoControls::default();
        let mut controlled_write = io_controls.clone().wrap_write(sink());
        let buf = vec![44u8; 1_000];
        let start = Instant::now();
        // We write 2MB
        for _ in 0..2_000 {
            controlled_write.write_all(&buf).await.unwrap();
        }
        controlled_write.flush().await.unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed <= Duration::from_millis(5));
        assert_eq!(io_controls.num_bytes(), 2_000_000u64);
    }

    #[test]
    fn test_controlled_writer_limited_sync() {
        let io_controls = IoControls::default().set_throughput_limit(2_000_000f64);
        let mut controlled_write = io_controls.clone().wrap_write(std::io::sink());
        let buf = vec![44u8; 1_000];
        let start = Instant::now();
        // We write 200 KB
        for _ in 0..200 {
            controlled_write.write_all(&buf).unwrap();
        }
        controlled_write.flush().unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(50));
        assert!(elapsed <= Duration::from_millis(150));
        assert_eq!(io_controls.num_bytes(), 200_000u64);
    }

    #[test]
    fn test_controlled_writer_no_limit_sync() {
        let io_controls = IoControls::default();
        let mut controlled_write = io_controls.clone().wrap_write(std::io::sink());
        let buf = vec![44u8; 1_000];
        let start = Instant::now();
        // We write 2MB
        for _ in 0..2_000 {
            controlled_write.write_all(&buf).unwrap();
        }
        controlled_write.flush().unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed <= Duration::from_millis(5));
        assert_eq!(io_controls.num_bytes(), 2_000_000u64);
    }

    #[test]
    fn test_truncate_io_slices_one_slice_too_long_corner_case() {
        let one_slice = IoSlice::new(&b"abcdef"[..]);
        assert_eq!(super::quirky_truncate_slices(&[one_slice], 2).len(), 1);
    }

    #[test]
    fn test_truncate_io_empty() {
        assert_eq!(super::quirky_truncate_slices(&[], 2).len(), 0);
    }

    #[test]
    fn test_truncate_io_slices() {
        let slices = &[
            IoSlice::new(&b"abc"[..]),
            IoSlice::new(&b"defg"[..]),
            IoSlice::new(&b"hi"[..]),
        ];
        assert_eq!(super::quirky_truncate_slices(slices, 0).len(), 1);
        assert_eq!(super::quirky_truncate_slices(slices, 6).len(), 1);
        assert_eq!(super::quirky_truncate_slices(slices, 7).len(), 2);
        assert_eq!(super::quirky_truncate_slices(slices, 9).len(), 3);
        assert_eq!(super::quirky_truncate_slices(slices, 10).len(), 3);
    }
}
