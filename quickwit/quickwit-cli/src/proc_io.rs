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

use std::time::Duration;

use procfs::ProcResult;
use procfs::process::Process;
use quickwit_common::metrics::{IO_METRICS, IntCounter};
use quickwit_common::rate_limited_tracing::rate_limited_warn;
use tracing::error;

const PROC_IO_METRICS_POLLING_INTERVAL: Duration = Duration::from_secs(5);

/// Reads `/proc/self/io` on a fixed interval and publishes the cumulative byte and syscall
/// counters as Prometheus counters.
///
/// `/proc/self/io` exposes monotonic per-process counters maintained by the kernel. Prometheus
/// counters are also monotonic but only expose an `inc_by(delta)` API, so we keep the previously
/// observed value and increment by the difference on each poll.
async fn proc_io_metrics_loop() -> ProcResult<()> {
    let process = Process::myself()?;

    let mut previous_read_bytes: u64 = 0;
    let mut previous_write_bytes: u64 = 0;
    let mut previous_read_syscalls: u64 = 0;
    let mut previous_write_syscalls: u64 = 0;

    let mut poll_interval = tokio::time::interval(PROC_IO_METRICS_POLLING_INTERVAL);
    poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        poll_interval.tick().await;

        let io = match process.io() {
            Ok(io) => io,
            Err(error) => {
                rate_limited_warn!(
                    limit_per_min = 1,
                    %error,
                    "failed to read /proc/self/io"
                );
                continue;
            }
        };
        increment_counter(
            &IO_METRICS.read_bytes_total,
            io.read_bytes,
            &mut previous_read_bytes,
        );
        increment_counter(
            &IO_METRICS.write_bytes_total,
            io.write_bytes,
            &mut previous_write_bytes,
        );
        increment_counter(
            &IO_METRICS.read_syscalls_total,
            io.syscr,
            &mut previous_read_syscalls,
        );
        increment_counter(
            &IO_METRICS.write_syscalls_total,
            io.syscw,
            &mut previous_write_syscalls,
        );
    }
}

fn increment_counter(counter: &IntCounter, current: u64, previous: &mut u64) {
    debug_assert!(
        current >= *previous,
        "/proc/self/io counters should be monotonic for a given PID"
    );
    if current >= *previous {
        counter.inc_by(current - *previous);
    }
    *previous = current;
}

pub fn start_proc_io_metrics_loop() {
    tokio::task::spawn(async {
        if let Err(error) = proc_io_metrics_loop().await {
            error!(%error, "failed to collect /proc/self/io metrics");
        }
    });
}
