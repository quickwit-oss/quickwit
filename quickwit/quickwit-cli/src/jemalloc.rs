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

use quickwit_common::metrics::MEMORY_METRICS;
use tikv_jemallocator::Jemalloc;
use tracing::error;

#[cfg(feature = "jemalloc-profiled")]
#[global_allocator]
pub static GLOBAL: quickwit_common::jemalloc_profiled::JemallocProfiled =
    quickwit_common::jemalloc_profiled::JemallocProfiled(Jemalloc);

#[cfg(not(feature = "jemalloc-profiled"))]
#[global_allocator]
pub static GLOBAL: Jemalloc = Jemalloc;

const JEMALLOC_METRICS_POLLING_INTERVAL: Duration = Duration::from_secs(1);

pub async fn jemalloc_metrics_loop() -> tikv_jemalloc_ctl::Result<()> {
    let memory_metrics = MEMORY_METRICS.clone();

    // Obtain a MIB for the `epoch`, `stats.active`, `stats.allocated`, and `stats.resident` keys:
    let epoch_mib = tikv_jemalloc_ctl::epoch::mib()?;
    let active_mib = tikv_jemalloc_ctl::stats::active::mib()?;
    let allocated_mib = tikv_jemalloc_ctl::stats::allocated::mib()?;
    let resident_mib = tikv_jemalloc_ctl::stats::resident::mib()?;

    let mut poll_interval = tokio::time::interval(JEMALLOC_METRICS_POLLING_INTERVAL);

    loop {
        poll_interval.tick().await;

        // Many statistics are cached and only updated when the epoch is advanced:
        epoch_mib.advance()?;

        // Read statistics using MIB keys:
        let active = active_mib.read()?;
        memory_metrics.active_bytes.set(active as i64);

        let allocated = allocated_mib.read()?;
        memory_metrics.allocated_bytes.set(allocated as i64);

        let resident = resident_mib.read()?;
        memory_metrics.resident_bytes.set(resident as i64);
    }
}

pub fn start_jemalloc_metrics_loop() {
    tokio::task::spawn(async {
        if let Err(error) = jemalloc_metrics_loop().await {
            error!(%error, "failed to collect metrics from jemalloc");
        }
    });
}
