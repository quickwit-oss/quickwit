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

use std::time::Duration;

use quickwit_common::metrics::MEMORY_METRICS;
use tikv_jemallocator::Jemalloc;
use tracing::error;

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
