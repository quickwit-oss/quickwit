// Copyright (C) 2022 Quickwit, Inc.
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

use quickwit_common::metrics::new_gauge;
use tikv_jemallocator::Jemalloc;
use tracing::error;

#[global_allocator]
pub static GLOBAL: Jemalloc = Jemalloc;

pub const JEMALLOC_METRICS_POLLING_INTERVAL: Duration = Duration::from_secs(1);

pub async fn jemalloc_metrics_loop() -> tikv_jemalloc_ctl::Result<()> {
    let allocated_gauge = new_gauge(
        "allocated_num_bytes",
        "Number of bytes allocated memory, as reported by jemallocated.",
        "quickwit",
    );

    // Obtain a MIB for the `epoch`, `stats.allocated`, and
    // `atats.resident` keys:
    let epoch_management_information_base = tikv_jemalloc_ctl::epoch::mib()?;
    let allocated = tikv_jemalloc_ctl::stats::allocated::mib()?;

    let mut poll_interval = tokio::time::interval(JEMALLOC_METRICS_POLLING_INTERVAL);

    loop {
        poll_interval.tick().await;

        // Many statistics are cached and only updated
        // when the epoch is advanced:
        epoch_management_information_base.advance()?;

        // Read statistics using MIB key:
        let allocated = allocated.read()?;

        allocated_gauge.set(allocated as i64);
    }
}

pub fn start_jemalloc_metrics_loop() {
    tokio::task::spawn(async {
        if let Err(jemalloc_metrics_err) = jemalloc_metrics_loop().await {
            error!(err=?jemalloc_metrics_err, "Failed to gather metrics from jemalloc.");
        }
    });
}
