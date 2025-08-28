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

use quickwit_common::metrics::SYSTEM_METRICS;
use sysinfo::{CpuRefreshKind, Disks, Networks, RefreshKind, System};

async fn sys_metrics_loop() {
    let sys_metrics = SYSTEM_METRICS.clone();

    let mut poll_interval = tokio::time::interval(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
    let mut system =
        System::new_with_specifics(RefreshKind::nothing().with_cpu(CpuRefreshKind::everything()));
    let mut disks = Disks::new_with_refreshed_list();
    let mut networks = Networks::new_with_refreshed_list();

    loop {
        poll_interval.tick().await;
        system.refresh_cpu_usage();
        disks.refresh(true);
        networks.refresh(true);

        let cpu_usage = system.global_cpu_usage();
        sys_metrics.dd_cpu_usage.set(cpu_usage as f64);

        let uptime = System::uptime();
        sys_metrics.dd_uptime.set(uptime as f64);

        let mut total_size = 0;
        let mut total_space_available = 0;
        for disk in &disks {
            let usage = disk.usage();
            sys_metrics
                .dd_disk_bytes_read
                .increment(usage.total_read_bytes);
            sys_metrics
                .dd_disk_bytes_written
                .increment(usage.total_written_bytes);

            total_size += disk.total_space();
            total_space_available += disk.available_space();
        }
        sys_metrics.dd_disk_size.set(total_size as f64);
        sys_metrics
            .dd_disk_space_available
            .set(total_space_available as f64);

        for (_, network) in &networks {
            let received = network.total_received();
            let transmitted = network.total_transmitted();
            sys_metrics.dd_network_bytes_recv.increment(received);
            sys_metrics.dd_network_bytes_sent.increment(transmitted);
        }
    }
}

pub(crate) fn start_sys_metrics_loop() {
    tokio::task::spawn(sys_metrics_loop());
}
