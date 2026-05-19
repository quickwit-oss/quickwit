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

use std::sync::LazyLock;

use metrics::{Counter, Gauge, Label, counter, gauge};
use sysinfo::{CpuRefreshKind, Disks, Networks, RefreshKind, System};

struct SystemMetrics {
    uptime: Gauge,
    cpu_usage: Gauge,
    disk_bytes_read: Counter,
    disk_bytes_written: Counter,
    disk_size: Gauge,
    disk_space_available: Gauge,
    network_bytes_recv: Counter,
    network_bytes_sent: Counter,
}

impl Default for SystemMetrics {
    fn default() -> Self {
        let mut uptime_labels = Vec::with_capacity(4);
        let keys = [
            ("KUBERNETES_LIMITS_CPU", "kube_limits_cpu"),
            ("KUBERNETES_LIMITS_MEMORY", "kube_limits_memory"),
            ("KUBERNETES_REQUESTS_CPU", "kube_requests_cpu"),
            ("KUBERNETES_REQUESTS_MEMORY", "kube_requests_memory"),
        ];
        for (env_var_key, label_key) in keys {
            if let Some(label_val) = quickwit_common::get_from_env_opt::<String>(env_var_key, false)
            {
                uptime_labels.push(Label::new(label_key, label_val));
            }
        }
        Self {
            uptime: gauge!("uptime.gauge", uptime_labels),
            cpu_usage: gauge!("cpu.usage.gauge"),
            disk_bytes_read: counter!("disk.bytes_read.counter"),
            disk_bytes_written: counter!("disk.bytes_written.counter"),
            disk_size: gauge!("disk.total_space.gauge"),
            disk_space_available: gauge!("disk.available_space.gauge"),
            network_bytes_recv: counter!("network.bytes_recv.counter"),
            network_bytes_sent: counter!("network.bytes_sent.counter"),
        }
    }
}

static SYSTEM_METRICS: LazyLock<SystemMetrics> = LazyLock::new(SystemMetrics::default);

async fn sys_metrics_loop() {
    let sys_metrics = &*SYSTEM_METRICS;

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
        sys_metrics.cpu_usage.set(cpu_usage as f64);

        let uptime = System::uptime();
        sys_metrics.uptime.set(uptime as f64);

        let mut total_size = 0;
        let mut total_space_available = 0;
        for disk in &disks {
            let usage = disk.usage();
            sys_metrics
                .disk_bytes_read
                .increment(usage.total_read_bytes);
            sys_metrics
                .disk_bytes_written
                .increment(usage.total_written_bytes);

            total_size += disk.total_space();
            total_space_available += disk.available_space();
        }
        sys_metrics.disk_size.set(total_size as f64);
        sys_metrics
            .disk_space_available
            .set(total_space_available as f64);

        for (_, network) in &networks {
            let received = network.received();
            let transmitted = network.transmitted();
            sys_metrics.network_bytes_recv.increment(received);
            sys_metrics.network_bytes_sent.increment(transmitted);
        }
    }
}

pub(crate) fn start_sys_metrics_loop() {
    tokio::task::spawn(sys_metrics_loop());
}
