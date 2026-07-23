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

use quickwit_metrics::{Labels, LazyCounter, LazyGauge, gauge, labels, lazy_counter, lazy_gauge};
use sysinfo::{CpuRefreshKind, Disks, MemoryRefreshKind, Networks, RefreshKind, System};

static UPTIME: LazyGauge = lazy_gauge!(
    name: "uptime.gauge",
    description: "Process uptime for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

static UPTIME_LABELS: LazyLock<Labels<4>> = LazyLock::new(|| {
    let env_label_value = |env_var_key| {
        quickwit_common::get_from_env_opt::<String>(env_var_key, false)
            .unwrap_or_else(|| "N/A".to_string())
    };

    let uptime_labels = labels!(
        "kube_limits_cpu" => env_label_value("KUBERNETES_LIMITS_CPU"),
        "kube_limits_memory" => env_label_value("KUBERNETES_LIMITS_MEMORY"),
        "kube_requests_cpu" => env_label_value("KUBERNETES_REQUESTS_CPU"),
        "kube_requests_memory" => env_label_value("KUBERNETES_REQUESTS_MEMORY"),
    );
    uptime_labels
});

static CPU_USAGE: LazyGauge = lazy_gauge!(
    name: "usage.gauge",
    description: "CPU usage for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "cpu",
    separator: ".",
);

static MEMORY_USAGE: LazyGauge = lazy_gauge!(
    name: "usage.gauge",
    description: "Memory usage for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "memory",
    separator: ".",
);

static SYSTEM_CPU_USAGE: LazyGauge = lazy_gauge!(
    name: "cpu_usage",
    description: "CPU usage percentage.",
    subsystem: "system",
);

static SYSTEM_MEMORY_USAGE: LazyGauge = lazy_gauge!(
    name: "memory_usage",
    description: "Memory usage percentage.",
    subsystem: "system",
);

static DISK_BYTES_READ: LazyCounter = lazy_counter!(
    name: "disk.bytes_read.counter",
    description: "Disk bytes read for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

static DISK_BYTES_WRITTEN: LazyCounter = lazy_counter!(
    name: "disk.bytes_written.counter",
    description: "Disk bytes written for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

static DISK_SIZE: LazyGauge = lazy_gauge!(
    name: "disk.total_space.gauge",
    description: "Total disk space for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

static DISK_SPACE_AVAILABLE: LazyGauge = lazy_gauge!(
    name: "disk.available_space.gauge",
    description: "Available disk space for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

static NETWORK_BYTES_RECV: LazyCounter = lazy_counter!(
    name: "network.bytes_recv.counter",
    description: "Network bytes received for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

static NETWORK_BYTES_SENT: LazyCounter = lazy_counter!(
    name: "network.bytes_sent.counter",
    description: "Network bytes sent for legacy Datadog dashboards.",
    system: "cloudprem",
    subsystem: "",
    separator: ".",
);

async fn sys_metrics_loop() {
    let mut poll_interval = tokio::time::interval(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
    let mut system = System::new_with_specifics(
        RefreshKind::nothing()
            .with_cpu(CpuRefreshKind::everything())
            .with_memory(MemoryRefreshKind::nothing().with_ram()),
    );
    let mut disks = Disks::new_with_refreshed_list();
    let mut networks = Networks::new_with_refreshed_list();

    loop {
        poll_interval.tick().await;
        system.refresh_cpu_usage();
        system.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
        disks.refresh(true);
        networks.refresh(true);

        let cpu_usage = system.global_cpu_usage();
        CPU_USAGE.set(cpu_usage as f64);
        SYSTEM_CPU_USAGE.set(cpu_usage as f64);
        if system.total_memory() != 0 {
            let memory_usage = system.used_memory() as f64 / system.total_memory() as f64 * 100.0;
            MEMORY_USAGE.set(memory_usage);
            SYSTEM_MEMORY_USAGE.set(memory_usage);
        }

        let uptime = System::uptime();
        gauge!(parent: UPTIME, labels: [UPTIME_LABELS.clone()]).set(uptime as f64);

        let mut total_size = 0;
        let mut total_space_available = 0;
        for disk in &disks {
            let usage = disk.usage();
            DISK_BYTES_READ.inc_by(usage.total_read_bytes);
            DISK_BYTES_WRITTEN.inc_by(usage.total_written_bytes);

            total_size += disk.total_space();
            total_space_available += disk.available_space();
        }
        DISK_SIZE.set(total_size as f64);
        DISK_SPACE_AVAILABLE.set(total_space_available as f64);

        for (_, network) in &networks {
            let received = network.received();
            let transmitted = network.transmitted();
            NETWORK_BYTES_RECV.inc_by(received);
            NETWORK_BYTES_SENT.inc_by(transmitted);
        }
    }
}

pub(crate) fn start_sys_metrics_loop() {
    tokio::task::spawn(sys_metrics_loop());
}
