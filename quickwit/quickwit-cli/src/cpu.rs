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

use quickwit_common::metrics::CPU_METRICS;
use sysinfo::{CpuRefreshKind, RefreshKind, System};

async fn cpu_metrics_loop() {
    let cpu_metrics = CPU_METRICS.clone();

    let mut poll_interval = tokio::time::interval(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
    let mut system =
        System::new_with_specifics(RefreshKind::nothing().with_cpu(CpuRefreshKind::everything()));

    loop {
        poll_interval.tick().await;
        system.refresh_cpu_usage();

        let cpu_usage = system.global_cpu_usage();
        cpu_metrics.dd_cpu_usage.set(cpu_usage as f64);
        let uptime = System::uptime();
        cpu_metrics.dd_uptime.set(uptime as f64);
    }
}

pub(crate) fn start_cpu_metrics_loop() {
    tokio::task::spawn(cpu_metrics_loop());
}
