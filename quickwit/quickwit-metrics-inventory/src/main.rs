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

//! Enumerates all registered `MetricInfo` entries via [`inventory`].
//!
//! **Do not run this binary directly** — it will only see metrics from
//! crates listed in this crate's `Cargo.toml` dependencies. To discover
//! metrics from all workspace crates, use the wrapper script which patches
//! in reverse dependencies:
//!
//! ```sh
//! ./scripts/run_inventory.sh
//! ```
//!
//! The script temporarily adds `extern crate` lines and `Cargo.toml`
//! dependencies for every crate that depends on `quickwit-metrics`, then
//! restores the files on exit. The `build.rs` ensures the linker pulls in
//! all inventory submissions even without explicit symbol references.

use std::collections::BTreeMap;

fn format_key(info: &quickwit_metrics::MetricInfo) -> String {
    if info.static_labels.is_empty() {
        info.key_name.to_string()
    } else {
        let pairs: Vec<String> = info
            .static_labels
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        format!("{}{{{}}}", info.key_name, pairs.join(", "))
    }
}

fn main() {
    let mut by_module: BTreeMap<&str, BTreeMap<String, &quickwit_metrics::MetricInfo>> =
        BTreeMap::new();

    for info in quickwit_metrics::metrics_info() {
        let module = info.metadata.module_path().unwrap_or("<unknown>");
        by_module
            .entry(module)
            .or_default()
            .insert(format_key(info), info);
    }

    for (module, metrics) in &by_module {
        let max_key_len = metrics.keys().map(|k| k.len()).max().unwrap_or(0);
        println!("{module}");
        for (key, info) in metrics {
            println!(
                "  {key:<width$}    kind: {}, observable: {}",
                format!("{:?}", info.kind).to_lowercase(),
                info.observable,
                width = max_key_len
            );
            println!("    {}", info.description);
        }
        println!();
    }
}
