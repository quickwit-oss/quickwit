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

//! Enumerates all registered `MetricInfo` entries via `inventory`.
//!
//! **Do not run this binary directly** — it will only see metrics from
//! crates listed in this crate's `Cargo.toml` dependencies. To discover
//! metrics from all workspace crates, use the wrapper script which patches
//! in reverse dependencies:
//!
//! ```sh
//! ./scripts/run.sh
//! ```
//!
//! The script temporarily adds `extern crate` lines and `Cargo.toml`
//! dependencies for every crate that depends on `quickwit-metrics`, then
//! restores the files on exit. The `build.rs` ensures the linker pulls in
//! all inventory submissions even without explicit symbol references.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::process::ExitCode;

use quickwit_metrics::{MetricInfo, MetricKind};
use serde::Serialize;

#[derive(Serialize)]
struct MetricEntry {
    module: &'static str,
    kind: &'static str,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    labels: BTreeMap<&'static str, &'static str>,
    description: &'static str,
}

/// Stable names for the inventory, independent of the `Debug` representation.
fn kind_name(kind: MetricKind) -> &'static str {
    match kind {
        MetricKind::Counter => "counter",
        MetricKind::Gauge => "gauge",
        MetricKind::Histogram => "histogram",
    }
}

fn metric_entry(info: &MetricInfo) -> MetricEntry {
    MetricEntry {
        module: info.metadata.module_path().unwrap_or("<unknown>"),
        kind: kind_name(info.kind),
        labels: info.static_labels.iter().copied().collect(),
        description: info.description,
    }
}

/// Prints the inventory as YAML, keyed by metric name in alphabetical order:
///
/// ```yaml
/// quickwit_metric_name:
///   module: quickwit_crate::module
///   kind: counter
///   labels:          # only if the metric has static labels
///     label: value
///   description: ...
/// ```
///
/// Fails if two declarations share the same metric name: the inventory is
/// keyed by name, so one of them would otherwise be silently dropped.
fn main() -> ExitCode {
    let mut metrics: BTreeMap<&'static str, MetricEntry> = BTreeMap::new();
    let mut duplicates = false;

    for info in quickwit_metrics::metrics_info() {
        let entry = metric_entry(info);
        match metrics.entry(info.key_name) {
            Entry::Vacant(vacant) => {
                vacant.insert(entry);
            }
            Entry::Occupied(occupied) => {
                eprintln!(
                    "error: metric `{}` is declared more than once (in `{}` and `{}`)",
                    info.key_name,
                    occupied.get().module,
                    entry.module
                );
                duplicates = true;
            }
        }
    }
    if duplicates {
        return ExitCode::FAILURE;
    }

    let yaml = serde_yaml::to_string(&metrics).expect("inventory should serialize to YAML");
    print!("{yaml}");
    ExitCode::SUCCESS
}
