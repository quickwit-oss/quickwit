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

fn main() {
    for info in quickwit_metrics::metrics_info() {
        println!(
            "[{:<9}] {}: {} (observable: {})",
            format!("{:?}", info.kind),
            info.key_name,
            info.description,
            info.observable
        );
    }
}
