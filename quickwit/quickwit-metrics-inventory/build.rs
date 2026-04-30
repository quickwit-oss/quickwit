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

fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    // Prevent the linker from stripping inventory-submitted statics that
    // live in dependency rlibs. Without this, the linker sees that the
    // inventory binary never references symbols from dependency crates
    // and drops them — along with the MetricInfo entries registered via
    // inventory::submit!().
    match target_os.as_str() {
        "macos" => {
            println!("cargo::rustc-link-arg-bins=-Wl,-all_load");
        }
        "linux" => {
            println!("cargo::rustc-link-arg-bins=-Wl,--whole-archive");
        }
        other => {
            eprintln!(
                "cargo:warning=quickwit-metrics-inventory: no whole-archive linker flag for \
                 target OS '{other}'; inventory discovery from dependency crates may not work"
            );
        }
    }
}
