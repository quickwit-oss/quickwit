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

//! Compiles every `.proto` under `proto/` into Rust via `prost-build` and
//! checks the generated sources into `src/codegen/` so they're visible in
//! code review and greppable locally.
//!
//! Regenerating: run `cargo build -p pomsky-intake`. Any change under `proto/`
//! triggers codegen thanks to the `rerun-if-changed` emits below. Commit the
//! resulting `src/codegen/*.rs` changes alongside the `.proto` edit.

use std::path::PathBuf;

use glob::glob;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = find_protos("proto");
    if protos.is_empty() {
        return Ok(());
    }

    let out_dir = PathBuf::from("src/codegen");
    std::fs::create_dir_all(&out_dir)?;

    // Include root is the crate root because `connections.proto` imports
    // `proto/process/agent.proto` by its full relative path.
    prost_build::Config::new()
        .out_dir(&out_dir)
        .compile_protos(&protos, &["."])?;

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }
    Ok(())
}

fn find_protos(dir: &str) -> Vec<PathBuf> {
    glob(&format!("{dir}/**/*.proto"))
        .expect("static glob pattern")
        .flatten()
        .collect()
}
