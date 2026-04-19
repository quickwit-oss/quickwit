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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate Rust code from the vendored DD Agent payload protos.
    // The .proto files import each other as "proto/process/agent.proto",
    // so the include root is the crate root (the directory containing "proto/").
    //
    // Source: github.com/DataDog/agent-payload (see README.md for version).
    let out_dir = std::env::var("OUT_DIR")?;
    prost_build::Config::new()
        .out_dir(&out_dir)
        .compile_protos(
            &[
                "proto/process/connections.proto",
                "proto/process/agent.proto",
            ],
            &["."],
        )?;

    println!("cargo:rerun-if-changed=proto/process/connections.proto");
    println!("cargo:rerun-if-changed=proto/process/agent.proto");
    Ok(())
}
