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

// Proto codegen only runs when the `grpc` feature is enabled. Gating both the
// imports and the body with `cfg` keeps `prost-build` / `tonic-prost-build`
// out of the dep graph entirely when the feature is off.

#[cfg(feature = "grpc")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut prost_config = prost_build::Config::default();
    prost_config.file_descriptor_set_path("src/codegen/datafusion_descriptor.bin");

    tonic_prost_build::configure()
        .out_dir("src/codegen")
        .compile_with_config(
            prost_config,
            &[std::path::PathBuf::from("proto/datafusion.proto")],
            &[std::path::PathBuf::from("proto")],
        )?;
    Ok(())
}

#[cfg(not(feature = "grpc"))]
fn main() {}
