// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/cluster.proto");
    println!("cargo:rerun-if-changed=proto/search_api.proto");
    println!("cargo:rerun-if-changed=proto/push_api.proto");

    let mut prost_config = prost_build::Config::default();
    // prost_config.type_attribute("LeafSearchResponse", "#[derive(Default)]");
    prost_config.protoc_arg("--experimental_allow_proto3_optional");
    tonic_build::configure()
        .type_attribute(
            ".",
            "#[derive(Serialize, Deserialize)]\n#[serde(rename_all = \"camelCase\")]",
        )
        .out_dir("src/")
        .compile_with_config(
            prost_config,
            &[
                "./proto/cluster.proto",
                "./proto/search_api.proto",
                "./proto/push_api.proto",
                // "./oltp/opentelemetry/proto/collector/logs/v1/logs_service.proto",
            ],
            &["./proto", "./oltp"],
        )?;
    tonic_build::configure().out_dir("src/").compile(
        &[
            "./oltp/opentelemetry/proto/collector/logs/v1/logs_service.proto",
            "./oltp/opentelemetry/proto/collector/trace/v1/trace_service.proto",
        ],
        &["./oltp"],
    )?;
    Ok(())
}
