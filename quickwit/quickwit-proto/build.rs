// Copyright (C) 2022 Quickwit, Inc.
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
    println!("cargo:rerun-if-changed=proto/ingest_api.proto");
    println!("cargo:rerun-if-changed=proto/metastore_api.proto");
    println!("cargo:rerun-if-changed=proto/search_api.proto");

    println!("cargo:rerun-if-changed=jaeger/model.proto");
    println!("cargo:rerun-if-changed=jaeger/storage.proto");

    println!(
        "cargo:rerun-if-changed=otlp/opentelemetry/proto/collector/logs/v1/logs_service.proto"
    );
    println!(
        "cargo:rerun-if-changed=otlp/opentelemetry/proto/collector/trace/v1/trace_service.proto"
    );
    println!("cargo:rerun-if-changed=otlp/opentelemetry/proto/common/v1/common.proto");
    println!("cargo:rerun-if-changed=otlp/opentelemetry/proto/logs/v1/logs.proto");
    println!("cargo:rerun-if-changed=otlp/opentelemetry/proto/resource/v1/resource.proto");
    println!("cargo:rerun-if-changed=otlp/opentelemetry/proto/trace/v1/trace.proto");

    {
        let mut prost_config = prost_build::Config::default();
        prost_config.protoc_arg("--experimental_allow_proto3_optional");

        tonic_build::configure()
            .type_attribute(".", "#[derive(Serialize, Deserialize)]")
            .type_attribute("DeleteQuery", "#[serde(default)]")
            .field_attribute(
                "DeleteQuery.start_timestamp",
                "#[serde(skip_serializing_if = \"Option::is_none\")]",
            )
            .field_attribute(
                "DeleteQuery.end_timestamp",
                "#[serde(skip_serializing_if = \"Option::is_none\")]",
            )
            .type_attribute("OutputFormat", "#[serde(rename_all = \"snake_case\")]")
            .out_dir("src/")
            .compile_with_config(
                prost_config,
                &[
                    "./proto/ingest_api.proto",
                    "./proto/metastore_api.proto",
                    "./proto/search_api.proto",
                ],
                &["./proto"],
            )?;
    }
    {
        let mut prost_config = prost_build::Config::default();
        prost_config.type_attribute("Operation", "#[derive(Eq, Ord, PartialOrd)]");

        tonic_build::configure()
            .out_dir("src/")
            .compile_with_config(
                prost_config,
                &["./jaeger/model.proto", "./jaeger/storage.proto"],
                &["./jaeger", "/opt/homebrew/Cellar/protobuf/21.5/include"],
            )?;
    }
    tonic_build::configure()
        .type_attribute(".", "#[derive(Serialize, Deserialize)]")
        .out_dir("src/")
        .compile(
            &[
                "./otlp/opentelemetry/proto/common/v1/common.proto", // Must be compiled first.
                "./otlp/opentelemetry/proto/resource/v1/resource.proto", // Must be compiled second.
                "./otlp/opentelemetry/proto/logs/v1/logs.proto",
                "./otlp/opentelemetry/proto/trace/v1/trace.proto",
                "./otlp/opentelemetry/proto/collector/logs/v1/logs_service.proto",
                "./otlp/opentelemetry/proto/collector/trace/v1/trace_service.proto",
            ],
            &["./otlp"],
        )?;
    Ok(())
}
