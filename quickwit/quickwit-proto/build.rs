// Copyright (C) 2023 Quickwit, Inc.
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

use std::path::PathBuf;

use glob::glob;
use quickwit_codegen::Codegen;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prost + tonic + Quickwit codegen for control plane, indexing, and ingest services.
    //
    // Control plane
    Codegen::run(
        &["protos/quickwit/control_plane.proto"],
        "src/codegen/quickwit",
        "crate::control_plane::ControlPlaneResult",
        "crate::control_plane::ControlPlaneError",
        &["protos"],
    )
    .unwrap();

    // Indexing Service
    Codegen::run(
        &["protos/quickwit/indexing.proto"],
        "src/codegen/quickwit",
        "crate::indexing::IndexingResult",
        "crate::indexing::IndexingError",
        &[],
    )
    .unwrap();

    // Ingest service
    let mut prost_config = prost_build::Config::default();
    prost_config.bytes(["DocBatchV2.doc_buffer"]);

    Codegen::run_with_config(
        &[
            "protos/quickwit/ingester.proto",
            "protos/quickwit/router.proto",
        ],
        "src/codegen/quickwit",
        "crate::ingest::IngestV2Result",
        "crate::ingest::IngestV2Error",
        &["protos"],
        prost_config,
    )
    .unwrap();

    // "Classic" prost + tonic codegen for metastore and search services.
    let mut prost_config = prost_build::Config::default();
    prost_config
        .bytes(["DocBatchV2.doc_buffer"])
        .protoc_arg("--experimental_allow_proto3_optional");

    tonic_build::configure()
        .enum_attribute(".", "#[serde(rename_all=\"snake_case\")]")
        .field_attribute("DeleteQuery.index_uid", "#[serde(alias = \"index_id\")]")
        .field_attribute("DeleteQuery.query_ast", "#[serde(alias = \"query\")]")
        .field_attribute(
            "DeleteQuery.start_timestamp",
            "#[serde(skip_serializing_if = \"Option::is_none\")]",
        )
        .field_attribute(
            "DeleteQuery.end_timestamp",
            "#[serde(skip_serializing_if = \"Option::is_none\")]",
        )
        .field_attribute(
            "Shard.follower_id",
            "#[serde(default, skip_serializing_if = \"Option::is_none\")]",
        )
        .field_attribute(
            "Shard.publish_position_inclusive",
            "#[serde(default, skip_serializing_if = \"String::is_empty\")]",
        )
        .field_attribute(
            "Shard.publish_token",
            "#[serde(default, skip_serializing_if = \"Option::is_none\")]",
        )
        .field_attribute(
            "Shard.replication_position_inclusive",
            "#[serde(default, skip_serializing_if = \"Option::is_none\")]",
        )
        .type_attribute(".", "#[derive(Serialize, Deserialize, utoipa::ToSchema)]")
        .type_attribute("PartialHit.sort_value", "#[derive(Copy)]")
        .type_attribute("SearchRequest", "#[derive(Eq, Hash)]")
        .type_attribute("Shard", "#[derive(Eq)]")
        .type_attribute("SortByValue", "#[derive(Ord, PartialOrd)]")
        .type_attribute("SortField", "#[derive(Eq, Hash)]")
        .out_dir("src/codegen/quickwit")
        .compile_with_config(
            prost_config,
            &[
                "protos/quickwit/metastore.proto",
                "protos/quickwit/search.proto",
            ],
            &["protos"],
        )?;

    // Jaeger proto
    let protos = find_protos("protos/third-party/jaeger");

    let mut prost_config = prost_build::Config::default();
    prost_config.type_attribute("Operation", "#[derive(Eq, Ord, PartialOrd)]");

    tonic_build::configure()
        .out_dir("src/codegen/jaeger")
        .compile_with_config(
            prost_config,
            &protos,
            &["protos/third-party/jaeger", "protos/third-party"],
        )?;

    // OTEL proto
    let mut prost_config = prost_build::Config::default();
    prost_config.protoc_arg("--experimental_allow_proto3_optional");

    let protos = find_protos("protos/third-party/opentelemetry");
    tonic_build::configure()
        .type_attribute(".", "#[derive(Serialize, Deserialize)]")
        .type_attribute("StatusCode", r#"#[serde(rename_all = "snake_case")]"#)
        .out_dir("src/codegen/opentelemetry")
        .compile_with_config(prost_config, &protos, &["protos/third-party"])?;
    Ok(())
}

fn find_protos(dir_path: &str) -> Vec<PathBuf> {
    glob(&format!("{dir_path}/**/*.proto"))
        .unwrap()
        .flatten()
        .collect()
}
