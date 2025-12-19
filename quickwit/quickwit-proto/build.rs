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

use std::path::PathBuf;

use glob::glob;
use quickwit_codegen::Codegen;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prost + tonic + Quickwit codegen for control plane, indexing, metastore, ingest and search
    // services.
    //
    // Cluster service.
    let mut prost_config = prost_build::Config::default();
    prost_config.file_descriptor_set_path("src/codegen/quickwit/cluster_descriptor.bin");

    Codegen::builder()
        .with_prost_config(prost_config)
        .with_protos(&["protos/quickwit/cluster.proto"])
        .with_output_dir("src/codegen/quickwit")
        .with_result_type_path("crate::cluster::ClusterResult")
        .with_error_type_path("crate::cluster::ClusterError")
        .generate_rpc_name_impls()
        .run()
        .unwrap();

    // Control plane.
    let mut prost_config = prost_build::Config::default();
    prost_config.file_descriptor_set_path("src/codegen/quickwit/control_plane_descriptor.bin");

    prost_config
        .extern_path(
            ".quickwit.common.DocMappingUid",
            "crate::types::DocMappingUid",
        )
        .extern_path(".quickwit.common.IndexUid", "crate::types::IndexUid");

    Codegen::builder()
        .with_prost_config(prost_config)
        .with_protos(&["protos/quickwit/control_plane.proto"])
        .with_includes(&["protos"])
        .with_output_dir("src/codegen/quickwit")
        .with_result_type_path("crate::control_plane::ControlPlaneResult")
        .with_error_type_path("crate::control_plane::ControlPlaneError")
        .run()
        .unwrap();

    // Developer service.
    let mut prost_config = prost_build::Config::default();
    prost_config
        .bytes(["GetDebugInfoResponse.debug_info_json"])
        .file_descriptor_set_path("src/codegen/quickwit/developer_descriptor.bin");

    Codegen::builder()
        .with_prost_config(prost_config)
        .with_protos(&["protos/quickwit/developer.proto"])
        .with_output_dir("src/codegen/quickwit")
        .with_result_type_path("crate::developer::DeveloperResult")
        .with_error_type_path("crate::developer::DeveloperError")
        .generate_rpc_name_impls()
        .run()
        .unwrap();

    // Indexing Service.
    let mut prost_config = prost_build::Config::default();
    prost_config
        .extern_path(
            ".quickwit.indexing.PipelineUid",
            "crate::types::PipelineUid",
        )
        .extern_path(".quickwit.common.IndexUid", "crate::types::IndexUid")
        .extern_path(".quickwit.ingest.ShardId", "crate::types::ShardId")
        .file_descriptor_set_path("src/codegen/quickwit/indexing_descriptor.bin");

    Codegen::builder()
        .with_prost_config(prost_config)
        .with_protos(&["protos/quickwit/indexing.proto"])
        .with_includes(&["protos"])
        .with_output_dir("src/codegen/quickwit")
        .with_result_type_path("crate::indexing::IndexingResult")
        .with_error_type_path("crate::indexing::IndexingError")
        .run()
        .unwrap();

    // Metastore service.
    let mut prost_config = prost_build::Config::default();
    prost_config
        .bytes([
            "IndexesMetadataResponse.indexes_metadata_json_zstd",
            "ListIndexesMetadataResponse.indexes_metadata_json_zstd",
        ])
        .extern_path(
            ".quickwit.common.DocMappingUid",
            "crate::types::DocMappingUid",
        )
        .extern_path(".quickwit.common.IndexUid", "crate::types::IndexUid")
        .extern_path(".quickwit.ingest.ShardId", "crate::types::ShardId")
        .field_attribute("DeleteQuery.index_uid", "#[schema(value_type = String)]")
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
        .file_descriptor_set_path("src/codegen/quickwit/metastore_descriptor.bin");

    Codegen::builder()
        .with_prost_config(prost_config)
        .with_protos(&["protos/quickwit/metastore.proto"])
        .with_includes(&["protos"])
        .with_output_dir("src/codegen/quickwit")
        .with_result_type_path("crate::metastore::MetastoreResult")
        .with_error_type_path("crate::metastore::MetastoreError")
        .generate_extra_service_methods()
        .generate_rpc_name_impls()
        .run()
        .unwrap();

    // Ingest service (metastore service proto should be generated before ingest).
    let mut prost_config = prost_build::Config::default();
    prost_config
        .bytes([
            "DocBatchV2.doc_buffer",
            "MRecordBatch.mrecord_buffer",
            "Position.position",
        ])
        .extern_path(
            ".quickwit.common.DocMappingUid",
            "crate::types::DocMappingUid",
        )
        .extern_path(".quickwit.common.DocUid", "crate::types::DocUid")
        .extern_path(".quickwit.common.IndexUid", "crate::types::IndexUid")
        .extern_path(".quickwit.ingest.Position", "crate::types::Position")
        .extern_path(".quickwit.ingest.ShardId", "crate::types::ShardId")
        .field_attribute(
            "Shard.follower_id",
            "#[serde(default, skip_serializing_if = \"Option::is_none\")]",
        )
        .field_attribute(
            "Shard.publish_position_inclusive",
            "#[serde(default, skip_serializing_if = \"Option::is_none\")]",
        )
        .field_attribute(
            "Shard.publish_token",
            "#[serde(default, skip_serializing_if = \"Option::is_none\")]",
        )
        .field_attribute(
            "Shard.replication_position_inclusive",
            "#[serde(default, skip_serializing_if = \"Option::is_none\")]",
        )
        .field_attribute(
            "Shard.update_timestamp",
            "#[serde(default = \"super::compatibility_shard_update_timestamp\")]",
        )
        .file_descriptor_set_path("src/codegen/quickwit/ingest_descriptor.bin");

    Codegen::builder()
        .with_prost_config(prost_config)
        .with_protos(&[
            "protos/quickwit/ingester.proto",
            "protos/quickwit/router.proto",
        ])
        .with_includes(&["protos"])
        .with_output_dir("src/codegen/quickwit")
        .with_result_type_path("crate::ingest::IngestV2Result")
        .with_error_type_path("crate::ingest::IngestV2Error")
        .generate_rpc_name_impls()
        .run()
        .unwrap();

    // Search service.
    let mut prost_config = prost_build::Config::default();
    prost_config
        .file_descriptor_set_path("src/codegen/quickwit/search_descriptor.bin")
        .protoc_arg("--experimental_allow_proto3_optional");

    tonic_prost_build::configure()
        .enum_attribute(".", "#[serde(rename_all=\"snake_case\")]")
        .type_attribute(
            ".",
            "#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]",
        )
        .type_attribute("PartialHit.sort_value", "#[derive(Copy)]")
        .type_attribute("SortByValue", "#[derive(Ord, PartialOrd)]")
        .type_attribute("SearchRequest", "#[derive(Hash, Eq)]")
        .type_attribute("PartialHit", "#[derive(Hash, Eq)]")
        .out_dir("src/codegen/quickwit")
        .compile_with_config(
            prost_config,
            &[std::path::PathBuf::from("protos/quickwit/search.proto")],
            &[std::path::PathBuf::from("protos")],
        )?;

    // Jaeger proto
    let protos = find_protos("protos/third-party/jaeger");

    let mut prost_config = prost_build::Config::default();
    prost_config.type_attribute("Operation", "#[derive(Ord, PartialOrd)]");

    tonic_prost_build::configure()
        .out_dir("src/codegen/jaeger")
        .compile_with_config(
            prost_config,
            &protos,
            &[
                std::path::PathBuf::from("protos/third-party/jaeger"),
                std::path::PathBuf::from("protos/third-party"),
            ],
        )?;

    // OTEL proto
    let mut prost_config = prost_build::Config::default();
    prost_config.protoc_arg("--experimental_allow_proto3_optional");

    let protos = find_protos("protos/third-party/opentelemetry");
    tonic_prost_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("StatusCode", r#"#[serde(rename_all = "snake_case")]"#)
        .type_attribute(
            "ExportLogsServiceResponse",
            r#"#[derive(utoipa::ToSchema)]"#,
        )
        .out_dir("src/codegen/opentelemetry")
        .compile_with_config(
            prost_config,
            &protos,
            &[std::path::PathBuf::from("protos/third-party")],
        )?;
    Ok(())
}

fn find_protos(dir_path: &str) -> Vec<PathBuf> {
    glob(&format!("{dir_path}/**/*.proto"))
        .unwrap()
        .flatten()
        .collect()
}
