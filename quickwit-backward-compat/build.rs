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

use byte_unit::Byte;
use quickwit_config::{
    DocMapping, IndexingResources, IndexingSettings, MergePolicy, SearchSettings, SourceConfig,
};
use quickwit_index_config::SortOrder;
use quickwit_metastore::checkpoint::{Checkpoint, CheckpointDelta, PartitionId, Position};
use quickwit_metastore::{IndexMetadata, SplitMetadata};

/// Creates a new [`IndexMetadata`] object against which backward compatibility tests will be run.
fn sample_index_metadata_for_regression() -> IndexMetadata {
    let mut checkpoint = Checkpoint::default();
    let delta = CheckpointDelta::from_partition_delta(
        PartitionId::from(0),
        Position::Beginning,
        Position::from(42),
    );
    checkpoint.try_apply_delta(delta).unwrap();
    let tenant_id_mapping = serde_json::from_str(
        r#"{
                "name": "tenant_id",
                "type": "u64",
                "fast": true
        }"#,
    )
    .unwrap();
    let timestamp_mapping = serde_json::from_str(
        r#"{
                "name": "timestamp",
                "type": "i64",
                "fast": true
        }"#,
    )
    .unwrap();
    let log_level_mapping = serde_json::from_str(
        r#"{
                "name": "log_level",
                "type": "text",
                "tokenizer": "raw"
        }"#,
    )
    .unwrap();
    let message_mapping = serde_json::from_str(
        r#"{
                "name": "message",
                "type": "text",
                "record": "position",
                "tokenizer": "default"
        }"#,
    )
    .unwrap();
    let doc_mapping = DocMapping {
        field_mappings: vec![
            tenant_id_mapping,
            timestamp_mapping,
            log_level_mapping,
            message_mapping,
        ],
        tag_fields: vec!["tenant_id".to_string(), "log_level".to_string()],
        store_source: true,
    };
    let merge_policy = MergePolicy {
        demux_factor: 7,
        merge_factor: 9,
        max_merge_factor: 11,
    };
    let indexing_resources = IndexingResources {
        num_threads: 3,
        heap_size: Byte::from_bytes(3),
    };
    let indexing_settings = IndexingSettings {
        demux_enabled: true,
        demux_field: Some("tenant_id".to_string()),
        timestamp_field: Some("timestamp".to_string()),
        sort_field: Some("timestamp".to_string()),
        sort_order: Some(SortOrder::Asc),
        commit_timeout_secs: 301,
        split_num_docs_target: 10_000_001,
        merge_enabled: true,
        merge_policy,
        resources: indexing_resources,
    };
    let search_settings = SearchSettings {
        default_search_fields: vec!["message".to_string()],
    };
    let kafka_source = SourceConfig {
        source_id: "kafka-source".to_string(),
        source_type: "kafka".to_string(),
        params: serde_json::json!({}),
    };
    let sources = vec![kafka_source];

    IndexMetadata {
        index_id: "my-index".to_string(),
        index_uri: "s3://quickwit-indexes/my-index".to_string(),
        checkpoint,
        doc_mapping,
        indexing_settings,
        search_settings,
        sources,
        create_timestamp: 1789,
    }
}

fn save_index_metadata_test_files() -> anyhow::Result<()> {
    let index_metadata = sample_index_metadata_for_regression();
    let index_metadata_value = serde_json::to_value(&index_metadata)?;
    let version: &str = index_metadata_value
        .as_object()
        .unwrap()
        .get("version")
        .expect("Key `version` is missing from index metadata object.")
        .as_str()
        .expect("Value for key `version` should be a string.");
    let mut index_metadata_json = serde_json::to_string_pretty(&index_metadata_value)?;
    index_metadata_json.push('\n');

    let md5_digest = md5::compute(&index_metadata_json);

    for extension in [".json", ".expected.json"] {
        std::fs::write(
            format!(
                "test-data/index-metadata/v{}-{:x}{}",
                version, md5_digest, extension
            ),
            index_metadata_json.as_bytes(),
        )?;
    }
    Ok(())
}

/// Creates a split metadata object that will be
/// used to check for non-regression
fn sample_split_metadata_for_regression() -> SplitMetadata {
    SplitMetadata {
        split_id: "split".to_string(),
        num_docs: 12303,
        original_size_in_bytes: 234234,
        time_range: Some(121000..=130198),
        create_timestamp: 3,
        tags: ["234".to_string(), "aaa".to_string()].into_iter().collect(),
        demux_num_ops: 1,
        footer_offsets: 1000..2000,
    }
}

fn save_split_metadata_test_files() -> anyhow::Result<()> {
    let split_metadata = sample_split_metadata_for_regression();
    let split_metadata_value = serde_json::to_value(&split_metadata)?;
    let version: &str = split_metadata_value
        .as_object()
        .unwrap()
        .get("version")
        .expect("Missing version")
        .as_str()
        .expect("version should be a string");
    let mut split_metadata_json = serde_json::to_string_pretty(&split_metadata_value)?;
    split_metadata_json.push('\n');
    let md5_digest = md5::compute(&split_metadata_json);
    let test_name = format!("test-data/split-metadata/v{}-{:x}", version, md5_digest);
    let file_regression_test_path = format!("{}.json", test_name);
    std::fs::write(&file_regression_test_path, split_metadata_json.as_bytes())?;
    let file_regression_expected_path = format!("{}.expected.json", test_name);
    std::fs::write(
        &file_regression_expected_path,
        split_metadata_json.as_bytes(),
    )?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    save_index_metadata_test_files()?;
    save_split_metadata_test_files()?;
    Ok(())
}
