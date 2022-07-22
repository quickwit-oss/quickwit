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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use byte_unit::Byte;
use quickwit_common::uri::Uri;
use quickwit_config::{
    DocMapping, IndexingResources, IndexingSettings, KafkaSourceParams, MergePolicy,
    SearchSettings, SourceConfig, SourceParams,
};
use quickwit_doc_mapper::{ModeType, SortOrder};

use crate::checkpoint::{
    CheckpointDelta, IndexCheckpoint, PartitionId, Position, SourceCheckpoint,
};
use crate::IndexMetadata;

pub(crate) fn test_index_metadata_eq(
    index_metadata: &IndexMetadata,
    expected_index_metadata: &IndexMetadata,
) {
    assert_eq!(index_metadata.index_id, expected_index_metadata.index_id);
    assert_eq!(index_metadata.index_uri, expected_index_metadata.index_uri);
    assert_eq!(
        index_metadata.checkpoint,
        expected_index_metadata.checkpoint
    );
    assert_eq!(
        index_metadata
            .doc_mapping
            .field_mappings
            .iter()
            .map(|field_mapping| &field_mapping.name)
            .collect::<Vec<_>>(),
        expected_index_metadata
            .doc_mapping
            .field_mappings
            .iter()
            .map(|field_mapping| &field_mapping.name)
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        index_metadata.doc_mapping.tag_fields,
        expected_index_metadata.doc_mapping.tag_fields,
    );
    assert_eq!(
        index_metadata.doc_mapping.store_source,
        expected_index_metadata.doc_mapping.store_source,
    );
    assert_eq!(
        index_metadata.indexing_settings,
        expected_index_metadata.indexing_settings
    );
    assert_eq!(
        index_metadata.search_settings,
        expected_index_metadata.search_settings
    );
    assert_eq!(index_metadata.sources, expected_index_metadata.sources);
    assert_eq!(
        index_metadata.update_timestamp,
        expected_index_metadata.update_timestamp
    );
    assert_eq!(
        index_metadata.create_timestamp,
        expected_index_metadata.create_timestamp
    );
}

/// Creates a new [`IndexMetadata`] object against which backward compatibility tests will be run.
pub(crate) fn sample_index_metadata_for_regression() -> IndexMetadata {
    let mut source_checkpoint = SourceCheckpoint::default();
    let delta = CheckpointDelta::from_partition_delta(
        PartitionId::from(0i64),
        Position::Beginning,
        Position::from(42u64),
    );
    source_checkpoint.try_apply_delta(delta).unwrap();
    let mut per_source_checkpoint: BTreeMap<String, SourceCheckpoint> = BTreeMap::default();
    per_source_checkpoint.insert("kafka-source".to_string(), source_checkpoint);
    let checkpoint = IndexCheckpoint::from(per_source_checkpoint);
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
        tag_fields: ["tenant_id", "log_level"]
            .into_iter()
            .map(|tag_field| tag_field.to_string())
            .collect::<BTreeSet<String>>(),
        store_source: true,
        mode: ModeType::Dynamic,
        dynamic_mapping: None,
    };
    let merge_policy = MergePolicy {
        demux_factor: 7,
        merge_factor: 9,
        max_merge_factor: 11,
    };
    let indexing_resources = IndexingResources {
        __num_threads_deprecated: serde::de::IgnoredAny,
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
        docstore_blocksize: IndexingSettings::default_docstore_blocksize(),
        docstore_compression_level: IndexingSettings::default_docstore_compression_level(),
    };
    let search_settings = SearchSettings {
        default_search_fields: vec!["message".to_string()],
    };
    let kafka_source = SourceConfig {
        source_id: "kafka-source".to_string(),
        source_params: SourceParams::Kafka(KafkaSourceParams {
            topic: "kafka-topic".to_string(),
            client_log_level: None,
            client_params: serde_json::json!({}),
        }),
    };
    let mut sources = HashMap::default();
    sources.insert("kafka-source".to_string(), kafka_source);

    IndexMetadata {
        index_id: "my-index".to_string(),
        index_uri: Uri::new("s3://quickwit-indexes/my-index".to_string()),
        checkpoint,
        doc_mapping,
        indexing_settings,
        search_settings,
        sources,
        create_timestamp: 1789,
        update_timestamp: 1789,
    }
}

#[test]
fn test_index_metadata_backward_compatibility() -> anyhow::Result<()> {
    let sample_index_metadata = sample_index_metadata_for_regression();
    super::test_json_backward_compatibility_helper(
        "index-metadata",
        test_index_metadata_eq,
        sample_index_metadata,
    )
}
