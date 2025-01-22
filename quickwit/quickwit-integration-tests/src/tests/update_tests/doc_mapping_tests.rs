// Copyright (C) 2024 Quickwit, Inc.
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

use std::time::Duration;

use quickwit_config::service::QuickwitService;
use serde_json::{json, Value};

use super::assert_hits_unordered;
use crate::test_utils::ClusterSandboxBuilder;

/// Update the doc mapping between 2 calls to local-ingest (forces separate indexing pipelines) and
/// assert the number of hits for the given query
async fn validate_search_across_doc_mapping_updates(
    index_id: &str,
    original_doc_mapping: Value,
    ingest_before_update: &[Value],
    updated_doc_mapping: Value,
    ingest_after_update: &[Value],
    query_and_expect: &[(&str, Result<&[Value], ()>)],
) {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;

    {
        // Wait for indexer to fully start.
        // The starting time is a bit long for a cluster.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let indexing_service_counters = sandbox
            .rest_client(QuickwitService::Indexer)
            .node_stats()
            .indexing()
            .await
            .unwrap();
        assert_eq!(indexing_service_counters.num_running_pipelines, 0);
    }

    // Create index
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(
            json!({
                "version": "0.8",
                "index_id": index_id,
                "doc_mapping": original_doc_mapping,
                "indexing_settings": {
                    "commit_timeout_secs": 1
                },
            })
            .to_string(),
            quickwit_config::ConfigFormat::Json,
            false,
        )
        .await
        .unwrap();

    assert!(sandbox
        .rest_client(QuickwitService::Indexer)
        .node_health()
        .is_live()
        .await
        .unwrap());

    // Wait until indexing pipelines are started.
    sandbox.wait_for_indexing_pipelines(1).await.unwrap();

    // We use local ingest to always pick up the latest doc mapping
    sandbox
        .local_ingest(index_id, ingest_before_update)
        .await
        .unwrap();

    // Update index to also search "body" by default, search should now have 1 hit
    sandbox
        .rest_client(QuickwitService::Searcher)
        .indexes()
        .update(
            index_id,
            json!({
                "version": "0.8",
                "index_id": index_id,
                "doc_mapping": updated_doc_mapping,
                "indexing_settings": {
                    "commit_timeout_secs": 1,
                },
            })
            .to_string(),
            quickwit_config::ConfigFormat::Json,
        )
        .await
        .unwrap();

    sandbox
        .local_ingest(index_id, ingest_after_update)
        .await
        .unwrap();

    for (query, expected_hits) in query_and_expect.iter().copied() {
        assert_hits_unordered(&sandbox, index_id, query, expected_hits).await;
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_update_doc_mapping_text_to_u64() {
    let index_id = "update-text-to-u64";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text"}
        ]
    });
    let ingest_before_update = &[json!({"body": "14"}), json!({"body": "15"})];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "u64"}
        ]
    });
    let ingest_after_update = &[json!({"body": 16}), json!({"body": 17})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            ("body:14", Ok(&[json!({"body": 14})])),
            ("body:16", Ok(&[json!({"body": 16})])),
            // error expected because the validation is performed
            // by latest doc mapping
            ("body:hello", Err(())),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mapping_u64_to_text() {
    let index_id = "update-u64-to-text";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "u64"}
        ],
        "mode": "strict",
    });
    let ingest_before_update = &[json!({"body": 14}), json!({"body": 15})];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text"},
        ],
        "mode": "strict",
    });
    let ingest_after_update = &[json!({"body": "16"}), json!({"body": "hello world"})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            ("body:14", Ok(&[json!({"body": "14"})])),
            ("body:16", Ok(&[json!({"body": "16"})])),
            ("body:hello", Ok(&[json!({"body": "hello world"})])),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mapping_json_to_text() {
    let index_id = "update-json-to-text";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "json"}
        ]
    });
    let ingest_before_update = &[
        json!({"body": {"field1": "hello"}}),
        json!({"body": {"field2": "world"}}),
    ];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text"}
        ]
    });
    let ingest_after_update = &[json!({"body": "hello world"})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            ("body:hello", Ok(&[json!({"body": "hello world"})])),
            // error expected because the validation is performed
            // by latest doc mapping
            ("body.field1:hello", Err(())),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mapping_json_to_object() {
    let index_id = "update-json-to-object";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "json"}
        ]
    });
    let ingest_before_update = &[
        json!({"body": {"field1": "hello"}}),
        json!({"body": {"field2": "world"}}),
    ];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {
                "name": "body",
                "type": "object",
                "field_mappings": [
                    {"name": "field1", "type": "text"},
                    {"name": "field2", "type": "text"},
                ]
            }
        ]
    });
    let ingest_after_update = &[
        json!({"body": {"field1": "hola"}}),
        json!({"body": {"field2": "mundo"}}),
    ];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            (
                "body.field1:hello",
                Ok(&[json!({"body": {"field1": "hello"}})]),
            ),
            (
                "body.field1:hola",
                Ok(&[json!({"body": {"field1": "hola"}})]),
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mapping_object_to_json() {
    let index_id = "update-object-to-json";
    let original_doc_mappings = json!({
        "field_mappings": [
            {
                "name": "body",
                "type": "object",
                "field_mappings": [
                    {"name": "field1", "type": "text"},
                    {"name": "field2", "type": "text"},
                ]
            }
        ]
    });
    let ingest_before_update = &[
        json!({"body": {"field1": "hello"}}),
        json!({"body": {"field2": "world"}}),
    ];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "json"}
        ]
    });
    let ingest_after_update = &[
        json!({"body": {"field1": "hola"}}),
        json!({"body": {"field2": "mundo"}}),
    ];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            (
                "body.field1:hello",
                Ok(&[json!({"body": {"field1": "hello"}})]),
            ),
            (
                "body.field1:hola",
                Ok(&[json!({"body": {"field1": "hola"}})]),
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mapping_tokenizer_default_to_raw() {
    let index_id = "update-tokenizer-default-to-raw";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text", "tokenizer": "default"}
        ]
    });
    let ingest_before_update = &[json!({"body": "hello-world"})];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text", "tokenizer": "raw"}
        ]
    });
    let ingest_after_update = &[json!({"body": "bonjour-monde"})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            ("body:hello", Ok(&[json!({"body": "hello-world"})])),
            ("body:world", Ok(&[json!({"body": "hello-world"})])),
            // phrases queries won't apply to older splits that didn't support them
            ("body:\"hello world\"", Ok(&[])),
            ("body:\"hello-world\"", Ok(&[])),
            ("body:\"hello-worl\"*", Ok(&[])),
            ("body:bonjour", Ok(&[])),
            ("body:monde", Ok(&[])),
            // the raw tokenizer only returns exact matches
            ("body:\"bonjour monde\"", Ok(&[])),
            (
                "body:\"bonjour-monde\"",
                Ok(&[json!({"body": "bonjour-monde"})]),
            ),
            (
                "body:\"bonjour-mond\"*",
                Ok(&[json!({"body": "bonjour-monde"})]),
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mapping_tokenizer_add_position() {
    let index_id = "update-tokenizer-add-position";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text", "tokenizer": "default"}
        ]
    });
    let ingest_before_update = &[json!({"body": "hello-world"})];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text", "tokenizer": "default", "record": "position"}
        ]
    });
    let ingest_after_update = &[json!({"body": "bonjour-monde"})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            ("body:hello", Ok(&[json!({"body": "hello-world"})])),
            ("body:world", Ok(&[json!({"body": "hello-world"})])),
            // phrases queries don't apply to older splits that didn't support them
            ("body:\"hello-world\"", Ok(&[])),
            ("body:\"hello world\"", Ok(&[])),
            ("body:\"hello-worl\"*", Ok(&[])),
            ("body:bonjour", Ok(&[json!({"body": "bonjour-monde"})])),
            ("body:monde", Ok(&[json!({"body": "bonjour-monde"})])),
            (
                "body:\"bonjour-monde\"",
                Ok(&[json!({"body": "bonjour-monde"})]),
            ),
            (
                "body:\"bonjour monde\"",
                Ok(&[json!({"body": "bonjour-monde"})]),
            ),
            (
                "body:\"bonjour-mond\"*",
                Ok(&[json!({"body": "bonjour-monde"})]),
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mapping_tokenizer_raw_to_phrase() {
    let index_id = "update-tokenizer-raw-to-phrase";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text", "tokenizer": "raw"}
        ]
    });
    let ingest_before_update = &[json!({"body": "hello-world"})];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text", "tokenizer": "default", "record": "position"}
        ]
    });
    let ingest_after_update = &[json!({"body": "bonjour-monde"})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            ("body:hello", Ok(&[])),
            ("body:world", Ok(&[])),
            // raw tokenizer used here, only exact matches returned
            (
                "body:\"hello-world\"",
                Ok(&[json!({"body": "hello-world"})]),
            ),
            ("body:\"hello world\"", Ok(&[])),
            ("body:bonjour", Ok(&[json!({"body": "bonjour-monde"})])),
            ("body:monde", Ok(&[json!({"body": "bonjour-monde"})])),
            (
                "body:\"bonjour-monde\"",
                Ok(&[json!({"body": "bonjour-monde"})]),
            ),
            (
                "body:\"bonjour monde\"",
                Ok(&[json!({"body": "bonjour-monde"})]),
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mapping_unindexed_to_indexed() {
    let index_id = "update-not-indexed-to-indexed";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text", "indexed": false}
        ]
    });
    let ingest_before_update = &[json!({"body": "hello"})];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text", "tokenizer": "raw"}
        ]
    });
    let ingest_after_update = &[json!({"body": "bonjour"})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            // term query won't apply to older splits that weren't indexed
            ("body:hello", Ok(&[])),
            ("body:IN [hello]", Ok(&[])),
            // works on newer data
            ("body:bonjour", Ok(&[json!({"body": "bonjour"})])),
            ("body:IN [bonjour]", Ok(&[json!({"body": "bonjour"})])),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mapping_strict_to_dynamic() {
    let index_id = "update-strict-to-dynamic";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text"}
        ],
        "mode": "strict",
    });
    let ingest_before_update = &[json!({"body": "hello"})];
    let updated_doc_mappings = json!({
        "mode": "dynamic",
    });
    let ingest_after_update = &[json!({"body": "world", "title": "salutations"})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            ("body:hello", Ok(&[json!({"body": "hello"})])),
            (
                "body:world",
                Ok(&[json!({"body": "world", "title": "salutations"})]),
            ),
            (
                "title:salutations",
                Ok(&[json!({"body": "world", "title": "salutations"})]),
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mapping_dynamic_to_strict() {
    let index_id = "update-dynamic-to-strict";
    let original_doc_mappings = json!({
        "mode": "dynamic",
    });
    let ingest_before_update = &[json!({"body": "hello"})];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text"}
        ],
        "mode": "strict",
    });
    let ingest_after_update = &[json!({"body": "world"})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            ("body:hello", Ok(&[json!({"body": "hello"})])),
            ("body:world", Ok(&[json!({"body": "world"})])),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_update_doc_mapping_add_field_on_strict() {
    let index_id = "update-add-field-on-strict";
    let original_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text"},
        ],
        "mode": "strict",
    });
    let ingest_before_update = &[json!({"body": "hello"})];
    let updated_doc_mappings = json!({
        "field_mappings": [
            {"name": "body", "type": "text"},
            {"name": "title", "type": "text"},
        ],
        "mode": "strict",
    });
    let ingest_after_update = &[json!({"body": "world", "title": "salutations"})];
    validate_search_across_doc_mapping_updates(
        index_id,
        original_doc_mappings,
        ingest_before_update,
        updated_doc_mappings,
        ingest_after_update,
        &[
            ("body:hello", Ok(&[json!({"body": "hello"})])),
            (
                "body:world",
                Ok(&[json!({"body": "world", "title": "salutations"})]),
            ),
            (
                "title:salutations",
                Ok(&[json!({"body": "world", "title": "salutations"})]),
            ),
        ],
    )
    .await;
}
