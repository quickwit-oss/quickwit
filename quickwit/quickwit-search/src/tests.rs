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

use std::collections::BTreeSet;

use assert_json_diff::{assert_json_eq, assert_json_include};
use quickwit_config::SearcherConfig;
use quickwit_doc_mapper::DefaultDocMapper;
use quickwit_indexing::TestSandbox;
use quickwit_proto::{LeafHit, SearchRequest, SortOrder};
use serde_json::json;

use super::*;
use crate::single_node_search;

#[tokio::test]
async fn test_single_node_simple() -> anyhow::Result<()> {
    let index_id = "single-node-simple-1";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
              - name: body
                type: text
              - name: url
                type: text
              - name: binary
                type: bytes
        "#;
    let test_sandbox =
        TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"], None).await?;
    let docs = vec![
        json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy", "binary": "dGhpcyBpcyBhIHRlc3Qu"}),
        json!({"title": "beagle", "body": "The beagle is a breed of small scent hound, similar in appearance to the much larger foxhound.", "url": "http://beagle", "binary": "bWFkZSB5b3UgbG9vay4="}),
    ];
    test_sandbox.add_documents(docs.clone()).await?;
    let search_request = SearchRequest {
        index_id: index_id.to_string(),
        query: "anthropomorphic".to_string(),
        search_fields: vec!["body".to_string()],
        start_timestamp: None,
        end_timestamp: None,
        max_hits: 2,
        start_offset: 0,
        ..Default::default()
    };
    let single_node_result = single_node_search(
        &search_request,
        &*test_sandbox.metastore(),
        test_sandbox.storage_uri_resolver(),
    )
    .await?;
    assert_eq!(single_node_result.num_hits, 1);
    assert_eq!(single_node_result.hits.len(), 1);
    let hit_json: serde_json::Value = serde_json::from_str(&single_node_result.hits[0].json)?;
    let expected_json: serde_json::Value = json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy", "binary": "dGhpcyBpcyBhIHRlc3Qu"});
    assert_json_include!(actual: hit_json, expected: expected_json);
    assert!(single_node_result.elapsed_time_micros > 10);
    assert!(single_node_result.elapsed_time_micros < 1_000_000);
    Ok(())
}

#[tokio::test]
async fn test_single_search_with_snippet() -> anyhow::Result<()> {
    let index_id = "single-node-with-snippet";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
              - name: body
                type: text
        "#;
    let test_sandbox =
        TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"], None).await?;
    let docs = vec![
        json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle in the comic strip."}),
        json!({"title": "beagle", "body": "The beagle is a breed of small scent hound."}),
        json!({"title": "lisa", "body": "Lisa is a character in `The Simpsons` animated tv series."}),
    ];
    test_sandbox.add_documents(docs.clone()).await?;
    let search_request = SearchRequest {
        index_id: index_id.to_string(),
        query: "beagle".to_string(),
        search_fields: vec!["title".to_string(), "body".to_string()],
        snippet_fields: vec!["title".to_string(), "body".to_string()],
        start_timestamp: None,
        end_timestamp: None,
        max_hits: 2,
        start_offset: 0,
        ..Default::default()
    };
    let single_node_result = single_node_search(
        &search_request,
        &*test_sandbox.metastore(),
        test_sandbox.storage_uri_resolver(),
    )
    .await?;
    assert_eq!(single_node_result.num_hits, 2);
    assert_eq!(single_node_result.hits.len(), 2);

    let highlight_json: serde_json::Value =
        serde_json::from_str(single_node_result.hits[0].snippet.as_ref().unwrap())?;
    let expected_json: serde_json::Value = json!({"title": [], "body": ["Snoopy is an anthropomorphic <b>beagle</b> in the comic strip"]});
    assert_json_eq!(highlight_json, expected_json);

    let highlight_json: serde_json::Value =
        serde_json::from_str(single_node_result.hits[1].snippet.as_ref().unwrap())?;
    let expected_json: serde_json::Value = json!({
        "title": ["<b>beagle</b>"],
        "body": ["The <b>beagle</b> is a breed of small scent hound"]
    });
    assert_json_eq!(highlight_json, expected_json);
    Ok(())
}

async fn slop_search_and_check(
    test_sandbox: &TestSandbox,
    index_id: &str,
    query: &str,
    expected_num_match: u64,
) -> anyhow::Result<()> {
    let search_request = SearchRequest {
        index_id: index_id.to_string(),
        query: query.to_string(),
        search_fields: vec!["body".to_string()],
        start_timestamp: None,
        end_timestamp: None,
        max_hits: 5,
        start_offset: 0,
        ..Default::default()
    };
    let single_node_result = single_node_search(
        &search_request,
        &*test_sandbox.metastore(),
        test_sandbox.storage_uri_resolver(),
    )
    .await?;
    assert_eq!(
        single_node_result.num_hits, expected_num_match,
        "query: {}",
        query
    );
    assert_eq!(
        single_node_result.hits.len(),
        expected_num_match as usize,
        "query: {}",
        query
    );
    Ok(())
}

#[tokio::test]
async fn test_slop_queries() -> anyhow::Result<()> {
    let index_id = "slop-query";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
              - name: body
                type: text
                record: position
        "#;

    let test_sandbox =
        TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"], None).await?;
    let docs = vec![
        json!({"title": "one", "body": "a red bike"}),
        json!({"title": "two", "body": "a small blue bike"}),
        json!({"title": "three", "body": "a small, rusty, and yellow bike"}),
        json!({"title": "four", "body": "fred's small bike"}),
        json!({"title": "five", "body": "a tiny shelter"}),
    ];
    test_sandbox.add_documents(docs.clone()).await?;

    slop_search_and_check(&test_sandbox, index_id, "\"small bird\"~2", 0).await?;
    slop_search_and_check(&test_sandbox, index_id, "\"red bike\"~2", 1).await?;
    slop_search_and_check(&test_sandbox, index_id, "\"small blue bike\"~3", 1).await?;
    slop_search_and_check(&test_sandbox, index_id, "\"small bike\"", 1).await?;
    slop_search_and_check(&test_sandbox, index_id, "\"small bike\"~1", 2).await?;
    slop_search_and_check(&test_sandbox, index_id, "\"small bike\"~2", 2).await?;
    slop_search_and_check(&test_sandbox, index_id, "\"small bike\"~3", 3).await?;
    slop_search_and_check(&test_sandbox, index_id, "\"tiny shelter\"~3", 1).await?;

    Ok(())
}

// TODO remove me once `Iterator::is_sorted_by_key` is stabilized.
fn is_sorted<E, I: Iterator<Item = E>>(mut it: I) -> bool
where E: Ord {
    let mut previous_el = if let Some(first_el) = it.next() {
        first_el
    } else {
        // The empty list is sorted!
        return true;
    };
    for next_el in it {
        if next_el < previous_el {
            return false;
        }
        previous_el = next_el;
    }
    true
}

#[tokio::test]
async fn test_single_node_several_splits() -> anyhow::Result<()> {
    let index_id = "single-node-several-splits";
    let doc_mapping_yaml = r#"
            tag_fields:
              - "owner"
            field_mappings:
              - name: title
                type: text
              - name: body
                type: text
              - name: url
                type: text
              - name: owner
                type: text
                tokenizer: 'raw'
        "#;
    let test_sandbox =
        TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"], None).await?;
    for _ in 0..10u32 {
        test_sandbox.add_documents(vec![
                json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy"}),
                json!({"title": "beagle", "body": "The beagle is a breed of small scent hound, similar in appearance to the much larger foxhound.", "url": "http://beagle"}),
            ]).await?;
    }
    let search_request = SearchRequest {
        index_id: index_id.to_string(),
        query: "beagle".to_string(),
        search_fields: vec![],
        start_timestamp: None,
        end_timestamp: None,
        max_hits: 6,
        start_offset: 0,
        ..Default::default()
    };
    let single_node_result = single_node_search(
        &search_request,
        &*test_sandbox.metastore(),
        test_sandbox.storage_uri_resolver(),
    )
    .await?;
    assert_eq!(single_node_result.num_hits, 20);
    assert_eq!(single_node_result.hits.len(), 6);
    assert!(&single_node_result.hits[0].json.contains("Snoopy"));
    assert!(&single_node_result.hits[1].json.contains("breed"));
    assert!(is_sorted(single_node_result.hits.iter().flat_map(|hit| {
        hit.partial_hit.as_ref().map(partial_hit_sorting_key)
    })));
    assert!(single_node_result.elapsed_time_micros > 10);
    assert!(single_node_result.elapsed_time_micros < 1_000_000);
    Ok(())
}

#[tokio::test]
async fn test_single_node_filtering() -> anyhow::Result<()> {
    let index_id = "single-node-filtering";
    let doc_mapping_yaml = r#"
            tag_fields:
              - owner
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: datetime
                input_formats:
                    - "rfc3339"
                    - "unix_ts_secs"
                fast: true
              - name: owner
                type: text
                tokenizer: raw
        "#;
    let indexing_settings_json = r#"{
            "timestamp_field": "ts",
            "sort_field": "ts",
            "sort_order": "desc"
        }"#;
    let test_sandbox = TestSandbox::create(
        index_id,
        doc_mapping_yaml,
        indexing_settings_json,
        &["body"],
        None,
    )
    .await?;

    let mut docs = vec![];
    for i in 0..30 {
        let body = format!("info @ t:{}", i + 1);
        docs.push(json!({"body": body, "ts": i+1}));
    }
    test_sandbox.add_documents(docs).await?;

    let search_request = SearchRequest {
        index_id: index_id.to_string(),
        query: "info".to_string(),
        search_fields: vec![],
        start_timestamp: Some(10),
        end_timestamp: Some(20),
        max_hits: 15,
        start_offset: 0,
        ..Default::default()
    };
    let single_node_response = single_node_search(
        &search_request,
        &*test_sandbox.metastore(),
        test_sandbox.storage_uri_resolver(),
    )
    .await?;
    assert_eq!(single_node_response.num_hits, 10);
    assert_eq!(single_node_response.hits.len(), 10);
    assert!(&single_node_response.hits[0].json.contains("t:19"));
    assert!(&single_node_response.hits[9].json.contains("t:10"));

    // filter on time range [i64::MIN 20[ should only hit first 19 docs because of filtering
    let search_request = SearchRequest {
        index_id: index_id.to_string(),
        query: "info".to_string(),
        search_fields: vec![],
        start_timestamp: None,
        end_timestamp: Some(20),
        max_hits: 25,
        start_offset: 0,
        ..Default::default()
    };
    let single_node_response = single_node_search(
        &search_request,
        &*test_sandbox.metastore(),
        test_sandbox.storage_uri_resolver(),
    )
    .await?;
    assert_eq!(single_node_response.num_hits, 19);
    assert_eq!(single_node_response.hits.len(), 19);
    assert!(&single_node_response.hits[0].json.contains("t:19"));
    assert!(&single_node_response.hits[18].json.contains("t:1"));

    // filter on tag, should return an error since no split is tagged
    let search_request = SearchRequest {
        index_id: index_id.to_string(),
        query: "tag:foo AND info".to_string(),
        search_fields: vec![],
        start_timestamp: None,
        end_timestamp: None,
        max_hits: 25,
        start_offset: 0,
        ..Default::default()
    };
    let single_node_response = single_node_search(
        &search_request,
        &*test_sandbox.metastore(),
        test_sandbox.storage_uri_resolver(),
    )
    .await;
    assert!(single_node_response.is_err());
    assert_eq!(
        single_node_response.err().map(|err| err.to_string()),
        Some("Invalid query: Field does not exists: 'tag'".to_string())
    );
    Ok(())
}

async fn single_node_search_sort_by_field(
    sort_by_field: &str,
    fieldnorms_enabled: bool,
) -> anyhow::Result<()> {
    let index_id = "single-node-sorting-sort-by-".to_string()
        + sort_by_field
        + "fieldnorms-"
        + &fieldnorms_enabled.to_string();

    let doc_mapping_with_fieldnorms = r#"
            field_mappings:
              - name: description
                type: text
                fieldnorms: true
              - name: ts
                type: i64
                fast: true
              - name: temperature
                type: i64
                fast: true
            "#;

    let doc_mapping_without_fieldnorms = r#"
            field_mappings:
              - name: description
                type: text
              - name: ts
                type: i64
                fast: true
              - name: temperature
                type: i64
                fast: true
            "#;

    let doc_mapping_yaml = if fieldnorms_enabled {
        doc_mapping_with_fieldnorms
    } else {
        doc_mapping_without_fieldnorms
    };

    let indexing_settings_json = r#"{
            "timestamp_field": "ts",
            "sort_field": "ts",
            "sort_order": "desc"
        }"#;
    let test_sandbox = TestSandbox::create(
        &index_id,
        doc_mapping_yaml,
        indexing_settings_json,
        &["description"],
        None,
    )
    .await?;

    let mut docs = vec![];
    for i in 0..30 {
        let description = format!("city info-{}", i + 1);
        docs.push(json!({"description": description, "ts": i+1, "temperature": i+32}));
    }
    test_sandbox.add_documents(docs).await?;

    let search_request = SearchRequest {
        index_id: index_id.to_string(),
        query: "city".to_string(),
        search_fields: vec![],
        start_timestamp: None,
        end_timestamp: None,
        max_hits: 15,
        start_offset: 0,
        sort_by_field: Some(sort_by_field.to_string()),
        sort_order: Some(SortOrder::Desc as i32),
        ..Default::default()
    };
    let single_node_response = single_node_search(
        &search_request,
        &*test_sandbox.metastore(),
        test_sandbox.storage_uri_resolver(),
    )
    .await?;
    assert_eq!(single_node_response.num_hits, 30);
    assert_eq!(single_node_response.hits.len(), 15);
    assert!(single_node_response.hits.windows(2).all(|hits| hits[0]
        .partial_hit
        .as_ref()
        .unwrap()
        .sorting_field_value
        >= hits[1].partial_hit.as_ref().unwrap().sorting_field_value));
    Ok(())
}

#[tokio::test]
async fn test_single_node_sorting_with_query() -> anyhow::Result<()> {
    single_node_search_sort_by_field("temperature", false).await?;
    single_node_search_sort_by_field("_score", true).await?;
    Ok(())
}

#[tokio::test]
async fn test_single_node_sort_by_score_should_fail() -> anyhow::Result<()> {
    let search_response = single_node_search_sort_by_field("_score", false).await;
    assert!(search_response.is_err());
    assert_eq!(
        search_response.err().map(|err| err.to_string()),
        Some(
            "Invalid query: Fieldnorms for field `description` is missing. Fieldnorms must be \
             stored for the field to compute the BM25 score of the documents."
                .to_string()
        )
    );
    Ok(())
}

#[tokio::test]
async fn test_single_node_invalid_sorting_with_query() -> anyhow::Result<()> {
    let index_id = "single-node-invalid-sorting";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: description
                type: text
                fast: true
              - name: temperature
                type: i64
        "#;
    let indexing_settings_json = r#"{
        }"#;
    let test_sandbox = TestSandbox::create(
        index_id,
        doc_mapping_yaml,
        indexing_settings_json,
        &["description"],
        None,
    )
    .await?;

    let mut docs = vec![];
    for i in 0..30 {
        let description = format!("city info-{}", i + 1);
        docs.push(json!({"description": description, "ts": i+1, "temperature": i+32}));
    }
    test_sandbox.add_documents(docs).await?;

    let search_request = SearchRequest {
        index_id: index_id.to_string(),
        query: "city".to_string(),
        search_fields: vec![],
        start_timestamp: None,
        end_timestamp: None,
        max_hits: 15,
        start_offset: 0,
        sort_by_field: Some("description".to_string()),
        sort_order: Some(SortOrder::Desc as i32),
        ..Default::default()
    };
    let single_node_response = single_node_search(
        &search_request,
        &*test_sandbox.metastore(),
        test_sandbox.storage_uri_resolver(),
    )
    .await;
    assert!(single_node_response.is_err());
    assert_eq!(
        single_node_response.err().map(|err| err.to_string()),
        Some(
            "Invalid query: Sort by field on type text is currently not supported `description`."
                .to_string()
        )
    );
    Ok(())
}

#[tokio::test]
async fn test_single_node_split_pruning_by_tags() -> anyhow::Result<()> {
    let doc_mapping_yaml = r#"
            tag_fields:
              - owner
            field_mappings:
              - name: owner
                type: text
                tokenizer: raw
        "#;
    let index_id = "single-node-pruning-by-tags";
    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &[], None).await?;
    let owners = ["paul", "adrien"];
    for owner in owners {
        let mut docs = vec![];
        for i in 0..10 {
            docs.push(json!({"body": format!("content num #{}", i + 1), "owner": owner}));
        }
        test_sandbox.add_documents(docs).await?;
    }

    let selected_splits = list_relevant_splits(
        &SearchRequest {
            index_id: index_id.to_string(),
            query: "owner:francois".to_string(),
            ..Default::default()
        },
        &*test_sandbox.metastore(),
    )
    .await?;
    assert!(selected_splits.is_empty());

    let selected_splits = list_relevant_splits(
        &SearchRequest {
            index_id: index_id.to_string(),
            query: "".to_string(),
            ..Default::default()
        },
        &*test_sandbox.metastore(),
    )
    .await?;
    assert_eq!(selected_splits.len(), 2);

    let selected_splits = list_relevant_splits(
        &SearchRequest {
            index_id: index_id.to_string(),
            query: "owner:francois OR owner:paul OR owner:adrien".to_string(),
            ..Default::default()
        },
        &*test_sandbox.metastore(),
    )
    .await?;
    assert_eq!(selected_splits.len(), 2);

    let split_tags: BTreeSet<String> = selected_splits
        .iter()
        .flat_map(|split| split.tags.clone())
        .collect();
    assert_eq!(
        split_tags
            .iter()
            .map(|tag| tag.as_str())
            .collect::<Vec<&str>>(),
        vec!["owner!", "owner:adrien", "owner:paul"]
    );

    Ok(())
}

const DYNAMIC_TEST_INDEX_ID: &str = "search_dynamic_mode";

async fn test_search_dynamic_util(test_sandbox: &TestSandbox, query: &str) -> Vec<u32> {
    let splits = test_sandbox
        .metastore()
        .list_all_splits(DYNAMIC_TEST_INDEX_ID)
        .await
        .unwrap();
    let splits_offsets: Vec<_> = splits
        .into_iter()
        .map(|split_meta| SplitIdAndFooterOffsets {
            split_id: split_meta.split_id().to_string(),
            split_footer_start: split_meta.split_metadata.footer_offsets.start,
            split_footer_end: split_meta.split_metadata.footer_offsets.end,
        })
        .collect();
    let request = quickwit_proto::SearchRequest {
        index_id: DYNAMIC_TEST_INDEX_ID.to_string(),
        query: query.to_string(),
        max_hits: 100,
        ..Default::default()
    };
    let searcher_context = Arc::new(SearcherContext::new(SearcherConfig::default()));
    let search_response = leaf_search(
        searcher_context,
        &request,
        test_sandbox.storage(),
        &splits_offsets,
        test_sandbox.doc_mapper(),
    )
    .await
    .unwrap();
    search_response
        .partial_hits
        .into_iter()
        .map(|partial_hit| partial_hit.doc_id)
        .collect::<Vec<u32>>()
}

#[tokio::test]
async fn test_search_dynamic_mode() -> anyhow::Result<()> {
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
                tokenizer: default
                indexed: true
            mode: dynamic
            dynamic_mapping:
                tokenizer: raw
        "#;
    let test_sandbox =
        TestSandbox::create(DYNAMIC_TEST_INDEX_ID, doc_mapping_yaml, "{}", &[], None)
            .await
            .unwrap();
    let docs = vec![
        json!({"body": "hello happy tax payer"}),
        json!({"body": "hello"}),
        json!({"body_dynamic": "hello happy tax payer"}),
        json!({"body_dynamic": "hello"}),
    ];
    test_sandbox.add_documents(docs).await.unwrap();
    {
        let docs = test_search_dynamic_util(&test_sandbox, "body:hello").await;
        assert_eq!(&docs[..], &[0u32, 1u32]);
    }
    {
        let docs = test_search_dynamic_util(&test_sandbox, "body_dynamic:hello").await;
        assert_eq!(&docs[..], &[3u32]); // 1 is not matched due to the raw tokenizer
    }
    Ok(())
}

#[track_caller]
fn test_convert_leaf_hit_aux(
    default_doc_mapper_json: serde_json::Value,
    leaf_hit_json: serde_json::Value,
    expected_hit_json: serde_json::Value,
) {
    let default_doc_mapper: DefaultDocMapper =
        serde_json::from_value(default_doc_mapper_json).unwrap();
    let hit = convert_leaf_hit(
        LeafHit {
            leaf_json: serde_json::to_string(&leaf_hit_json).unwrap(),
            ..Default::default()
        },
        &default_doc_mapper,
    )
    .unwrap();
    let hit_json: serde_json::Value = serde_json::from_str(&hit.json).unwrap();
    assert_eq!(hit_json, expected_hit_json);
}

#[test]
fn test_convert_leaf_hit_multiple_cardinality() {
    test_convert_leaf_hit_aux(
        json!({
            "field_mappings": [
                { "name": "body", "type": "array<text>" }
            ],
            "mode": "lenient"
        }),
        json!({ "body": ["hello", "happy"] }),
        json!({ "body": ["hello", "happy"] }),
    );
}

#[test]
fn test_convert_leaf_hit_simple_cardinality() {
    test_convert_leaf_hit_aux(
        json!({
            "field_mappings": [
                { "name": "body", "type": "text" }
            ],
            "mode": "lenient"
        }),
        json!({ "body": ["hello", "happy"] }),
        json!({ "body": "hello" }),
    );
}

#[test]
fn test_convert_dynamic() {
    test_convert_leaf_hit_aux(
        json!({
            "field_mappings": [
                { "name": "body", "type": "text" }
            ],
            "mode": "dynamic"
        }),
        json!({ "body": ["hello", "happy"], "_dynamic": [{"title": "hello"}] }),
        json!({ "body": "hello", "title": "hello" }),
    );
}

#[test]
fn test_convert_leaf_object() {
    test_convert_leaf_hit_aux(
        json!({
            "field_mappings": [
                {
                    "name": "user",
                    "type": "object",
                    "field_mappings": [
                        {"name": "username", "type": "text"},
                        {"name": "email", "type": "text"}
                    ]
                }
            ],
            "mode": "lenient"
        }),
        json!({ "user.username": ["fulmicoton"], "user.email": ["werwe33@quickwit.io"]}),
        json!({ "user": {"username": "fulmicoton", "email": "werwe33@quickwit.io"}}),
    );
}

#[test]
fn test_convert_leaf_object_used_to_be_dynamic() {
    test_convert_leaf_hit_aux(
        json!({
            "field_mappings": [
                {
                    "name": "user",
                    "type": "object",
                    "field_mappings": [
                        {"name": "username", "type": "text"},
                    ]
                }
            ],
            "mode": "dynamic"
        }),
        json!({ "_dynamic": [{ "user": {"username": "fulmicoton", "email": "werwe33@quickwit.io"}}]}),
        json!({ "user": {"username": "fulmicoton", "email": "werwe33@quickwit.io"}}),
    );
    test_convert_leaf_hit_aux(
        json!({
            "field_mappings": [
                {
                    "name": "user",
                    "type": "object",
                    "field_mappings": [
                        {"name": "username", "type": "text"},
                    ]
                }
            ],
            "mode": "dynamic"
        }),
        json!({ "_dynamic": [{ "user": {"email": "werwe33@quickwit.io"}}], "user.username": ["fulmicoton"] }),
        json!({ "user": {"username": "fulmicoton", "email": "werwe33@quickwit.io"}}),
    );
}

// This spec might change in the future. THe mode has no impact on the
// output of convert_leaf_doc. In particular, it does not ignore the previously gathered
// dynamic field.
#[test]
fn test_convert_leaf_object_arguable_mode_does_not_affect_format() {
    test_convert_leaf_hit_aux(
        json!({ "mode": "strict" }),
        json!({ "_dynamic": [{ "user": {"username": "fulmicoton", "email": "werwe33@quickwit.io"}}]}),
        json!({ "user": {"username": "fulmicoton", "email": "werwe33@quickwit.io"}}),
    );
}

#[test]
fn test_convert_leaf_hit_with_source() {
    test_convert_leaf_hit_aux(
        json!({
            "field_mappings": [ {"name": "username", "type": "text"} ],
            "mode": "strict"
        }),
        json!({ "_source": [{"username": "fulmicoton"}], "username": ["fulmicoton"] }),
        json!({ "username": "fulmicoton", "_source": {"username": "fulmicoton"}}),
    );
}

#[tokio::test]
async fn test_single_node_aggregation() -> anyhow::Result<()> {
    let index_id = "single-node-agg-1";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: color
                type: text
                fast: true
              - name: price
                type: f64
                fast: true
        "#;
    let test_sandbox =
        TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["color"], None).await?;
    let docs = vec![
        json!({"color": "blue", "price": 10.0}),
        json!({"color": "blue", "price": 15.0}),
        json!({"color": "green", "price": 10.0}),
        json!({"color": "white", "price": 100.0}),
        json!({"color": "white", "price": 1.0}),
    ];
    let agg_req = r#"
 {
   "expensive_colors": {
     "terms": {
       "field": "color",
       "order": {
            "price_stats.max": "desc"
       }
     },
     "aggs": {
       "price_stats" : {
          "stats": {
              "field": "price"
          }
       }
     }
   }
 }"#;

    test_sandbox.add_documents(docs.clone()).await?;
    let search_request = SearchRequest {
        index_id: index_id.to_string(),
        query: "*".to_string(),
        search_fields: vec!["color".to_string()],
        max_hits: 2,
        start_offset: 0,
        aggregation_request: Some(agg_req.to_string()),
        ..Default::default()
    };
    let single_node_result = single_node_search(
        &search_request,
        &*test_sandbox.metastore(),
        test_sandbox.storage_uri_resolver(),
    )
    .await?;
    let agg_res_json: serde_json::Value =
        serde_json::from_str(&single_node_result.aggregation.unwrap())?;
    assert_eq!(
        agg_res_json["expensive_colors"]["buckets"][0]["key"],
        "white"
    );
    assert_eq!(
        agg_res_json["expensive_colors"]["buckets"][1]["key"],
        "blue"
    );
    assert_eq!(
        agg_res_json["expensive_colors"]["buckets"][2]["key"],
        "green"
    );
    assert!(single_node_result.elapsed_time_micros > 10);
    assert!(single_node_result.elapsed_time_micros < 1_000_000);
    Ok(())
}

#[tokio::test]
async fn test_single_node_aggregation_missing_fast_field() -> anyhow::Result<()> {
    let index_id = "single-node-agg-2";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: color
                type: text
              - name: price
                type: f64
                fast: true
        "#;
    let test_sandbox =
        TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["color"], None).await?;
    let docs = vec![
        json!({"color": "blue", "price": 10.0}),
        json!({"color": "blue", "price": 15.0}),
        json!({"color": "green", "price": 10.0}),
        json!({"color": "white", "price": 100.0}),
        json!({"color": "white", "price": 1.0}),
    ];
    let agg_req = r#"
 {
   "expensive_colors": {
     "terms": {
       "field": "color",
       "order": {
            "price_stats.max": "desc"
       }
     },
     "aggs": {
       "price_stats" : {
          "stats": {
              "field": "price"
          }
       }
     }
   }
 }"#;

    test_sandbox.add_documents(docs.clone()).await?;
    let search_request = SearchRequest {
        index_id: index_id.to_string(),
        query: "*".to_string(),
        search_fields: vec!["color".to_string()],
        max_hits: 2,
        start_offset: 0,
        aggregation_request: Some(agg_req.to_string()),
        ..Default::default()
    };
    let single_node_result = single_node_search(
        &search_request,
        &*test_sandbox.metastore(),
        test_sandbox.storage_uri_resolver(),
    )
    .await?;
    assert_eq!(single_node_result.num_hits, 0);
    assert_eq!(single_node_result.errors.len(), 1);
    assert!(single_node_result.errors[0].contains("color"));
    assert!(single_node_result.errors[0].contains("is not a fast field"));

    Ok(())
}
