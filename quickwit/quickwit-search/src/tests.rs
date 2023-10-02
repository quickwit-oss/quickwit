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

use std::collections::{BTreeMap, BTreeSet};

use assert_json_diff::{assert_json_eq, assert_json_include};
use quickwit_config::SearcherConfig;
use quickwit_doc_mapper::tag_pruning::extract_tags_from_query;
use quickwit_doc_mapper::DefaultDocMapper;
use quickwit_indexing::TestSandbox;
use quickwit_opentelemetry::otlp::TraceId;
use quickwit_proto::search::{
    LeafListTermsResponse, ListTermsRequest, SearchRequest, SortByValue, SortField, SortOrder,
    SortValue,
};
use quickwit_query::query_ast::{
    qast_helper, qast_json_helper, query_ast_from_user_text, QueryAst,
};
use serde_json::{json, Value as JsonValue};
use tantivy::schema::Value as TantivyValue;
use tantivy::time::OffsetDateTime;
use tantivy::Term;

use super::*;
use crate::find_trace_ids_collector::Span;
use crate::service::SearcherContext;
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
    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"]).await?;
    let docs = vec![
        json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy", "binary": "dGhpcyBpcyBhIHRlc3Qu"}),
        json!({"title": "beagle", "body": "The beagle is a breed of small scent hound, similar in appearance to the much larger foxhound.", "url": "http://beagle", "binary": "bWFkZSB5b3UgbG9vay4="}),
    ];
    test_sandbox.add_documents(docs.clone()).await?;
    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: qast_json_helper("anthropomorphic", &["body"]),
        max_hits: 2,
        ..Default::default()
    };
    let single_node_result = single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await?;
    assert_eq!(single_node_result.num_hits, 1);
    assert_eq!(single_node_result.hits.len(), 1);
    let hit_json: JsonValue = serde_json::from_str(&single_node_result.hits[0].json)?;
    let expected_json: JsonValue = json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy", "binary": "dGhpcyBpcyBhIHRlc3Qu"});
    assert_json_include!(actual: hit_json, expected: expected_json);
    assert!(single_node_result.elapsed_time_micros > 10);
    assert!(single_node_result.elapsed_time_micros < 1_000_000);
    test_sandbox.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_single_node_termset() -> anyhow::Result<()> {
    let index_id = "single-node-termset-1";
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
    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"]).await?;
    let docs = vec![
        json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy", "binary": "dGhpcyBpcyBhIHRlc3Qu"}),
        json!({"title": "beagle", "body": "The beagle is a breed of small scent hound, similar in appearance to the much larger foxhound.", "url": "http://beagle", "binary": "bWFkZSB5b3UgbG9vay4="}),
    ];
    test_sandbox.add_documents(docs.clone()).await?;
    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: qast_json_helper("title: IN [beagle]", &[]),
        start_timestamp: None,
        end_timestamp: None,
        max_hits: 2,
        start_offset: 0,
        ..Default::default()
    };
    let single_node_result = single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await?;
    assert_eq!(single_node_result.num_hits, 1);
    assert_eq!(single_node_result.hits.len(), 1);
    let hit_json: JsonValue = serde_json::from_str(&single_node_result.hits[0].json)?;
    let expected_json: JsonValue = json!({"title": "beagle", "body": "The beagle is a breed of small scent hound, similar in appearance to the much larger foxhound.", "url": "http://beagle", "binary": "bWFkZSB5b3UgbG9vay4="});
    assert_json_include!(actual: hit_json, expected: expected_json);
    assert!(single_node_result.elapsed_time_micros > 10);
    assert!(single_node_result.elapsed_time_micros < 1_000_000);
    test_sandbox.assert_quit().await;
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
    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"]).await?;
    let docs = vec![
        json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle in the comic strip."}),
        json!({"title": "beagle", "body": "The beagle is a breed of small scent hound."}),
        json!({"title": "lisa", "body": "Lisa is a character in `The Simpsons` animated tv series."}),
    ];
    test_sandbox.add_documents(docs.clone()).await?;
    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: qast_json_helper("beagle", &["title", "body"]),
        snippet_fields: vec!["title".to_string(), "body".to_string()],
        max_hits: 2,
        ..Default::default()
    };
    let single_node_result = single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await?;
    assert_eq!(single_node_result.num_hits, 2);
    assert_eq!(single_node_result.hits.len(), 2);

    let highlight_json: JsonValue =
        serde_json::from_str(single_node_result.hits[0].snippet.as_ref().unwrap())?;
    let expected_json: JsonValue = json!({
        "title": ["<b>beagle</b>"],
        "body": ["The <b>beagle</b> is a breed of small scent hound"]
    });

    assert_json_eq!(highlight_json, expected_json);
    let highlight_json: JsonValue =
        serde_json::from_str(single_node_result.hits[1].snippet.as_ref().unwrap())?;
    let expected_json: JsonValue = json!({"title": [], "body": ["Snoopy is an anthropomorphic <b>beagle</b> in the comic strip"]});
    assert_json_eq!(highlight_json, expected_json);

    test_sandbox.assert_quit().await;
    Ok(())
}

async fn slop_search_and_check(
    test_sandbox: &TestSandbox,
    index_id: &str,
    query: &str,
    expected_num_match: u64,
) -> anyhow::Result<()> {
    let query_ast = qast_json_helper(query, &["body"]);
    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast,
        max_hits: 5,
        ..Default::default()
    };
    let single_node_result = single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await?;
    assert_eq!(
        single_node_result.num_hits, expected_num_match,
        "query: {query}"
    );
    assert_eq!(
        single_node_result.hits.len(),
        expected_num_match as usize,
        "query: {query}"
    );
    Ok(())
}

#[tokio::test]
async fn test_slop_queries() {
    let index_id = "slop-query";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
              - name: body
                type: text
                record: position
        "#;

    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"])
        .await
        .unwrap();
    let docs = vec![
        json!({"title": "one", "body": "a red bike"}),
        json!({"title": "two", "body": "a small blue bike"}),
        json!({"title": "three", "body": "a small, rusty, and yellow bike"}),
        json!({"title": "four", "body": "fred's small bike"}),
        json!({"title": "five", "body": "a tiny shelter"}),
    ];
    test_sandbox.add_documents(docs.clone()).await.unwrap();

    slop_search_and_check(&test_sandbox, index_id, "\"small bird\"~2", 0)
        .await
        .unwrap();
    slop_search_and_check(&test_sandbox, index_id, "\"red bike\"~2", 1)
        .await
        .unwrap();
    slop_search_and_check(&test_sandbox, index_id, "\"small blue bike\"~3", 1)
        .await
        .unwrap();
    slop_search_and_check(&test_sandbox, index_id, "\"small bike\"", 1)
        .await
        .unwrap();
    slop_search_and_check(&test_sandbox, index_id, "\"small bike\"~1", 2)
        .await
        .unwrap();
    slop_search_and_check(&test_sandbox, index_id, "\"small bike\"~2", 2)
        .await
        .unwrap();
    slop_search_and_check(&test_sandbox, index_id, "\"small bike\"~3", 3)
        .await
        .unwrap();
    slop_search_and_check(&test_sandbox, index_id, "\"tiny shelter\"~3", 1)
        .await
        .unwrap();
    test_sandbox.assert_quit().await;
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
#[cfg_attr(not(feature = "ci-test"), ignore)]
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
    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"]).await?;
    for _ in 0..10u32 {
        test_sandbox.add_documents(vec![
                json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy"}),
                json!({"title": "beagle", "body": "The beagle is a breed of small scent hound, similar in appearance to the much larger foxhound.", "url": "http://beagle"}),
            ]).await?;
    }
    let query_ast = query_ast_from_user_text("beagle", None);
    let query_ast_json = serde_json::to_string(&query_ast).unwrap();
    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: query_ast_json,
        max_hits: 6,
        ..Default::default()
    };
    let single_node_result = single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await?;
    assert_eq!(single_node_result.num_hits, 20);
    assert_eq!(single_node_result.hits.len(), 6);
    assert!(&single_node_result.hits[0].json.contains("Snoopy"));
    assert!(&single_node_result.hits[1].json.contains("breed"));
    assert!(is_sorted(single_node_result.hits.iter().flat_map(|hit| {
        hit.partial_hit.as_ref().map(|partial_hit| {
            (
                partial_hit.sort_value,
                partial_hit.split_id.as_str(),
                partial_hit.doc_id,
            )
        })
    })));
    assert!(single_node_result.elapsed_time_micros > 10);
    assert!(single_node_result.elapsed_time_micros < 1_000_000);
    test_sandbox.assert_quit().await;
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
                    - "unix_timestamp"
                fast: true
              - name: owner
                type: text
                tokenizer: raw
            timestamp_field: ts
            mode: lenient
        "#;
    let indexing_settings_json = r#"{}"#;
    let test_sandbox = TestSandbox::create(
        index_id,
        doc_mapping_yaml,
        indexing_settings_json,
        &["body"],
    )
    .await?;

    let mut docs = Vec::new();
    let start_timestamp = OffsetDateTime::now_utc().unix_timestamp();
    for i in 0..30 {
        let body = format!("info @ t:{}", i + 1);
        docs.push(json!({"body": body, "ts": start_timestamp + i + 1}));
    }
    test_sandbox.add_documents(docs).await?;

    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: qast_json_helper("info", &["body"]),
        start_timestamp: Some(start_timestamp + 10),
        end_timestamp: Some(start_timestamp + 20),
        max_hits: 15,
        sort_fields: vec![SortField {
            field_name: "ts".to_string(),
            sort_order: SortOrder::Desc as i32,
        }],
        ..Default::default()
    };
    let single_node_response = single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await?;
    assert_eq!(single_node_response.num_hits, 10);
    assert_eq!(single_node_response.hits.len(), 10);
    assert!(&single_node_response.hits[0].json.contains("t:19"));
    assert!(&single_node_response.hits[9].json.contains("t:10"));

    // filter on time range [i64::MIN 20[ should only hit first 19 docs because of filtering
    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: qast_json_helper("info", &["body"]),
        end_timestamp: Some(start_timestamp + 20),
        max_hits: 25,
        sort_fields: vec![SortField {
            field_name: "ts".to_string(),
            sort_order: SortOrder::Desc as i32,
        }],
        ..Default::default()
    };
    let single_node_response = single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await?;
    assert_eq!(single_node_response.num_hits, 19);
    assert_eq!(single_node_response.hits.len(), 19);
    assert!(&single_node_response.hits[0].json.contains("t:19"));
    assert!(&single_node_response.hits[18].json.contains("t:1"));

    // filter on tag, should return an error since no split is tagged
    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: qast_json_helper("tag:foo AND info", &["body"]),
        max_hits: 25,
        sort_fields: vec![SortField {
            field_name: "ts".to_string(),
            sort_order: SortOrder::Desc as i32,
        }],
        ..Default::default()
    };
    let single_node_response = single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await;
    assert!(single_node_response.is_err());
    assert_eq!(
        single_node_response.err().map(|err| err.to_string()),
        Some("invalid query: field does not exist: `tag`".to_string())
    );
    test_sandbox.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_single_node_without_timestamp_with_query_start_timestamp_enabled(
) -> anyhow::Result<()> {
    let index_id = "single-node-no-timestamp";
    let doc_mapping_yaml = r#"
            tag_fields:
              - owner
            field_mappings:
              - name: body
                type: text
              - name: owner
                type: text
                tokenizer: raw
        "#;
    let indexing_settings_json = r#"{}"#;
    let test_sandbox = TestSandbox::create(
        index_id,
        doc_mapping_yaml,
        indexing_settings_json,
        &["body"],
    )
    .await?;

    let mut docs = Vec::new();
    let start_timestamp = OffsetDateTime::now_utc().unix_timestamp();
    for i in 0..30 {
        let body = format!("info @ t:{}", i + 1);
        docs.push(json!({"body": body}));
    }
    test_sandbox.add_documents(docs).await?;

    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: qast_json_helper("info", &["body"]),
        start_timestamp: Some(start_timestamp + 10),
        end_timestamp: Some(start_timestamp + 20),
        max_hits: 15,
        ..Default::default()
    };
    let single_node_response = single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await;

    assert!(single_node_response.is_err());
    assert_eq!(
        single_node_response.err().map(|err| err.to_string()),
        Some(
            "the timestamp field is not set in index: [\"single-node-no-timestamp\"] definition \
             but start-timestamp or end-timestamp are set in the query"
                .to_string()
        )
    );
    test_sandbox.assert_quit().await;
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
                type: datetime
                fast: true
              - name: temperature
                type: i64
                fast: true
            timestamp_field: ts
            "#;

    let doc_mapping_without_fieldnorms = r#"
            field_mappings:
              - name: description
                type: text
              - name: ts
                type: datetime
                fast: true
              - name: temperature
                type: i64
                fast: true
            timestamp_field: ts
            "#;

    let doc_mapping_yaml = if fieldnorms_enabled {
        doc_mapping_with_fieldnorms
    } else {
        doc_mapping_without_fieldnorms
    };

    let indexing_settings_json = r#"{}"#;
    let test_sandbox = TestSandbox::create(
        &index_id,
        doc_mapping_yaml,
        indexing_settings_json,
        &["description"],
    )
    .await?;

    let mut docs = Vec::new();
    let start_timestamp = 72057595;
    for i in 0..30 {
        let timestamp = start_timestamp + (i + 1) as i64;
        let description = format!("city info-{timestamp}");
        docs.push(json!({"description": description, "ts": timestamp, "temperature": i+32}));
    }
    test_sandbox.add_documents(docs).await?;

    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: qast_json_helper("city", &["description"]),
        max_hits: 15,
        sort_fields: vec![SortField {
            field_name: sort_by_field.to_string(),
            sort_order: SortOrder::Desc as i32,
        }],
        ..Default::default()
    };

    match single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await
    {
        Ok(single_node_response) => {
            assert_eq!(single_node_response.num_hits, 30);
            assert_eq!(single_node_response.hits.len(), 15);
            assert!(single_node_response.hits.windows(2).all(|hits| hits[0]
                .partial_hit
                .as_ref()
                .unwrap()
                .sort_value
                >= hits[1].partial_hit.as_ref().unwrap().sort_value));
            test_sandbox.assert_quit().await;
            Ok(())
        }
        Err(err) => {
            test_sandbox.assert_quit().await;
            Err(err).map_err(anyhow::Error::from)
        }
    }
}

#[tokio::test]
async fn test_single_node_sorting_with_query_fieldnorms_enabled() -> anyhow::Result<()> {
    single_node_search_sort_by_field("_score", true).await
}

#[tokio::test]
async fn test_single_node_sorting_with_query_fieldnorms_disabled() -> anyhow::Result<()> {
    single_node_search_sort_by_field("temperature", false).await
}

#[tokio::test]
async fn test_sort_bm25() {
    let index_id = "sort_by_bm25".to_string();
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
                record: freq
                fieldnorms: true
              - name: body
                type: text
                record: freq
                fieldnorms: true
              - name: nofreq
                type: text
                record: basic
                fieldnorms: true
              - name: nofreq_nofieldnorms
                type: text
                fieldnorms: false
            "#;
    let default_search_fields = &["title", "body", "nofreq", "nofreq_nofieldnorms"];
    let test_sandbox = TestSandbox::create(
        &index_id,
        doc_mapping_yaml,
        "{}",
        &default_search_fields[..],
    )
    .await
    .unwrap();
    let docs = vec![
        json!({"title": "one pad", "nofreq": "two pad"}), // 0
        json!({"title": "one", "nofreq": "two"}),         // 1
        json!({"title": "one one", "nofreq": "two two"}), // 2
    ];
    test_sandbox.add_documents(docs).await.unwrap();
    let search_hits = |query: &str| {
        let query_ast_json = serde_json::to_string(&query_ast_from_user_text(query, None)).unwrap();
        let search_request = SearchRequest {
            index_id_patterns: vec![index_id.to_string()],
            query_ast: query_ast_json,
            max_hits: 1_000,
            sort_fields: vec![SortField {
                field_name: "_score".to_string(),
                sort_order: SortOrder::Desc as i32,
            }],
            ..Default::default()
        };
        let metastore = test_sandbox.metastore();
        let storage_resolver = test_sandbox.storage_resolver();
        async move {
            single_node_search(search_request, metastore, storage_resolver)
                .await
                .unwrap()
                .hits
                .into_iter()
                .map(|hit| {
                    let partial_hit = hit.partial_hit.unwrap();
                    let Some(SortByValue {
                        sort_value: Some(SortValue::F64(score)),
                    }) = partial_hit.sort_value
                    else {
                        panic!()
                    };
                    (score as f32, partial_hit.doc_id)
                })
                .collect()
        }
    };
    {
        let hits: Vec<(f32, u32)> = search_hits("title:one").await;
        assert_eq!(
            &hits[..],
            &[(0.1738279, 2), (0.15965714, 1), (0.12343242, 0)]
        );
    }
    {
        let hits: Vec<(f32, u32)> = search_hits("nofreq:two").await;
        assert_eq!(
            &hits[..],
            &[(0.15965714, 1), (0.12343242, 2), (0.12343242, 0)]
        );
    }
    {
        let hits: Vec<(f32, u32)> = search_hits("title:one nofreq:two").await;
        assert_eq!(
            &hits[..],
            &[(0.31931427, 1), (0.2972603, 2), (0.24686484, 0)]
        );
    }
    test_sandbox.assert_quit().await;
}

#[tokio::test]
async fn test_sort_by_static_and_dynamic_field() {
    let index_id = "sort_by_dynamic_field".to_string();
    // In this test, we will try sorting docs by several fields.
    // - static_i64
    // - static_u64
    // - dynamic_i64
    // - dynamic_u64
    let doc_mapping_yaml = r#"
            mode: dynamic
            field_mappings:
              - name: static_u64
                type: u64
                fast: true
              - name: static_i64
                type: i64
                fast: true
            dynamic_mapping:
                fast: true
                stored: true
            "#;
    let test_sandbox = TestSandbox::create(&index_id, doc_mapping_yaml, "{}", &[])
        .await
        .unwrap();
    let docs = vec![
        // 0
        json!({"static_u64": 3u64, "dynamic_u64": 3u64, "static_i64": 0i64, "dynamic_i64": 0i64}),
        // 1
        json!({"static_u64": 2u64, "dynamic_u64": 2u64, "static_i64": -1i64, "dynamic_i64": -1i64}),
        // 2
        json!({}),
        // 3
        json!({"static_u64": 4u64, "dynamic_u64": (i64::MAX as u64) + 1, "static_i64": 1i64, "dynamic_i64": 1i64}),
    ];
    test_sandbox.add_documents(docs).await.unwrap();
    let search_hits = |sort_field: &str, order: SortOrder| {
        let query_ast_json = serde_json::to_string(&QueryAst::MatchAll).unwrap();
        let search_request = SearchRequest {
            index_id_patterns: vec![index_id.to_string()],
            query_ast: query_ast_json,
            max_hits: 1_000,
            sort_fields: vec![SortField {
                field_name: sort_field.to_string(),
                sort_order: order as i32,
            }],
            ..Default::default()
        };
        let metastore = test_sandbox.metastore();
        let storage_resolver = test_sandbox.storage_resolver();
        async move {
            let search_resp = single_node_search(search_request, metastore, storage_resolver)
                .await
                .unwrap();
            assert_eq!(search_resp.num_hits, 4);
            search_resp
                .hits
                .into_iter()
                .map(|hit| {
                    let partial_hit = hit.partial_hit.unwrap();
                    partial_hit.doc_id
                })
                .collect::<Vec<u32>>()
        }
    };
    {
        let ordered_docs: Vec<u32> = search_hits("static_u64", SortOrder::Desc).await;
        assert_eq!(&ordered_docs[..], &[3, 0, 1, 2]);
    }
    {
        let ordered_docs: Vec<u32> = search_hits("static_u64", SortOrder::Asc).await;
        assert_eq!(&ordered_docs[..], &[1, 0, 3, 2]);
    }
    {
        let ordered_docs: Vec<u32> = search_hits("static_i64", SortOrder::Desc).await;
        assert_eq!(&ordered_docs[..], &[3, 0, 1, 2]);
    }
    {
        let ordered_docs: Vec<u32> = search_hits("static_i64", SortOrder::Asc).await;
        assert_eq!(&ordered_docs[..], &[1, 0, 3, 2]);
    }
    {
        let ordered_docs: Vec<u32> = search_hits("dynamic_u64", SortOrder::Desc).await;
        assert_eq!(&ordered_docs[..], &[3, 0, 1, 2]);
    }
    {
        let ordered_docs: Vec<u32> = search_hits("dynamic_u64", SortOrder::Asc).await;
        assert_eq!(&ordered_docs[..], &[1, 0, 3, 2]);
    }
    {
        let ordered_docs: Vec<u32> = search_hits("dynamic_i64", SortOrder::Desc).await;
        assert_eq!(&ordered_docs[..], &[3, 0, 1, 2]);
    }
    {
        let ordered_docs: Vec<u32> = search_hits("dynamic_i64", SortOrder::Asc).await;
        assert_eq!(&ordered_docs[..], &[1, 0, 3, 2]);
    }
    test_sandbox.assert_quit().await;
}

#[tokio::test]
async fn test_sort_by_2_field() {
    let index_id = "sort_by_dynamic_field".to_string();
    // In this test, we will try sorting docs by several fields.
    // - static_u64
    // - dynamic_u64
    let doc_mapping_yaml = r#"
            mode: dynamic
            field_mappings:
              - name: static_u64
                type: u64
                fast: true
            dynamic_mapping:
                fast: true
                stored: true
            "#;
    let test_sandbox = TestSandbox::create(&index_id, doc_mapping_yaml, "{}", &[])
        .await
        .unwrap();
    let docs = vec![
        // 0
        json!({"static_u64": 3u64, "dynamic_u64": 3u64}),
        // 1
        json!({"static_u64": 3u64, "dynamic_u64": 2u64}),
        // 2
        json!({}),
        // 3
        json!({"dynamic_u64": 2u64}),
        // 4
        json!({"static_u64": 4u64, "dynamic_u64": (i64::MAX as u64) + 1}),
    ];
    test_sandbox.add_documents(docs).await.unwrap();
    let search_hits =
        |sort_field1: &str, order1: SortOrder, sort_field2: &str, order2: SortOrder| {
            let query_ast_json = serde_json::to_string(&QueryAst::MatchAll).unwrap();
            let search_request = SearchRequest {
                index_id_patterns: vec![index_id.to_string()],
                query_ast: query_ast_json,
                max_hits: 1_000,
                sort_fields: vec![
                    SortField {
                        field_name: sort_field1.to_string(),
                        sort_order: order1 as i32,
                    },
                    SortField {
                        field_name: sort_field2.to_string(),
                        sort_order: order2 as i32,
                    },
                ],
                ..Default::default()
            };
            let metastore = test_sandbox.metastore();
            let storage_resolver = test_sandbox.storage_resolver();
            async move {
                let search_resp = single_node_search(search_request, metastore, storage_resolver)
                    .await
                    .unwrap();
                assert_eq!(search_resp.num_hits, 5);
                search_resp
                    .hits
                    .into_iter()
                    .map(|hit| {
                        let partial_hit = hit.partial_hit.unwrap();
                        partial_hit.doc_id
                    })
                    .collect::<Vec<u32>>()
            }
        };
    {
        let ordered_docs: Vec<u32> = search_hits(
            "static_u64",
            SortOrder::Desc,
            "dynamic_u64",
            SortOrder::Desc,
        )
        .await;
        assert_eq!(&ordered_docs[..], &[4, 0, 1, 3, 2]);
    }
    {
        let ordered_docs: Vec<u32> =
            search_hits("static_u64", SortOrder::Desc, "dynamic_u64", SortOrder::Asc).await;
        assert_eq!(&ordered_docs[..], &[4, 1, 0, 3, 2]);
    }
    {
        let ordered_docs: Vec<u32> =
            search_hits("static_u64", SortOrder::Asc, "dynamic_u64", SortOrder::Desc).await;
        assert_eq!(&ordered_docs[..], &[0, 1, 4, 3, 2]);
    }
    {
        let ordered_docs: Vec<u32> =
            search_hits("static_u64", SortOrder::Asc, "dynamic_u64", SortOrder::Asc).await;
        assert_eq!(&ordered_docs[..], &[1, 0, 4, 3, 2]);
    }
    test_sandbox.assert_quit().await;
}

#[tokio::test]
async fn test_single_node_invalid_sorting_with_query() {
    let index_id = "single-node-invalid-sorting";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: description
                type: text
                fast: true
              - name: temperature
                type: i64
        "#;
    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["description"])
        .await
        .unwrap();

    let mut docs = Vec::new();
    for i in 0..30 {
        let description = format!("city info-{}", i + 1);
        docs.push(json!({"description": description, "ts": i+1, "temperature": i+32}));
    }
    test_sandbox.add_documents(docs).await.unwrap();

    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: qast_json_helper("city", &["description"]),
        max_hits: 15,
        sort_fields: vec![SortField {
            field_name: "description".to_string(),
            sort_order: SortOrder::Desc as i32,
        }],
        ..Default::default()
    };
    let single_node_response = single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await;
    assert!(single_node_response.is_err());
    let error_msg = single_node_response.unwrap_err().to_string();
    assert_eq!(
        error_msg,
        "Invalid argument: sort by field on type text is currently not supported `description`"
    );
    test_sandbox.assert_quit().await;
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
    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &[]).await?;
    let index_uid = test_sandbox.index_uid();

    let owners = ["paul", "adrien"];
    for owner in owners {
        let mut docs = Vec::new();
        for i in 0..10 {
            docs.push(json!({"body": format!("content num #{}", i + 1), "owner": owner}));
        }
        test_sandbox.add_documents(docs).await?;
    }

    let query_ast: QueryAst = qast_helper("owner:francois", &[]);

    let selected_splits = list_relevant_splits(
        vec![index_uid.clone()],
        None,
        None,
        extract_tags_from_query(query_ast),
        &*test_sandbox.metastore(),
    )
    .await?;
    assert!(selected_splits.is_empty());

    let query_ast: QueryAst = qast_helper("", &[]);

    let selected_splits = list_relevant_splits(
        vec![index_uid.clone()],
        None,
        None,
        extract_tags_from_query(query_ast),
        &*test_sandbox.metastore(),
    )
    .await?;
    assert_eq!(selected_splits.len(), 2);

    let query_ast: QueryAst = qast_helper("owner:francois OR owner:paul OR owner:adrien", &[]);

    let selected_splits = list_relevant_splits(
        vec![index_uid.clone()],
        None,
        None,
        extract_tags_from_query(query_ast),
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
    test_sandbox.assert_quit().await;
    Ok(())
}

async fn test_search_util(test_sandbox: &TestSandbox, query: &str) -> Vec<u32> {
    let splits = test_sandbox
        .metastore()
        .list_all_splits(test_sandbox.index_uid())
        .await
        .unwrap();
    let splits_offsets: Vec<_> = splits
        .into_iter()
        .map(|split_meta| extract_split_and_footer_offsets(&split_meta.split_metadata))
        .collect();
    let request = Arc::new(SearchRequest {
        index_id_patterns: vec![test_sandbox.index_uid().index_id().to_string()],
        query_ast: qast_json_helper(query, &[]),
        max_hits: 100,
        ..Default::default()
    });
    let searcher_context: Arc<SearcherContext> =
        Arc::new(SearcherContext::new(SearcherConfig::default(), None));
    let search_response = leaf_search(
        searcher_context,
        request,
        test_sandbox.storage(),
        splits_offsets,
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
    let test_sandbox = TestSandbox::create("search_dynamic_mode", doc_mapping_yaml, "{}", &[])
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
        let docs = test_search_util(&test_sandbox, "body:hello").await;
        assert_eq!(&docs[..], &[1u32, 0u32]);
    }
    {
        let docs = test_search_util(&test_sandbox, "body_dynamic:hello").await;
        assert_eq!(&docs[..], &[3u32]); // 1 is not matched due to the raw tokenizer
    }
    test_sandbox.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_search_dynamic_mode_expand_dots() -> anyhow::Result<()> {
    let doc_mapping_yaml = r#"
            field_mappings: []
            mode: dynamic
            #dynamic_mapping:
            #  expand_dots: true -- that's the default value.
        "#;
    let test_sandbox = TestSandbox::create(
        "search_dynamic_mode_expand_dots",
        doc_mapping_yaml,
        "{}",
        &[],
    )
    .await
    .unwrap();
    let docs = vec![json!({"k8s.component.name": "quickwit"})];
    test_sandbox.add_documents(docs).await.unwrap();
    {
        let docs = test_search_util(&test_sandbox, "k8s.component.name:quickwit").await;
        assert_eq!(&docs[..], &[0u32]);
    }
    {
        let docs = test_search_util(&test_sandbox, r"k8s\.component\.name:quickwit").await;
        assert_eq!(&docs[..], &[0u32]);
    }
    test_sandbox.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_search_dynamic_mode_do_not_expand_dots() -> anyhow::Result<()> {
    let doc_mapping_yaml = r#"
            field_mappings: []
            mode: dynamic
            dynamic_mapping:
                expand_dots: false
        "#;
    let test_sandbox = TestSandbox::create(
        "search_dynamic_mode_not_expand_dots",
        doc_mapping_yaml,
        "{}",
        &[],
    )
    .await
    .unwrap();
    let docs = vec![json!({"k8s.component.name": "quickwit"})];
    test_sandbox.add_documents(docs).await.unwrap();
    {
        let docs = test_search_util(&test_sandbox, r"k8s\.component\.name:quickwit").await;
        assert_eq!(&docs[..], &[0u32]);
    }
    {
        let docs = test_search_util(&test_sandbox, r#"k8s.component.name:quickwit"#).await;
        assert!(docs.is_empty());
    }
    test_sandbox.assert_quit().await;
    Ok(())
}

fn json_to_named_field_doc(doc_json: JsonValue) -> NamedFieldDocument {
    assert!(doc_json.is_object());
    let mut doc_map: BTreeMap<String, Vec<TantivyValue>> = BTreeMap::new();
    for (key, value) in doc_json.as_object().unwrap().clone() {
        doc_map.insert(key, json_value_to_tantivy_value(value));
    }
    NamedFieldDocument(doc_map)
}

fn json_value_to_tantivy_value(value: JsonValue) -> Vec<TantivyValue> {
    match value {
        JsonValue::Bool(val) => vec![TantivyValue::Bool(val)],
        JsonValue::String(val) => vec![TantivyValue::Str(val)],
        JsonValue::Array(values) => values
            .into_iter()
            .flat_map(json_value_to_tantivy_value)
            .collect(),
        JsonValue::Object(object) => {
            vec![TantivyValue::JsonObject(object)]
        }
        JsonValue::Null => Vec::new(),
        value => vec![value.into()],
    }
}

#[track_caller]
fn test_convert_leaf_hit_aux(
    default_doc_mapper_json: JsonValue,
    document_json: JsonValue,
    expected_hit_json: JsonValue,
) {
    let default_doc_mapper: DefaultDocMapper =
        serde_json::from_value(default_doc_mapper_json).unwrap();
    let named_field_doc = json_to_named_field_doc(document_json);
    let hit_json_str =
        convert_document_to_json_string(named_field_doc, &default_doc_mapper).unwrap();
    let hit_json: JsonValue = serde_json::from_str(&hit_json_str).unwrap();
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

// This spec might change in the future. The mode has no impact on the
// output of convert_document_to_json_string. In particular, it does not ignore
// the previously gathered dynamic field.
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
    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["color"]).await?;
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
        index_id_patterns: vec![index_id.to_string()],
        query_ast: qast_json_helper("*", &[]),
        max_hits: 2,
        aggregation_request: Some(agg_req.to_string()),
        ..Default::default()
    };
    let single_node_result = single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await?;
    let agg_res_json: JsonValue = serde_json::from_str(&single_node_result.aggregation.unwrap())?;
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
    test_sandbox.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_single_node_aggregation_missing_fast_field() {
    let index_id = "single-node-agg-2";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: color
                type: text
              - name: price
                type: f64
                fast: true
        "#;
    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["color"])
        .await
        .unwrap();
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

    test_sandbox.add_documents(docs.clone()).await.unwrap();
    let search_request = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: qast_json_helper("*", &[]),
        max_hits: 2,
        aggregation_request: Some(agg_req.to_string()),
        ..Default::default()
    };
    let single_node_error = single_node_search(
        search_request,
        test_sandbox.metastore(),
        test_sandbox.storage_resolver(),
    )
    .await
    .unwrap_err();
    let SearchError::Internal(error_msg) = single_node_error else {
        panic!();
    };
    assert!(error_msg.contains("Field \"color\" is not configured as fast field"));
    test_sandbox.assert_quit().await;
}

#[tokio::test]
async fn test_single_node_with_ip_field() -> anyhow::Result<()> {
    let index_id = "single-node-with-ip-field";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: log
                type: text
              - name: host
                type: ip
        "#;
    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["log"]).await?;
    let docs = vec![
        json!({"log": "User not found", "host": "192.168.0.1"}),
        json!({"log": "Request failed", "host": "10.10.12.123"}),
        json!({"log": "Request successful", "host": "10.10.11.125"}),
        json!({"log": "Auth service error", "host": "2001:db8::1:0:0:1"}),
        json!({"log": "Settings saved", "host": "::afff:4567:890a"}),
        json!({"log": "Request failed", "host": "10.10.12.123"}),
    ];
    test_sandbox.add_documents(docs.clone()).await?;
    {
        let search_request = SearchRequest {
            index_id_patterns: vec![index_id.to_string()],
            query_ast: qast_json_helper("*", &[]),
            max_hits: 10,
            ..Default::default()
        };
        let single_node_result = single_node_search(
            search_request,
            test_sandbox.metastore(),
            test_sandbox.storage_resolver(),
        )
        .await?;
        assert_eq!(single_node_result.num_hits, 6);
        assert_eq!(single_node_result.hits.len(), 6);
    }
    {
        let search_request = SearchRequest {
            index_id_patterns: vec![index_id.to_string()],
            query_ast: qast_json_helper("10.10.11.125", &["host"]),
            max_hits: 10,
            ..Default::default()
        };
        let single_node_result = single_node_search(
            search_request,
            test_sandbox.metastore(),
            test_sandbox.storage_resolver(),
        )
        .await?;
        assert_eq!(single_node_result.num_hits, 1);
        assert_eq!(single_node_result.hits.len(), 1);
        let hit_json: JsonValue = serde_json::from_str(&single_node_result.hits[0].json)?;
        let expected_json: JsonValue = json!({"log": "Request successful", "host": "10.10.11.125"});
        assert_json_include!(actual: hit_json, expected: expected_json);
    }
    test_sandbox.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_single_node_range_queries() -> anyhow::Result<()> {
    let index_id = "single-node-range-queries";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: datetime
                type: datetime
                fast: true
              - name: log
                type: text
              - name: status_code
                type: u64
                fast: true
              - name: host
                type: ip
                fast: true
              - name: latency
                type: f64
                fast: true
              - name: error_code
                type: i64
                fast: true
        "#;
    let docs = vec![
        json!({"datetime": "2023-01-10T15:13:35Z", "log": "User not found", "status_code": 404, "host": "192.168.0.1", "latency": 12.34, "error_code": 4}),
        json!({"datetime": "2023-01-10T15:13:36Z", "log": "Request failed", "status_code": 400, "host": "10.10.12.123", "latency": 56.78, "error_code": 1}),
        json!({"datetime": "2023-01-10T15:13:37Z", "log": "Request successful", "status_code": 200, "host": "10.10.11.125", "latency": 91.10, "error_code": -1}),
        json!({"datetime": "2023-01-10T15:13:38Z", "log": "Auth service error", "status_code": 401, "host": "2001:db8::1:0:0:1", "latency": 111.12, "error_code": 2}),
        json!({"datetime": "2023-01-10T15:13:39Z", "log": "Settings saved", "status_code": 200, "host": "::afff:4567:890a", "latency": 112.13, "error_code": -1}),
        json!({"datetime": "2023-01-10T15:13:40Z", "log": "Request failed", "status_code": 400, "host": "10.10.12.123", "latency": 114.15, "error_code": 1}),
    ];
    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["log"]).await?;
    test_sandbox.add_documents(docs).await?;
    {
        let search_request = SearchRequest {
            index_id_patterns: vec![index_id.to_string()],
            query_ast: qast_json_helper(
                "datetime:[2023-01-10T15:13:36Z TO 2023-01-10T15:13:38Z}",
                &[],
            ),
            max_hits: 10,
            ..Default::default()
        };
        let single_node_result = single_node_search(
            search_request,
            test_sandbox.metastore(),
            test_sandbox.storage_resolver(),
        )
        .await?;
        assert_eq!(single_node_result.num_hits, 2);
        assert_eq!(single_node_result.hits.len(), 2);
    }
    {
        let search_request = SearchRequest {
            index_id_patterns: vec![index_id.to_string()],
            query_ast: qast_json_helper("status_code:[400 TO 401]", &[]),
            max_hits: 10,
            ..Default::default()
        };
        let single_node_result = single_node_search(
            search_request,
            test_sandbox.metastore(),
            test_sandbox.storage_resolver(),
        )
        .await?;
        assert_eq!(single_node_result.num_hits, 3);
        assert_eq!(single_node_result.hits.len(), 3);
    }
    {
        let search_request = SearchRequest {
            index_id_patterns: vec![index_id.to_string()],
            query_ast: qast_json_helper("host:[10.0.0.0 TO 10.255.255.255]", &[]),
            max_hits: 10,
            ..Default::default()
        };
        let single_node_result = single_node_search(
            search_request,
            test_sandbox.metastore(),
            test_sandbox.storage_resolver(),
        )
        .await?;
        assert_eq!(single_node_result.num_hits, 3);
        assert_eq!(single_node_result.hits.len(), 3);
    }
    {
        let search_request = SearchRequest {
            index_id_patterns: vec![index_id.to_string()],
            query_ast: qast_json_helper("latency:[100 TO *]", &[]),
            max_hits: 10,
            ..Default::default()
        };
        let single_node_result = single_node_search(
            search_request,
            test_sandbox.metastore(),
            test_sandbox.storage_resolver(),
        )
        .await?;
        assert_eq!(single_node_result.num_hits, 3);
        assert_eq!(single_node_result.hits.len(), 3);
    }
    {
        let search_request = SearchRequest {
            index_id_patterns: vec![index_id.to_string()],
            query_ast: qast_json_helper("error_code:[-1 TO 1]", &[]),
            max_hits: 10,
            ..Default::default()
        };
        let single_node_result = single_node_search(
            search_request,
            test_sandbox.metastore(),
            test_sandbox.storage_resolver(),
        )
        .await?;
        assert_eq!(single_node_result.num_hits, 4);
        assert_eq!(single_node_result.hits.len(), 4);
    }
    test_sandbox.assert_quit().await;
    Ok(())
}

fn collect_str_terms(response: LeafListTermsResponse) -> Vec<String> {
    response
        .terms
        .into_iter()
        .map(|term| Term::wrap(term).value().as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn test_single_node_list_terms() -> anyhow::Result<()> {
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
        TestSandbox::create("single-node-list-terms", doc_mapping_yaml, "{}", &["body"]).await?;
    let docs = vec![
        json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy", "binary": "dGhpcyBpcyBhIHRlc3Qu"}),
        json!({"title": "beagle", "body": "The beagle is a breed of small scent hound, similar in appearance to the much larger foxhound.", "url": "http://beagle", "binary": "bWFkZSB5b3UgbG9vay4="}),
    ];
    test_sandbox.add_documents(docs).await.unwrap();

    let splits = test_sandbox
        .metastore()
        .list_all_splits(test_sandbox.index_uid())
        .await
        .unwrap();
    let splits_offsets: Vec<_> = splits
        .into_iter()
        .map(|split_meta| extract_split_and_footer_offsets(&split_meta.split_metadata))
        .collect();
    let searcher_context = Arc::new(SearcherContext::new(SearcherConfig::default(), None));

    {
        let request = ListTermsRequest {
            index_id: test_sandbox.index_uid().index_id().to_string(),
            field: "title".to_string(),
            start_key: None,
            end_key: None,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: Some(100),
        };
        let search_response = leaf_list_terms(
            searcher_context.clone(),
            &request,
            test_sandbox.storage(),
            &splits_offsets,
        )
        .await
        .unwrap();
        let terms = collect_str_terms(search_response);
        assert_eq!(terms, &["beagle", "snoopy",]);
    }
    {
        let request = ListTermsRequest {
            index_id: test_sandbox.index_uid().index_id().to_string(),
            field: "title".to_string(),
            start_key: None,
            end_key: None,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: Some(1),
        };
        let search_response = leaf_list_terms(
            searcher_context.clone(),
            &request,
            test_sandbox.storage(),
            &splits_offsets,
        )
        .await
        .unwrap();
        let terms = collect_str_terms(search_response);
        assert_eq!(terms, &["beagle"]);
    }
    {
        let request = ListTermsRequest {
            index_id: test_sandbox.index_uid().index_id().to_string(),
            field: "title".to_string(),
            start_key: Some("casper".as_bytes().to_vec()),
            end_key: None,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: Some(100),
        };
        let search_response = leaf_list_terms(
            searcher_context.clone(),
            &request,
            test_sandbox.storage(),
            &splits_offsets,
        )
        .await
        .unwrap();
        let terms = collect_str_terms(search_response);
        assert_eq!(terms, &["snoopy"]);
    }
    {
        let request = ListTermsRequest {
            index_id: test_sandbox.index_uid().index_id().to_string(),
            field: "title".to_string(),
            start_key: None,
            end_key: Some("casper".as_bytes().to_vec()),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: Some(100),
        };
        let search_response = leaf_list_terms(
            searcher_context.clone(),
            &request,
            test_sandbox.storage(),
            &splits_offsets,
        )
        .await
        .unwrap();
        let terms = collect_str_terms(search_response);
        assert_eq!(terms, &["beagle"]);
    }
    test_sandbox.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_single_node_find_trace_ids_collector() {
    let index_id = "single-node-find-trace-ids-collector";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: trace_id
                type: bytes
                fast: true
              - name: span_timestamp_secs
                type: datetime
                fast: true
                precision: seconds
        "#;
    let foo_trace_id = TraceId::new([1u8; 16]);
    let bar_trace_id = TraceId::new([2u8; 16]);
    let qux_trace_id = TraceId::new([3u8; 16]);
    let baz_trace_id = TraceId::new([4u8; 16]);

    let docs = vec![
        json!({"trace_id": foo_trace_id, "span_timestamp_secs": "2023-01-10T15:13:35Z"}),
        json!({"trace_id": foo_trace_id, "span_timestamp_secs": "2023-01-10T15:13:36Z"}),
        json!({"trace_id": foo_trace_id, "span_timestamp_secs": "2023-01-10T15:13:37Z"}),
        json!({"trace_id": foo_trace_id, "span_timestamp_secs": "2023-01-10T15:13:38Z"}),
        json!({"trace_id": foo_trace_id, "span_timestamp_secs": "2023-01-10T15:13:39Z"}),
        json!({"trace_id": foo_trace_id, "span_timestamp_secs": "2023-01-10T15:13:40Z"}),
        json!({"trace_id": bar_trace_id, "span_timestamp_secs": "2024-01-10T15:13:35Z"}),
        json!({"trace_id": bar_trace_id, "span_timestamp_secs": "2024-01-10T15:13:40Z"}),
        json!({"trace_id": qux_trace_id, "span_timestamp_secs": "2025-01-10T15:13:40Z"}),
        json!({"trace_id": qux_trace_id, "span_timestamp_secs": "2025-01-10T15:13:35Z"}),
        json!({"trace_id": baz_trace_id, "span_timestamp_secs": "2022-01-10T15:13:35Z"}),
    ];
    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &[])
        .await
        .unwrap();
    test_sandbox.add_documents(docs).await.unwrap();
    {
        let aggregations = r#"{
            "num_traces": 3,
            "trace_id_field_name": "trace_id",
            "span_timestamp_field_name": "span_timestamp_secs"
        }"#
        .to_string();

        let search_request = SearchRequest {
            index_id_patterns: vec![index_id.to_string()],
            query_ast: qast_json_helper("*", &[]),
            aggregation_request: Some(aggregations),
            ..Default::default()
        };
        let single_node_result = single_node_search(
            search_request,
            test_sandbox.metastore(),
            test_sandbox.storage_resolver(),
        )
        .await
        .unwrap();
        let aggregation = single_node_result.aggregation.unwrap();
        let trace_ids: Vec<Span> = serde_json::from_str(&aggregation).unwrap();
        assert_eq!(trace_ids.len(), 3);

        assert_eq!(trace_ids[0].trace_id, qux_trace_id);
        assert_eq!(
            trace_ids[0].span_timestamp.into_timestamp_secs(),
            1736522020
        );
        assert_eq!(trace_ids[1].trace_id, bar_trace_id);
        assert_eq!(
            trace_ids[1].span_timestamp.into_timestamp_secs(),
            1704899620
        );
        assert_eq!(trace_ids[2].trace_id, foo_trace_id);
        assert_eq!(
            trace_ids[2].span_timestamp.into_timestamp_secs(),
            1673363620
        );
    }
    test_sandbox.assert_quit().await;
}

#[tokio::test]
async fn test_search_in_text_field_with_custom_tokenizer() -> anyhow::Result<()> {
    let doc_mapping_yaml = r#"
            tokenizers:
              - name: custom_tokenizer
                type: ngram
                min_gram: 3
                max_gram: 5
                prefix_only: true
            field_mappings:
              - name: body
                type: text
                tokenizer: custom_tokenizer
                indexed: true
        "#;
    let test_sandbox = TestSandbox::create("search_custom_tokenizer", doc_mapping_yaml, "{}", &[])
        .await
        .unwrap();
    let docs = vec![json!({"body": "hellohappy"})];
    test_sandbox.add_documents(docs).await.unwrap();
    {
        let docs = test_search_util(&test_sandbox, "body:happy").await;
        assert!(&docs.is_empty());
    }
    {
        let docs = test_search_util(&test_sandbox, "body:hel").await;
        assert_eq!(&docs[..], &[0u32]);
    }
    test_sandbox.assert_quit().await;
    Ok(())
}
