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

use std::time::Duration;

use quickwit_config::ConfigFormat;
use quickwit_config::service::QuickwitService;
use quickwit_rest_client::models::IngestSource;
use quickwit_rest_client::rest_client::CommitType;
use serde_json::{Value as JsonValue, json};

use crate::test_utils::{ClusterSandboxBuilder, ingest};

/// A Server-Sent Event: (event name, JSON data).
type SseEvent = (String, JsonValue);

/// Minimal Server-Sent Events reader over a streaming HTTP response.
struct SseReader {
    response: reqwest::Response,
    buffer: String,
}

impl SseReader {
    /// Returns the next event, or `None` when the stream ends. Skips keep-alive comments.
    async fn next_event(&mut self) -> Option<SseEvent> {
        loop {
            if let Some(end) = self.buffer.find("\n\n") {
                let raw_event: String = self.buffer.drain(..end + 2).collect();
                let mut event_name = None;
                let mut data = String::new();
                for line in raw_event.lines() {
                    if let Some(value) = line.strip_prefix("event:") {
                        event_name = Some(value.trim().to_string());
                    } else if let Some(value) = line.strip_prefix("data:") {
                        data.push_str(value.trim());
                    }
                }
                let Some(event_name) = event_name else {
                    continue;
                };
                return Some((event_name, serde_json::from_str(&data).unwrap()));
            }
            let chunk = self.response.chunk().await.unwrap()?;
            self.buffer.push_str(std::str::from_utf8(&chunk).unwrap());
        }
    }
}

#[tokio::test]
async fn test_live_tail_streams_matching_docs_before_commit() {
    quickwit_common::setup_logging_for_tests();
    // The REST node coordinates the live tail and reaches the indexer node over gRPC.
    let sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Indexer])
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Metastore,
            QuickwitService::Searcher,
            QuickwitService::Janitor,
        ])
        .build_and_start()
        .await;
    let index_id = "test_live_tail";
    // Documents are not searchable before the commit timeout: they can only be received through
    // the live tail.
    let index_config = format!(
        r#"
        version: 0.8
        index_id: {index_id}
        doc_mapping:
            field_mappings:
            - name: level
              type: text
              tokenizer: raw
            - name: body
              type: text
        indexing_settings:
            commit_timeout_secs: 60
        "#
    );
    let indexer_client = sandbox.rest_client(QuickwitService::Indexer);
    indexer_client
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    // Ingest a first document so that a shard is opened and an indexing pipeline is spawned. It
    // must not match the query.
    ingest(
        &indexer_client,
        index_id,
        IngestSource::Str(json!({"level": "INFO", "body": "before tail"}).to_string()),
        CommitType::Auto,
    )
    .await
    .unwrap();
    sandbox.wait_for_indexing_pipelines(1).await.unwrap();

    let searcher_config = sandbox
        .node_configs
        .iter()
        .find(|(_, services)| services.contains(&QuickwitService::Searcher))
        .unwrap()
        .0
        .clone();
    let tail_url = format!(
        "http://{}/api/v1/{index_id}/tail?query=level:ERROR",
        searcher_config.rest_config.listen_addr
    );
    let response = reqwest::get(&tail_url).await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()["content-type"].to_str().unwrap(),
        "text/event-stream"
    );
    let mut sse_reader = SseReader {
        response,
        buffer: String::new(),
    };
    // The coordinator discovers the indexer within one refresh interval, once the indexer's
    // indexing tasks are gossiped: ingest until the first matching document is received.
    let ndjson = [
        json!({"level": "INFO", "body": "all good"}),
        json!({"level": "ERROR", "body": "connection refused"}),
    ]
    .iter()
    .map(|doc| doc.to_string())
    .collect::<Vec<_>>()
    .join("\n");
    let received_docs: Vec<JsonValue> = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            ingest(
                &indexer_client,
                index_id,
                IngestSource::Str(ndjson.clone()),
                CommitType::Auto,
            )
            .await
            .unwrap();
            let Ok(event_opt) =
                tokio::time::timeout(Duration::from_secs(2), sse_reader.next_event()).await
            else {
                continue;
            };
            let (event_name, data) = event_opt.unwrap();
            assert_ne!(event_name, "error", "unexpected error event: {data}");
            if event_name == "hits" {
                assert_eq!(data["index_id"], index_id);
                return data["docs"].as_array().unwrap().clone();
            }
        }
    })
    .await
    .unwrap();
    // Only the matching documents are streamed, well before the commit timeout.
    assert!(!received_docs.is_empty());
    for doc in received_docs {
        assert_eq!(doc, json!({"level": "ERROR", "body": "connection refused"}));
    }
    drop(sse_reader);

    // An invalid query is rejected before the stream is opened.
    let invalid_tail_url = format!(
        "http://{}/api/v1/{index_id}/tail?query=body:\"a b",
        searcher_config.rest_config.listen_addr
    );
    let response = reqwest::get(&invalid_tail_url).await.unwrap();
    assert_eq!(response.status(), 400);

    indexer_client
        .indexes()
        .delete(index_id, false)
        .await
        .unwrap();
    sandbox.shutdown().await.unwrap();
}
