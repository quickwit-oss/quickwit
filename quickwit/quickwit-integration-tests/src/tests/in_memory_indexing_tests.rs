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

use std::process::Command;

use quickwit_config::ConfigFormat;
use quickwit_config::service::QuickwitService;
use quickwit_rest_client::models::IngestSource;
use quickwit_rest_client::rest_client::CommitType;
use quickwit_serve::SearchRequestQueryString;
use serde_json::json;

use crate::test_utils::{ClusterSandboxBuilder, ingest};

#[tokio::test]
async fn test_in_memory_indexing() {
    // The flag is cached on first use. Set it before startup in a fresh process, rather than
    // mutating the environment shared by concurrent tests (including under cargo test).
    if std::env::var("QW_ENABLE_IN_MEMORY_INDEXING").as_deref() != Ok("true") {
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::in_memory_indexing_tests::test_in_memory_indexing",
                "--nocapture",
            ])
            .env("QW_ENABLE_IN_MEMORY_INDEXING", "true")
            .status()
            .unwrap();
        assert!(status.success(), "in-memory indexing test failed: {status}");
        return;
    }
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let client = sandbox.rest_client(QuickwitService::Indexer);
    let index_id = "in-memory-index";
    client
        .indexes()
        .create(
            r#"
            version: 0.8
            index_id: in-memory-index
            doc_mapping:
              field_mappings:
                - name: body
                  type: text
            "#,
            ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();
    sandbox.wait_for_indexing_pipelines(1).await.unwrap();
    ingest(
        &client,
        index_id,
        IngestSource::Str(
            "{\"body\":\"first message\"}\n{\"body\":\"second message\"}".to_string(),
        ),
        CommitType::Force,
    )
    .await
    .unwrap();
    // Search the published split, after serialization, packaging, and upload have completed.
    sandbox.assert_hit_count(index_id, "*", 2).await;
    let response = client
        .search(
            index_id,
            SearchRequestQueryString {
                query: "body:first".to_string(),
                max_hits: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(response.num_hits, 1);
    assert_eq!(response.hits, vec![json!({"body": "first message"})]);
    sandbox.shutdown().await.unwrap();
}
