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

//! Tests for cluster configurations without a control plane.

use quickwit_config::ConfigFormat;
use quickwit_config::service::QuickwitService;
use quickwit_rest_client::error::{ApiError, Error as RestClientError};
use quickwit_serve::SearchRequestQueryString;

use crate::test_utils::ClusterSandboxBuilder;

fn initialize_tests() {
    // SAFETY: this test may not be entirely sound if not run with nextest or --test-threads=1
    // as this is only a test, and it would be extremly inconvenient to run it in a different way,
    // we are keeping it that way

    quickwit_common::setup_logging_for_tests();
    unsafe { std::env::set_var("QW_ENABLE_INGEST_V2", "true") };
}

#[tokio::test]
async fn test_search_after_control_plane_shutdown() {
    initialize_tests();
    let mut sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::ControlPlane])
        .add_node([QuickwitService::Metastore])
        .add_node([QuickwitService::Searcher])
        .build_and_start()
        .await;
    let index_id = "test-search-after-control-plane-shutdown";
    let index_config = format!(
        r#"
            version: 0.8
            index_id: {index_id}
            doc_mapping:
                field_mappings:
                - name: body
                  type: text
            indexing_settings:
                commit_timeout_secs: 1
            "#
    );

    sandbox
        .rest_client(QuickwitService::Metastore)
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap();

    sandbox
        .shutdown_services([QuickwitService::ControlPlane])
        .await
        .unwrap();

    sandbox.assert_hit_count(index_id, "", 0).await;

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_searcher_and_metastore_without_control_plane() {
    initialize_tests();
    let sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Metastore])
        .add_node([QuickwitService::Searcher])
        .build_and_start()
        .await;

    // we cannot create an actual index without control plane

    let search_error = sandbox
        .rest_client(QuickwitService::Searcher)
        .search(
            "does-not-exist",
            SearchRequestQueryString {
                query: String::new(),
                max_hits: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

    if let RestClientError::Api(ApiError { message, code }) = search_error {
        assert_eq!(
            message.unwrap(),
            "could not find indexes matching the IDs `[\"does-not-exist\"]`"
        );
        assert_eq!(code.as_u16(), 404);
    } else {
        panic!("unexpected error: {search_error:?}");
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
#[should_panic]
async fn test_indexer_fails_without_control_plane() {
    initialize_tests();
    let sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Metastore])
        .add_node([QuickwitService::Indexer, QuickwitService::Searcher])
        .build_and_start()
        .await;

    let _ = sandbox.shutdown().await;
}
