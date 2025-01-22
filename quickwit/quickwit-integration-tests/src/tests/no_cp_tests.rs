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

//! Tests for cluster configurations without a control plane.

use quickwit_config::service::QuickwitService;
use quickwit_config::ConfigFormat;
use quickwit_rest_client::error::{ApiError, Error as RestClientError};
use quickwit_serve::SearchRequestQueryString;

use crate::test_utils::ClusterSandboxBuilder;

fn initialize_tests() {
    quickwit_common::setup_logging_for_tests();
    std::env::set_var("QW_ENABLE_INGEST_V2", "true");
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
            index_id: {}
            doc_mapping:
                field_mappings:
                - name: body
                  type: text
            indexing_settings:
                commit_timeout_secs: 1
            "#,
        index_id
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
        panic!("unexpected error: {:?}", search_error);
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
