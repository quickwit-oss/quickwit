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

use std::collections::BTreeMap;

use quickwit_authorize::{UserTokenSpec, generate_root_key_pair, generate_token};
use quickwit_config::service::QuickwitService;
use quickwit_config::{AuthorizationConfig, ConfigFormat};
use quickwit_metastore::SplitState;
use quickwit_rest_client::rest_client::CommitType;
use quickwit_serve::SearchRequestQueryString;
use reqwest::StatusCode;
use serde_json::json;

use crate::ingest_json;
use crate::test_utils::{ClusterSandboxBuilder, ingest};

fn make_user_token(root_private_key: &str, user: &str, roles: &[&str]) -> String {
    let spec = UserTokenSpec {
        user_opt: Some(user.to_string()),
        roles: roles.iter().map(|role| role.to_string()).collect(),
        ..Default::default()
    };
    generate_token(root_private_key, &spec).unwrap()
}

/// Authorization is initialized once per process: this test must run in its own process, which
/// is the case with `cargo nextest`.
#[tokio::test]
async fn test_authorization_multi_nodes() {
    quickwit_common::setup_logging_for_tests();
    let (root_private_key, root_public_key) = generate_root_key_pair();
    let node_token = generate_token(
        &root_private_key,
        &UserTokenSpec {
            services: vec!["node".to_string()],
            ..Default::default()
        },
    )
    .unwrap();
    let admin_token = make_user_token(&root_private_key, "alice", &["admin"]);
    let reader_token = make_user_token(&root_private_key, "bob", &["reader"]);
    let writer_token = make_user_token(&root_private_key, "carol", &["writer"]);

    let roles = BTreeMap::from([
        (
            "admin".to_string(),
            vec![
                "index:read".to_string(),
                "index:write".to_string(),
                "index:admin".to_string(),
                "cluster".to_string(),
            ],
        ),
        ("reader".to_string(), vec!["index:read".to_string()]),
        ("writer".to_string(), vec!["index:write".to_string()]),
    ]);
    let authorization_config = AuthorizationConfig {
        root_public_key,
        node_token,
        roles,
    };
    // The searcher runs on its own node so that its metastore calls go through gRPC.
    let mut sandbox_config = ClusterSandboxBuilder::default()
        .add_node([
            QuickwitService::Metastore,
            QuickwitService::ControlPlane,
            QuickwitService::Indexer,
            QuickwitService::Janitor,
        ])
        .add_node([QuickwitService::Searcher])
        .build_config()
        .await;
    for (node_config, _) in &mut sandbox_config.node_configs {
        node_config.authorization_config = Some(authorization_config.clone());
    }
    sandbox_config.rest_auth_token_opt = Some(admin_token.clone());
    let sandbox = sandbox_config.start().await;

    // No token or a token signed by another key: 401.
    let anonymous_client = sandbox.rest_client_with_auth_token(QuickwitService::Searcher, None);
    let error = anonymous_client.cluster().snapshot().await.unwrap_err();
    assert_eq!(error.status_code(), Some(StatusCode::UNAUTHORIZED));

    let (other_private_key, _) = generate_root_key_pair();
    let forged_token = make_user_token(&other_private_key, "mallory", &["admin"]);
    let forged_client =
        sandbox.rest_client_with_auth_token(QuickwitService::Searcher, Some(forged_token));
    let error = forged_client.cluster().snapshot().await.unwrap_err();
    assert_eq!(error.status_code(), Some(StatusCode::UNAUTHORIZED));

    let index_id = "test-authorization-index";
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
    // A reader cannot create an index.
    let reader_client =
        sandbox.rest_client_with_auth_token(QuickwitService::Indexer, Some(reader_token.clone()));
    let error = reader_client
        .indexes()
        .create(index_config.clone(), ConfigFormat::Yaml, false)
        .await
        .unwrap_err();
    assert_eq!(error.status_code(), Some(StatusCode::FORBIDDEN));

    // An admin can.
    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(index_config, ConfigFormat::Yaml, false)
        .await
        .unwrap();

    // A reader cannot ingest.
    let error = ingest(
        &reader_client,
        index_id,
        ingest_json!({"body": "from the reader"}),
        CommitType::Auto,
    )
    .await
    .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<quickwit_rest_client::error::Error>()
            .and_then(|error| error.status_code()),
        Some(StatusCode::FORBIDDEN)
    );

    // A writer can ingest. Indexing then runs in the background with the node token.
    let writer_client =
        sandbox.rest_client_with_auth_token(QuickwitService::Indexer, Some(writer_token.clone()));
    ingest(
        &writer_client,
        index_id,
        ingest_json!({"body": "from the writer"}),
        CommitType::Force,
    )
    .await
    .unwrap();
    sandbox
        .wait_for_splits(index_id, Some(vec![SplitState::Published]), 1)
        .await
        .unwrap();

    // A reader can search, on a searcher node that reaches the metastore over gRPC.
    let search_query = || SearchRequestQueryString {
        query: "body:writer".to_string(),
        max_hits: 10,
        ..Default::default()
    };
    let searcher_reader_client =
        sandbox.rest_client_with_auth_token(QuickwitService::Searcher, Some(reader_token));
    let search_response = searcher_reader_client
        .search(index_id, search_query())
        .await
        .unwrap();
    assert_eq!(search_response.num_hits, 1);

    // A writer cannot search: the metastore rejects its `ListIndexesMetadata` call.
    let searcher_writer_client =
        sandbox.rest_client_with_auth_token(QuickwitService::Searcher, Some(writer_token));
    let error = searcher_writer_client
        .search(index_id, search_query())
        .await
        .unwrap_err();
    assert_eq!(error.status_code(), Some(StatusCode::FORBIDDEN));

    // The health endpoints remain public.
    anonymous_client.node_health().is_live().await.unwrap();

    sandbox.shutdown().await.unwrap();
}

#[test]
fn test_authorization_config_json_roundtrip() {
    // Guards the config field names used by the test above.
    let authorization_config: AuthorizationConfig = serde_json::from_value(json!({
        "root_public_key": "abcd",
        "node_token": "efgh",
        "roles": {"reader": ["index:read"]},
    }))
    .unwrap();
    assert_eq!(authorization_config.roles["reader"], vec!["index:read"]);
}
