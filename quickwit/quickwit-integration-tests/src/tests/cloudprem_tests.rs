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

use prost::Message;
use prost_types::Any;
use quickwit_config::service::QuickwitService;
use quickwit_proto::cloudprem::*;
use serde_json::Value;
use tonic::Request;

use crate::test_utils::{ClusterSandbox, ClusterSandboxBuilder};

const TEST_CERT: &[u8] = include_bytes!("../../test_data/test_cert_main_ca.crt");

fn build_list_request(query: &QueryNode) -> ListRequest {
    let any_query = Any {
        type_url: "type.googleapis.com/queryparser_proto.QueryNode".to_string(),
        value: query.encode_to_vec(),
    };
    ListRequest {
        query: Some(any_query),
        num_events_to_fetch: 5,
        should_compute_count: true,
        columns: Vec::new(), // ?
        sort: Vec::new(),    // check in staging what is always sent
        org_id: 2,
    }
}

fn authenticated_request<T>(raw_request: T) -> Request<T> {
    let mut request = Request::new(raw_request);

    let encoded_cert = urlencoding::encode_binary(TEST_CERT);
    request
        .metadata_mut()
        .insert("x-amzn-mtls-clientcert", encoded_cert.parse().unwrap());
    request
}

async fn setup_env(docs: &[Value]) -> ClusterSandbox {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;

    {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let indexing_service_counters = sandbox
            .rest_client(QuickwitService::Indexer)
            .node_stats()
            .indexing()
            .await
            .unwrap();
        assert_eq!(indexing_service_counters.num_running_pipelines, 0);
    }

    sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .create(
            include_str!("../../../../config/cloudprem/datadog.yaml"),
            quickwit_config::ConfigFormat::Yaml,
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

    sandbox.wait_for_indexing_pipelines(1).await.unwrap();
    sandbox.local_ingest("datadog", docs).await.unwrap();

    sandbox
}

#[tokio::test]
async fn test_simple_list() {
    let sandbox = setup_env(&[]).await;

    let mut client = sandbox.cloudprem_client();

    let query_node = QueryNode {
        node: Some(query_node::Node::All(MatchAllQueryNode {})),
    };
    let request = build_list_request(&query_node);

    // this should fail for lack of auth
    client.list(request.clone()).await.unwrap_err();

    let res = client.list(authenticated_request(request)).await.unwrap();
    let res = res.into_inner();
    assert_eq!(res.count, 0);
    assert_eq!(res.streams, vec![Stream { events: Vec::new() }]);

    sandbox.shutdown().await.unwrap();
}
