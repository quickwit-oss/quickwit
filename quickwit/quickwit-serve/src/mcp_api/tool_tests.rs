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

use std::sync::Arc;

use quickwit_common::ServiceStream;
use quickwit_metastore::{IndexMetadata, ListIndexesMetadataResponseExt};
use quickwit_proto::metastore::{
    ListIndexesMetadataResponse, MetastoreServiceClient, MockMetastoreService,
};
use quickwit_proto::search::{
    CountHits, Hit, ListFieldsEntry, ListFieldsResponse, ListFieldsType, SearchResponse,
};
use quickwit_search::{MockSearchService, SearchError};
use serde_json::{Value, json};
use warp::Filter;

use super::tests::request;
use super::tools::Tools;
use super::{mcp_api_handlers, protocol};

async fn call(tools: &Tools, name: &str, arguments: Value) -> Value {
    let message = json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
        "params":{"name":name,"arguments":arguments}});
    let (status, response) = protocol::handle(
        message.to_string().as_bytes(),
        Some(protocol::PROTOCOL_VERSION),
        tools,
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    let response = response.unwrap();
    assert!(response.get("error").is_none(), "{response}");
    response["result"].clone()
}

fn payload(result: &Value) -> Value {
    assert_eq!(result["content"][0]["type"], "text");
    serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn test_mcp_search_uses_es_query_and_response_conversion() {
    let mut search = MockSearchService::new();
    search.expect_root_search().times(1).returning(|request| {
        assert_eq!(request.index_id_patterns, ["logs-*", "traces"]);
        assert_eq!(request.max_hits, 2);
        assert_eq!(request.start_offset, 3);
        assert_eq!(request.count_hits(), CountHits::CountAll);
        assert!(request.query_ast.contains("level"));
        assert!(request.query_ast.contains("error"));
        Ok(SearchResponse {
            num_hits: 42,
            hits: vec![Hit {
                json: json!({"message":"test", "secret":"hidden"}).to_string(),
                index_id: "logs-test".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        })
    });
    let routes = warp::path!("api" / "v1" / ..).and(mcp_api_handlers(
        Arc::new(search),
        MetastoreServiceClient::mocked(),
        Vec::new(),
    ));
    let response = request().json(&json!({"jsonrpc":"2.0","id":"search-1","method":"tools/call",
        "params":{"name":"search","arguments":{
            "index":"logs-*,traces",
            "params":{"size":"2", "from":"3", "track_total_hits":"true", "_source_includes":"message"},
            "body":{"query":{"term":{"level":"error"}}, "size":99}
        }}
    })).reply(&routes).await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body: Value = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(body["id"], "search-1");
    assert_eq!(body["result"]["isError"], false, "{body}");
    let search_result = payload(&body["result"]);
    assert_eq!(search_result["hits"]["total"]["value"], 42);
    assert_eq!(
        search_result["hits"]["hits"][0]["_source"],
        json!({"message":"test"})
    );
    assert_eq!(search_result["hits"]["hits"][0]["_index"], "logs-test");
}

#[tokio::test]
async fn test_mcp_count_and_query_parameter_encoding() {
    let mut search = MockSearchService::new();
    search.expect_root_search().times(1).returning(|request| {
        assert_eq!(request.index_id_patterns, ["logs"]);
        assert_eq!(request.max_hits, 0);
        assert_eq!(request.count_hits(), CountHits::CountAll);
        // '&', '+' and brackets must remain part of q, not introduce extra URL parameters.
        assert!(request.query_ast.contains("a&b+[c]"));
        Ok(SearchResponse {
            num_hits: 17,
            ..Default::default()
        })
    });
    let tools = Tools {
        search_service: Arc::new(search),
        metastore: MetastoreServiceClient::mocked(),
    };
    let result = call(
        &tools,
        "count",
        json!({"index":"logs", "params":{"q":"a&b+[c]"}}),
    )
    .await;
    assert_eq!(result["isError"], false);
    assert_eq!(payload(&result), json!({"count":17}));
}

#[tokio::test]
async fn test_mcp_field_capabilities() {
    let mut search = MockSearchService::new();
    search
        .expect_root_list_fields()
        .times(1)
        .returning(|request| {
            assert_eq!(request.index_id_patterns, ["logs-*"]);
            assert_eq!(request.field_patterns, ["message", "time*"]);
            assert_eq!(request.start_timestamp, Some(1700000000));
            assert!(request.query_ast.is_some());
            Ok(ListFieldsResponse {
                entries: vec![ListFieldsEntry {
                    field_name: "message".to_string(),
                    field_type: ListFieldsType::Str as i32,
                    index_ids: vec!["logs-test".to_string()],
                    searchable: true,
                    aggregatable: false,
                    ..Default::default()
                }],
            })
        });
    let tools = Tools {
        search_service: Arc::new(search),
        metastore: MetastoreServiceClient::mocked(),
    };
    let result = call(
        &tools,
        "get_field_capabilities",
        json!({"index":"logs-*",
            "params":{"fields":"message,time*", "start_timestamp":"1700000000"},
            "body":{"index_filter":{"term":{"level":"error"}}}
        }),
    )
    .await;
    assert_eq!(result["isError"], false);
    let fields = payload(&result);
    assert_eq!(fields["indices"], json!(["logs-test"]));
    assert_eq!(fields["fields"]["message"]["text"]["searchable"], true);
    assert_eq!(
        fields["fields"]["message"]["keyword"]["aggregatable"],
        false
    );
}

#[tokio::test]
async fn test_mcp_list_indices_defaults_and_patterns() {
    let mut metastore = MockMetastoreService::new();
    metastore
        .expect_list_indexes_metadata()
        .times(2)
        .returning(|request| {
            assert!(request.index_id_patterns == ["*"] || request.index_id_patterns == ["logs-*"]);
            Ok(ListIndexesMetadataResponse::for_test(Vec::new()))
        });
    let tools = Tools {
        search_service: Arc::new(MockSearchService::new()),
        metastore: MetastoreServiceClient::from_mock(metastore),
    };
    for arguments in [
        json!({}),
        json!({"index":"logs-*", "params":{"h":"index,docs.count"}}),
    ] {
        let result = call(&tools, "list_indices", arguments).await;
        assert_eq!(result["isError"], false);
        assert_eq!(payload(&result), json!([]));
    }
}

#[tokio::test]
async fn test_mcp_list_indices_returns_es_columns() {
    let mut metastore = MockMetastoreService::new();
    metastore
        .expect_list_indexes_metadata()
        .times(1)
        .returning(|request| {
            assert_eq!(request.index_id_patterns, ["logs"]);
            Ok(ListIndexesMetadataResponse::for_test(vec![
                IndexMetadata::for_test("logs", "ram:///indexes/logs"),
            ]))
        });
    metastore
        .expect_list_splits()
        .times(1)
        .returning(|_| Ok(ServiceStream::from(Vec::new())));
    let tools = Tools {
        search_service: Arc::new(MockSearchService::new()),
        metastore: MetastoreServiceClient::from_mock(metastore),
    };
    let result = call(
        &tools,
        "list_indices",
        json!({"index":"logs", "params":{"h":"index,docs.count"}}),
    )
    .await;
    assert_eq!(result["isError"], false, "{result}");
    assert_eq!(
        payload(&result),
        json!([{"index":"logs", "docs.count":"0"}])
    );
}

#[tokio::test]
async fn test_mcp_tool_validation_errors_do_not_call_backend() {
    let tools = Tools {
        search_service: Arc::new(MockSearchService::new()),
        metastore: MetastoreServiceClient::mocked(),
    };
    for (name, arguments) in [
        ("search", json!({})),
        ("search", json!(null)),
        ("search", json!({"index":"logs", "unknown":true})),
        ("search", json!({"index":"logs", "params":{"size":10}})),
        (
            "search",
            json!({"index":"logs", "params":{"size":"not-a-number"}}),
        ),
        (
            "search",
            json!({"index":"logs", "params":{"unknown":"value"}}),
        ),
        ("search", json!({"index":"logs", "params":{"scroll":"1m"}})),
        (
            "search",
            json!({"index":"logs", "body":{"query":{"not_a_query":{}}}}),
        ),
        ("search", json!({"index":""})),
        ("search", json!({"index":"logs,"})),
        ("search", json!({"index":"logs%2A"})),
        ("count", json!({"index":"logs", "params":{"size":"1"}})),
        (
            "get_field_capabilities",
            json!({"index":"logs", "params":{"fields":["message"]}}),
        ),
        ("list_indices", json!({"params":{"format":"text"}})),
    ] {
        let result = call(&tools, name, arguments.clone()).await;
        assert_eq!(result["isError"], true, "{name} {arguments}");
        assert_eq!(payload(&result)["status"], 400);
    }
}

#[tokio::test]
async fn test_mcp_backend_failure_is_tool_error() {
    let mut search = MockSearchService::new();
    search
        .expect_root_search()
        .times(1)
        .returning(|_| Err(SearchError::Internal("test failure".to_string())));
    let tools = Tools {
        search_service: Arc::new(search),
        metastore: MetastoreServiceClient::mocked(),
    };
    let result = call(&tools, "search", json!({"index":"logs"})).await;
    assert_eq!(result["isError"], true);
    assert_eq!(payload(&result)["status"], 500);
    assert!(
        payload(&result)["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("test failure")
    );
}
