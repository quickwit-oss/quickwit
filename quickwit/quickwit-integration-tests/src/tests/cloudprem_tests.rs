use std::time::Duration;

use itertools::Itertools;
use pomchi::DatadogLogMsg;
use prost::Message;
use prost_types::Any;
use quickwit_config::service::QuickwitService;
use quickwit_proto::cloudprem::*;
use quickwit_proto::types::IndexId;
use serde_json::Value;
use tonic::Request;

use crate::test_utils::{ClusterSandbox, ClusterSandboxBuilder};

const TEST_CERT: &[u8] = include_bytes!("../../test_data/test_cert_main_ca.crt");

// this data is extracted from staging through the admin ui, and cleaned off a bit
//
// to generate a larger dataset, go to https://logs-admin.us1.staging.dog/web/#/query/replayer ,
// select the `sample list` example. configure a limit (n of logs to retrieve). Add
// `random_draw:>0.99` to the filter (or other chance of your choosing) so as to get random logs,
// and not a burst from a single service.
// copy the result to a file.
// run `jq -r .list.events[].content $your_file | jq -sc . > test_data.json` to convert to the
// right format
// some tests are written as to work with any dataset smaller than 100 elems (configurable),
// others may assert on values specific to the provided dataset
const TEST_DATA: &[u8] = include_bytes!("../../test_data/test_data.json");

fn build_list_request(query: &QueryNode) -> ListRequest {
    let any_query = Any {
        type_url: "type.googleapis.com/queryparser_proto.QueryNode".to_string(),
        value: query.encode_to_vec(),
    };
    ListRequest {
        query: Some(any_query),
        num_events_to_fetch: 100,
        should_compute_count: true,
        columns: Vec::new(),
        fetch_only_requested_columns: false,
        sort: vec![
            SortKv {
                ascending: false,
                name: "timestamp".to_string(),
                path: "timestamp".to_string(),
            },
            SortKv {
                ascending: false,
                name: "tiebreaker".to_string(),
                path: "tiebreaker".to_string(),
            },
        ],
        search_after: None,
        org_id: 2,
        scope: Default::default(),
        index_id_patterns: Vec::new(),
    }
}

fn build_aggregation_request(
    query: &QueryNode,
    aggregation: aggregation::Aggregation,
) -> AggregationRequest {
    let any_query = Any {
        type_url: "type.googleapis.com/queryparser_proto.QueryNode".to_string(),
        value: query.encode_to_vec(),
    };
    AggregationRequest {
        query: Some(any_query),
        aggregation: Some(Aggregation {
            aggregation: Some(aggregation),
        }),
        org_id: 2,
        scope: Default::default(),
        index_id_patterns: Vec::new(),
    }
}

fn agg_computes(compute_aggs: &[aggregation::Aggregation]) -> aggregation::Aggregation {
    aggregation::Aggregation::Computes(Computes {
        aggregation: compute_aggs
            .iter()
            .map(|agg| Aggregation {
                aggregation: Some(agg.clone()),
            })
            .collect(),
        time_grouping: Vec::new(),
    })
}

fn agg_group_by(
    expression: Option<ExpressionNode>,
    child: aggregation::Aggregation,
) -> aggregation::Aggregation {
    aggregation::Aggregation::AttributeGroupBy(Box::new(AttributeGroupBy {
        expression,
        limit: 100,
        sort: None, // not implemented
        missing: None,
        total: None,
        child: Some(Box::new(Aggregation {
            aggregation: Some(child),
        })),
        include: None,
    }))
}

fn agg_time_grouping(child: aggregation::Aggregation) -> aggregation::Aggregation {
    aggregation::Aggregation::TimeGroupBy(Box::new(TimeGrouping {
        output: "timeagg".to_string(),
        path: "timestamp".to_string(),
        time_zone: String::new(),
        interval_ns: Some(1_000_000_000 * 10),
        rollup: None,
        child: Some(Box::new(Aggregation {
            aggregation: Some(child),
        })),
    }))
}

fn agg_count(id: &str) -> aggregation::Aggregation {
    aggregation::Aggregation::MetricCompute(MetricCompute {
        expression: expression_field("count"),
        id: id.to_string(),
        r#type: "COUNT".to_string(),
    })
}

fn expression_field(field_name: &str) -> Option<ExpressionNode> {
    let calc_node = CalcNode {
        calc_node: Some(calc_node::CalcNode::FieldRef(calc_node::FieldRef {
            field_name: field_name.to_string(),
        })),
    };
    let calc_node_any = Any {
        type_url: "type.googleapis.com/calcfieldspb.CalcNode".to_string(),
        value: calc_node.encode_to_vec(),
    };

    Some(ExpressionNode {
        calc_node: Some(calc_node_any),
    })
}

fn authenticated_request<T>(raw_request: T) -> Request<T> {
    let mut request = Request::new(raw_request);

    let encoded_cert = urlencoding::encode_binary(TEST_CERT);
    request
        .metadata_mut()
        .insert("x-amzn-mtls-clientcert", encoded_cert.parse().unwrap());
    request
}

async fn setup_env(docs: &mut [Value]) -> ClusterSandbox {
    // Match CI environment: disable telemetry, reverse connection, and allow old test data
    // timestamps (test data is from April 2025).
    // SAFETY: called before spawning threads in test setup.
    unsafe {
        std::env::set_var("QW_DISABLE_TELEMETRY", "1");
        std::env::set_var("CP_ENABLE_REVERSE_CONNECTION", "false");
        std::env::set_var("QW_MAX_LOG_PAST_AGE_HOURS", "10000000000");
    }
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
            include_str!("../../../../config/cloudprem/datadog-logs.yaml"),
            quickwit_config::ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();

    assert!(
        sandbox
            .rest_client(QuickwitService::Indexer)
            .node_health()
            .is_live()
            .await
            .unwrap()
    );
    // Check if docs can be serialized from json::Value to DatadogLogMsg
    for doc in docs.iter() {
        serde_json::from_value::<DatadogLogMsg>(doc.clone()).unwrap_or_else(|e| {
            panic!(
                "{}: failed to deserialize doc {} to DatadogLogMsg",
                e,
                serde_json::to_string_pretty(doc).unwrap()
            )
        });
    }

    sandbox.wait_for_indexing_pipelines(1).await.unwrap();
    sandbox.local_ingest("datadog", docs).await.unwrap();

    sandbox
}

#[tokio::test]
async fn test_list() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    let query_node = QueryNode {
        node: Some(query_node::Node::None(MatchNoneQueryNode {})),
    };
    let request = build_list_request(&query_node);

    // this should fail for lack of auth
    client.list(request.clone()).await.unwrap_err();

    let res = client.list(authenticated_request(request)).await.unwrap();
    let res = res.into_inner();
    assert_eq!(res.count, 0);
    assert_eq!(res.streams, vec![Stream { events: Vec::new() }]);

    let query_node = QueryNode {
        node: Some(query_node::Node::All(MatchAllQueryNode {})),
    };
    let request = build_list_request(&query_node);

    let res = client.list(authenticated_request(request)).await.unwrap();
    let res = res.into_inner();
    assert_eq!(res.count as usize, data.len());
    assert_eq!(res.streams[0].events.len(), data.len());

    let parse_res =
        |i: usize| serde_json::from_str(&res.streams[0].events[i].content_json).unwrap();
    let event_tracker = |i: usize| res.streams[0].events[i].tracker.as_ref().unwrap();

    for (i, doc) in data.iter().enumerate() {
        let parsed_res: Value = parse_res(i);
        let custom_from_msg: Value =
            serde_json::from_str(doc.get("message").unwrap().as_str().unwrap()).unwrap();
        assert_eq!(parsed_res.get("custom").unwrap(), &custom_from_msg,);
        let timestamp_num = doc.get("timestamp").unwrap().as_u64().unwrap();
        assert_eq!(event_tracker(i).epoch_ms, timestamp_num);
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_index_routing_table_crud() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    // Step 1: Get routing table - should have default catch-all rule
    let get_response = client
        .get_index_routing_table(authenticated_request(GetIndexRoutingTableRequest {
            org_id: 2,
            cluster_id: "test-cluster".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    let routing_table = &get_response.routing_table;
    assert_eq!(routing_table.len(), 1, "should have default catch-all rule");
    assert_eq!(routing_table[0].filter, "*");
    assert_eq!(routing_table[0].index_id, "datadog");

    // Step 2: Set routing table with a specific filter
    let set_request = SetIndexRoutingTableRequest {
        routing_table: vec![IndexRoutingRule {
            filter: "source:nginx".to_string(),
            index_id: "datadog".to_string(),
        }],
        org_id: 2,
        cluster_id: "test-cluster".to_string(),
    };
    client
        .set_index_routing_table(authenticated_request(set_request))
        .await
        .expect("set_index_routing_table should succeed");

    // Step 3: Verify routing table was updated
    let get_response = client
        .get_index_routing_table(authenticated_request(GetIndexRoutingTableRequest {
            org_id: 2,
            cluster_id: "test-cluster".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    let routing_table = &get_response.routing_table;
    assert_eq!(routing_table.len(), 1);
    assert_eq!(routing_table[0].filter, "source:nginx");
    assert_eq!(routing_table[0].index_id, "datadog");

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_index_routing_table_error_non_existent_index() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    // Try to set routing table with non-existent index - should fail
    let set_request = SetIndexRoutingTableRequest {
        routing_table: vec![IndexRoutingRule {
            filter: "*".to_string(),
            index_id: "non-existent-index".to_string(),
        }],
        org_id: 2,
        cluster_id: "test-cluster".to_string(),
    };
    let err = client
        .set_index_routing_table(authenticated_request(set_request))
        .await
        .expect_err("set_index_routing_table should fail with non-existent index");

    assert!(
        err.message().contains("non-existent"),
        "error should mention non-existent index: {}",
        err.message()
    );

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_index_crud_operations() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    let test_index_id = "test-index";

    // Step 1: Get indexes - record initial index count
    let get_response = client
        .get_indexes(authenticated_request(GetIndexesRequest {
            org_id: 2,
            cluster_id: "test-cluster".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    let initial_index_count = get_response.indexes.len();

    // Step 2: Create index with retention policy
    let create_request = CreateIndexRequest {
        index_id: test_index_id.to_string(),
        index_config: Some(index::IndexConfig {
            retention_policy: Some(index::RetentionPolicy {
                period: "8 days".to_string(),
            }),
        }),
        org_id: 2,
        cluster_id: "test-cluster".to_string(),
    };
    let create_response = client
        .create_index(authenticated_request(create_request))
        .await
        .expect("create_index should succeed")
        .into_inner();

    let created_metadata = create_response.index_metadata.unwrap();
    let created_config = created_metadata.index_config.unwrap();
    assert_eq!(created_metadata.index_id, test_index_id);
    assert_eq!(
        created_config.retention_policy.as_ref().unwrap().period,
        "8 days"
    );

    // Step 3: Verify with get_indexes - should have one more index
    {
        let get_response = client
            .get_indexes(authenticated_request(GetIndexesRequest {
                org_id: 2,
                cluster_id: "test-cluster".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(get_response.indexes.len(), initial_index_count + 1);
        assert!(
            get_response
                .indexes
                .iter()
                .any(|idx| idx.index_id == test_index_id)
        );
    }

    // Step 4: Update index - change retention policy
    let update_request = UpdateIndexRequest {
        index_id: test_index_id.to_string(),
        index_config: Some(index::IndexConfig {
            retention_policy: Some(index::RetentionPolicy {
                period: "32 days".to_string(),
            }),
        }),
        org_id: 2,
        cluster_id: "test-cluster".to_string(),
    };
    let update_response = client
        .update_index(authenticated_request(update_request))
        .await
        .expect("update_index should succeed")
        .into_inner();

    let updated_metadata = update_response.index_metadata.unwrap();
    let updated_config = updated_metadata.index_config.unwrap();
    assert_eq!(updated_metadata.index_id, test_index_id);
    assert_eq!(
        updated_config.retention_policy.as_ref().unwrap().period,
        "32 days"
    );

    // Step 5: Verify update with get_indexes
    {
        let get_response = client
            .get_indexes(authenticated_request(GetIndexesRequest {
                org_id: 2,
                cluster_id: "test-cluster".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        let index = get_response
            .indexes
            .iter()
            .find(|idx| idx.index_id == test_index_id)
            .expect("updated index should be in list");
        let config = index.index_config.as_ref().unwrap();
        assert_eq!(config.retention_policy.as_ref().unwrap().period, "32 days");
    }

    // Step 6: Delete index
    let delete_request = DeleteIndexRequest {
        index_id: test_index_id.to_string(),
        org_id: 2,
        cluster_id: "test-cluster".to_string(),
    };
    client
        .delete_index(authenticated_request(delete_request))
        .await
        .expect("delete_index should succeed");

    // Step 7: Verify deletion - should be back to initial count
    {
        let get_response = client
            .get_indexes(authenticated_request(GetIndexesRequest {
                org_id: 2,
                cluster_id: "test-cluster".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(get_response.indexes.len(), initial_index_count);
        assert!(
            !get_response
                .indexes
                .iter()
                .any(|idx| idx.index_id == test_index_id),
            "deleted index should not be in list"
        );
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_index_invalid_retention_period() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;
    let mut client = sandbox.cloudprem_client();

    // create_index with invalid retention period should fail
    let create_request = CreateIndexRequest {
        index_id: "test-invalid-retention".to_string(),
        index_config: Some(index::IndexConfig {
            retention_policy: Some(index::RetentionPolicy {
                period: "not-a-valid-duration".to_string(),
            }),
        }),
        org_id: 2,
        cluster_id: "test-cluster".to_string(),
    };
    let result = client
        .create_index(authenticated_request(create_request))
        .await;
    assert!(
        result.is_err(),
        "create_index should reject invalid retention period"
    );

    // Create a valid index, then update_index with invalid retention should fail
    let create_request = CreateIndexRequest {
        index_id: "test-invalid-retention".to_string(),
        index_config: Some(index::IndexConfig {
            retention_policy: Some(index::RetentionPolicy {
                period: "7 days".to_string(),
            }),
        }),
        org_id: 2,
        cluster_id: "test-cluster".to_string(),
    };
    client
        .create_index(authenticated_request(create_request))
        .await
        .expect("create_index should succeed");

    let update_request = UpdateIndexRequest {
        index_id: "test-invalid-retention".to_string(),
        index_config: Some(index::IndexConfig {
            retention_policy: Some(index::RetentionPolicy {
                period: "not-a-valid-duration".to_string(),
            }),
        }),
        org_id: 2,
        cluster_id: "test-cluster".to_string(),
    };
    let result = client
        .update_index(authenticated_request(update_request))
        .await;
    assert!(
        result.is_err(),
        "update_index should reject invalid retention period"
    );

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_search_after() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    let query_node = QueryNode {
        node: Some(query_node::Node::All(MatchAllQueryNode {})),
    };
    let mut request = build_list_request(&query_node);

    let res = client
        .list(authenticated_request(request.clone()))
        .await
        .unwrap();
    let res = res.into_inner();
    let events = &res.streams[0].events;

    for (i, event) in events.iter().enumerate() {
        request.search_after = event.tracker.clone();

        let after_res = client
            .list(authenticated_request(request.clone()))
            .await
            .unwrap();
        let after_res = after_res.into_inner();
        assert_eq!(after_res.streams[0].events, &events[i + 1..]);
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_fetch_one() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    let query_node = QueryNode {
        node: Some(query_node::Node::All(MatchAllQueryNode {})),
    };
    let list_request = build_list_request(&query_node);

    let list_res = client
        .list(authenticated_request(list_request))
        .await
        .unwrap();
    let list_res = list_res.into_inner();

    for i in 0..data.len() {
        let mut source_event_tracker = list_res.streams[0].events[i].tracker.clone().unwrap();
        let fetch_request = FetchOneRequest {
            event_tracker: Some(source_event_tracker.clone()),
            restriction_query: None,
            org_id: 2,
            scope: Default::default(),
            index_id_patterns: Vec::new(),
        };
        let fetch_res = client
            .fetch_one(authenticated_request(fetch_request))
            .await
            .unwrap();
        let fetch_res = fetch_res.into_inner();
        assert_eq!(fetch_res.event.unwrap(), list_res.streams[0].events[i]);

        // simulate the split containing our event having went through merging since we listed
        // resutls.
        source_event_tracker.fragment_id = Some("01JRAZ6KW4QVQESE2JCDGN3TFM".to_string());
        let fetch_request = FetchOneRequest {
            event_tracker: Some(source_event_tracker),
            restriction_query: None,
            org_id: 2,
            scope: Default::default(),
            index_id_patterns: Vec::new(),
        };
        let fetch_res = client
            .fetch_one(authenticated_request(fetch_request))
            .await
            .unwrap();
        let fetch_res = fetch_res.into_inner();
        assert_eq!(fetch_res.event.unwrap(), list_res.streams[0].events[i]);
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_fetch_one_unknown_id() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    let fetch_request = FetchOneRequest {
        event_tracker: Some(EventTracker {
            id: "unknown id".to_string(),
            epoch_ms: 123456789,
            tiebreaker: 0,
            fragment_id: None,
            row_number: None,
        }),
        restriction_query: None,
        org_id: 2,
        scope: Default::default(),
        index_id_patterns: Vec::new(),
    };
    let fetch_res = client
        .fetch_one(authenticated_request(fetch_request))
        .await
        .unwrap();
    let fetch_res = fetch_res.into_inner();
    assert!(fetch_res.event.is_none());

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_fetch_one_restriction() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    let query_node = QueryNode {
        node: Some(query_node::Node::All(MatchAllQueryNode {})),
    };
    let list_request = build_list_request(&query_node);

    let list_res = client
        .list(authenticated_request(list_request))
        .await
        .unwrap();
    let list_res = list_res.into_inner();

    for i in 0..data.len() {
        let source_event_tracker = list_res.streams[0].events[i].tracker.clone().unwrap();
        let fetch_request = FetchOneRequest {
            event_tracker: Some(source_event_tracker.clone()),
            restriction_query: Some(Any {
                type_url: "type.googleapis.com/queryparser_proto.QueryNode".to_string(),
                value: QueryNode {
                    node: Some(query_node::Node::All(MatchAllQueryNode {})),
                }
                .encode_to_vec(),
            }),
            org_id: 2,
            scope: Default::default(),
            index_id_patterns: Vec::new(),
        };
        let fetch_res = client
            .fetch_one(authenticated_request(fetch_request))
            .await
            .unwrap();
        let fetch_res = fetch_res.into_inner();
        assert_eq!(fetch_res.event.unwrap(), list_res.streams[0].events[i]);

        let fetch_request = FetchOneRequest {
            event_tracker: Some(source_event_tracker),
            restriction_query: Some(Any {
                type_url: "type.googleapis.com/queryparser_proto.QueryNode".to_string(),
                value: QueryNode {
                    node: Some(query_node::Node::None(MatchNoneQueryNode {})),
                }
                .encode_to_vec(),
            }),
            org_id: 2,
            scope: Default::default(),
            index_id_patterns: Vec::new(),
        };
        let fetch_res = client
            .fetch_one(authenticated_request(fetch_request))
            .await
            .unwrap();
        let fetch_res = fetch_res.into_inner();
        assert!(fetch_res.event.is_none());
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_aggregation_count() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    let query_node = QueryNode {
        node: Some(query_node::Node::All(MatchAllQueryNode {})),
    };

    let agg = agg_computes(&[agg_count("count:count")]);
    let list_request = build_aggregation_request(&query_node, agg);

    let agg_res = client
        .aggregate(authenticated_request(list_request))
        .await
        .unwrap();
    let agg_res = agg_res.into_inner();

    assert_eq!(agg_res.result.len(), 1);
    assert!(agg_res.result[0].key.is_empty());
    assert_eq!(agg_res.result[0].value.len(), 1);
    assert_eq!(
        agg_res.result[0].value[0].value.as_ref().unwrap(),
        &agg_value::Value::Uint64Value(data.len() as u64)
    );

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_aggregation_group_by_count() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    let query_node = QueryNode {
        node: Some(query_node::Node::All(MatchAllQueryNode {})),
    };

    let agg = agg_group_by(
        expression_field("status"),
        agg_computes(&[agg_count("count:count")]),
    );
    let list_request = build_aggregation_request(&query_node, agg);

    let agg_res = client
        .aggregate(authenticated_request(list_request))
        .await
        .unwrap();
    let agg_res = agg_res.into_inner();

    assert_eq!(agg_res.result.len(), 2);
    for bucket in agg_res.result {
        assert_eq!(bucket.key.len(), 1);
        assert_eq!(bucket.value.len(), 1);

        let expected_count = match &*bucket.key[0] {
            "ok" => 2,
            "error" => 1,
            other => panic!("unexpected bucket key: {other}"),
        };
        assert_eq!(
            bucket.value[0].value.as_ref().unwrap(),
            &agg_value::Value::Uint64Value(expected_count)
        );
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_aggregation_time_grouping_count() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    let query_node = QueryNode {
        node: Some(query_node::Node::All(MatchAllQueryNode {})),
    };

    let agg = agg_time_grouping(agg_computes(&[agg_count("count:count")]));
    let list_request = build_aggregation_request(&query_node, agg);

    let agg_res = client
        .aggregate(authenticated_request(list_request))
        .await
        .unwrap();
    let agg_res = agg_res.into_inner();

    assert_eq!(agg_res.result.len(), 2);
    for bucket in agg_res.result {
        assert_eq!(bucket.key.len(), 1);
        assert_eq!(bucket.value.len(), 1);

        let expected_count = match &*bucket.key[0] {
            "2025-04-08T08:12:40Z" => 2,
            "2025-04-08T08:12:50Z" => 1,
            other => panic!("unexpected bucket key: {other}"),
        };
        assert_eq!(
            bucket.value[0].value.as_ref().unwrap(),
            &agg_value::Value::Uint64Value(expected_count)
        );
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_aggregation_group_and_time_grouping_count() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    let query_node = QueryNode {
        node: Some(query_node::Node::All(MatchAllQueryNode {})),
    };

    let agg = agg_group_by(
        expression_field("status"),
        agg_time_grouping(agg_computes(&[agg_count("count:count")])),
    );
    let list_request = build_aggregation_request(&query_node, agg);

    let agg_res = client
        .aggregate(authenticated_request(list_request))
        .await
        .unwrap();
    let agg_res = dbg!(agg_res.into_inner());

    assert_eq!(agg_res.result.len(), 3);
    for bucket in agg_res.result {
        assert_eq!(bucket.key.len(), 2);
        assert_eq!(bucket.value.len(), 1);

        assert!(
            [
                vec!["ok".to_string(), "2025-04-08T08:12:40Z".to_string()],
                vec!["error".to_string(), "2025-04-08T08:12:40Z".to_string()],
                vec!["ok".to_string(), "2025-04-08T08:12:50Z".to_string()],
            ]
            .contains(&bucket.key),
            "unexpected key: {:?}",
            bucket.key
        );
        assert_eq!(
            bucket.value[0].value.as_ref().unwrap(),
            &agg_value::Value::Uint64Value(1)
        );
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_create_managed_indexes_on_startup() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::default()
        .add_node([QuickwitService::Searcher])
        .add_node([QuickwitService::Metastore])
        .add_node_with_otlp([QuickwitService::Indexer])
        .add_node_with_datadog([QuickwitService::Indexer])
        .add_node([QuickwitService::ControlPlane])
        .add_node([QuickwitService::Janitor])
        .build_and_start()
        .await;

    let indexes = sandbox
        .rest_client(QuickwitService::Indexer)
        .indexes()
        .list()
        .await
        .unwrap();

    let index_ids: Vec<IndexId> = indexes
        .into_iter()
        .map(|index_metadata| index_metadata.index_uid.index_id)
        .sorted()
        .collect();

    assert_eq!(
        index_ids,
        vec![
            "datadog",
            "datadog-metrics",
            "datadog-spans",
            "otel-logs-v0_9",
            "otel-traces-v0_9"
        ]
    );

    for index_id in index_ids {
        sandbox
            .rest_client(QuickwitService::Indexer)
            .indexes()
            .delete(&index_id, false)
            .await
            .unwrap();
    }
    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_get_indexes() {
    let mut data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&mut data).await;

    let mut client = sandbox.cloudprem_client();

    let request = GetIndexesRequest {
        org_id: 2,
        cluster_id: "test-cluster".to_string(),
    };

    let res = client
        .get_indexes(authenticated_request(request))
        .await
        .unwrap();
    let res = res.into_inner();

    // We should have at least the "datadog" index created by setup_env
    let datadog_index = res
        .indexes
        .iter()
        .find(|idx| idx.index_id == "datadog")
        .expect("datadog index should exist");

    assert!(!datadog_index.index_uri.is_empty());
    assert!(datadog_index.create_timestamp > 0);

    // Verify index_config is present
    let index_config = datadog_index
        .index_config
        .as_ref()
        .expect("index_config should be present");

    // The datadog index may or may not have a retention policy configured
    // Just verify the structure is correct
    if let Some(retention_policy) = &index_config.retention_policy {
        assert!(!retention_policy.period.is_empty());
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_extra_fts_indexing_and_search() {
    // Test that extra_fts (concatenate), error (object), and title fields work.
    // Covers: FTS via extra_fts, individual field queries, and type:object regression.
    let mut docs: Vec<Value> = serde_json::from_value(serde_json::json!([
        {
            "message": "{\"error\":{\"message\":\"connection refused by remote host\",\"stack\":\"RuntimeError at handle_request line 42\"},\"title\":\"payment service crash\"}",
            "status": "error",
            "timestamp": 1762360556800u64,
            "hostname": "test-host",
            "service": "service123",
            "ddsource": "java",
            "ddtags": "env:dev"
        },
        {
            "message": "Successfully processed payment for order_id=ORD-12345",
            "status": "info",
            "timestamp": 1762360556900u64,
            "hostname": "test-host",
            "service": "service123",
            "ddsource": "go",
            "ddtags": "env:dev"
        },
        {
            "message": "{\"error\":\"plain string error, not an object\"}",
            "status": "error",
            "timestamp": 1762360557000u64,
            "hostname": "test-host",
            "service": "service123",
            "ddsource": "python",
            "ddtags": "env:dev"
        }
    ]))
    .unwrap();

    // Verify PomChi produces ExtraFts with error object and title
    let msg: DatadogLogMsg = serde_json::from_value(docs[0].clone()).unwrap();
    let processed = pomchi::ProcessedLog::from_datadog_log_msg(msg);
    assert!(!processed.extra_fts.is_empty());
    assert_eq!(
        processed.extra_fts.error.message.as_deref(),
        Some("connection refused by remote host")
    );
    assert_eq!(
        processed.extra_fts.error.stack.as_deref(),
        Some("RuntimeError at handle_request line 42")
    );
    assert_eq!(
        processed.extra_fts.title.as_deref(),
        Some("payment service crash")
    );

    // Verify PomChi ignores error when it's a plain string (not an object)
    let msg_str_error: DatadogLogMsg = serde_json::from_value(docs[2].clone()).unwrap();
    let processed_str = pomchi::ProcessedLog::from_datadog_log_msg(msg_str_error);
    assert!(processed_str.extra_fts.error.is_empty());
    assert!(processed_str.extra_fts.title.is_none());

    let sandbox = setup_env(&mut docs).await;

    // Verify documents were indexed
    sandbox.assert_hit_count("datadog", "source:java", 1).await;
    sandbox.assert_hit_count("datadog", "source:go", 1).await;
    sandbox
        .assert_hit_count("datadog", "source:python", 1)
        .await;

    // --- FTS via extra_fts (concatenate field) ---
    // Bare text search should find words in error.message, error.stack, title
    sandbox.assert_hit_count("datadog", "refused", 1).await;
    sandbox.assert_hit_count("datadog", "crash", 1).await;
    sandbox.assert_hit_count("datadog", "RuntimeError", 1).await;

    // Bare text search should also find words in message field
    sandbox.assert_hit_count("datadog", "processed", 1).await;

    // --- Individual field queries (type:object regression test) ---
    // error.message should be queryable as an individual field
    sandbox
        .assert_hit_count("datadog", "error.message:refused", 1)
        .await;
    sandbox
        .assert_hit_count("datadog", "error.message:nonexistent", 0)
        .await;

    // error.stack should be queryable as an individual field
    sandbox
        .assert_hit_count("datadog", "error.stack:RuntimeError", 1)
        .await;

    // title should be queryable as an individual field
    sandbox.assert_hit_count("datadog", "title:crash", 1).await;
    sandbox
        .assert_hit_count("datadog", "title:nonexistent", 0)
        .await;

    // --- Negative tests ---
    sandbox
        .assert_hit_count("datadog", "nonexistentword12345", 0)
        .await;

    // error as plain string should NOT appear in error.message field
    sandbox
        .assert_hit_count("datadog", "error.message:plain", 0)
        .await;

    sandbox.shutdown().await.unwrap();
}
