use std::time::Duration;

use prost::Message;
use prost_types::Any;
use quickwit_config::service::QuickwitService;
use quickwit_datetime::{parse_date_time_str, DateTimeInputFormat};
use quickwit_proto::cloudprem::*;
use serde_json::Value;
use tonic::Request;

use crate::test_utils::{ClusterSandbox, ClusterSandboxBuilder};

const TEST_CERT: &[u8] = include_bytes!("../../test_data/test_cert_main_ca.crt");
const TEST_DATA: &[u8] = include_bytes!("../../test_data/test_data.json");

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
        // TODO check in staging what is sent
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
        org_id: 2,
    }
}

// assert two json are equal, ignoring some known issues
#[track_caller]
fn assert_eq_fuzzy(left: &Value, right: &Value) {
    let mut left = left.clone();
    let mut right = right.clone();
    // this get converted from integer to date string
    left.as_object_mut().unwrap().remove("discovery_timestamp");
    right.as_object_mut().unwrap().remove("discovery_timestamp");
    // we don't store these?
    right
        .as_object_mut()
        .unwrap()
        .remove("ingest_size_in_bytes");
    right.as_object_mut().unwrap().remove("error");
    assert_eq!(left, right);
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
async fn test_list() {
    let data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&data).await;

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
        assert_eq_fuzzy(&parse_res(i), doc);
        assert_eq!(
            event_tracker(i).id,
            data[i].get("id").unwrap().as_str().unwrap()
        );
        let timestamp_str = doc.get("timestamp").unwrap().as_str().unwrap();
        let timestamp_ms = parse_date_time_str(timestamp_str, &[DateTimeInputFormat::Rfc3339])
            .unwrap()
            .into_timestamp_millis() as u64;
        assert_eq!(event_tracker(i).epoch_ms, timestamp_ms);
    }

    sandbox.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_fetch_one() {
    let data: Vec<Value> = serde_json::from_slice(TEST_DATA).unwrap();
    let sandbox = setup_env(&data).await;

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
            org_id: 2,
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
            org_id: 2,
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

// TODO test search after and aggregations
