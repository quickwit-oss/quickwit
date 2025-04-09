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

use quickwit_config::service::QuickwitService;
use quickwit_serve::SearchRequestQueryString;

use crate::test_utils::{filter_metrics, parse_prometheus_metrics, ClusterSandboxBuilder};

#[tokio::test]
async fn test_metrics_standalone_server() {
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let client = sandbox.rest_client(QuickwitService::Indexer);

    client
        .indexes()
        .create(
            r#"
                version: 0.8
                index_id: my-new-index
                doc_mapping:
                  field_mappings:
                  - name: body
                    type: text
                "#,
            quickwit_config::ConfigFormat::Yaml,
            false,
        )
        .await
        .unwrap();

    assert_eq!(
        client
            .search(
                "my-new-index",
                SearchRequestQueryString {
                    query: "body:test".to_string(),
                    max_hits: 10,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .num_hits,
        0
    );

    let prometheus_url = format!("{}metrics", client.base_url());
    let response = reqwest::Client::new()
        .get(&prometheus_url)
        .send()
        .await
        .expect("Failed to send request");

    assert!(
        response.status().is_success(),
        "Request failed with status {}",
        response.status(),
    );

    let body = response.text().await.expect("Failed to read response body");
    // println!("Prometheus metrics:\n{}", body);
    let metrics = parse_prometheus_metrics(&body);
    // The assertions validate some very specific metrics. Feel free to add more as needed.
    {
        let filtered_metrics = filter_metrics(
            &metrics,
            "quickwit_http_requests_total",
            vec![("method", "GET")],
        );
        assert_eq!(filtered_metrics.len(), 1);
        // we don't know exactly how many GET requests to expect as they are used to
        // poll the node state
        assert!(filtered_metrics[0].metric_value > 0.0);
    }
    {
        let filtered_metrics = filter_metrics(
            &metrics,
            "quickwit_http_requests_total",
            vec![("method", "POST")],
        );
        assert_eq!(filtered_metrics.len(), 1);
        // 2 POST requests: create index + search
        assert_eq!(filtered_metrics[0].metric_value, 2.0);
    }
    {
        let filtered_metrics = filter_metrics(
            &metrics,
            "quickwit_search_root_search_requests_total",
            vec![],
        );
        assert_eq!(filtered_metrics.len(), 1);
        assert_eq!(filtered_metrics[0].metric_value, 1.0);
        assert_eq!(filtered_metrics[0].labels.get("status").unwrap(), "success");
    }
    sandbox.shutdown().await.unwrap();
}
