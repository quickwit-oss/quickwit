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

use quickwit_config::NodeConfig;
use serde_json::json;
use warp::{Filter, Rejection};

use crate::rest::recover_fn;
use crate::{BuildInfo, RuntimeInfo, with_arg};

#[derive(utoipa::OpenApi)]
#[openapi(paths(node_version_handler, node_config_handler,))]
pub struct NodeInfoApi;

pub fn node_info_handler(
    build_info: &'static BuildInfo,
    runtime_info: &'static RuntimeInfo,
    config: Arc<NodeConfig>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    node_version_handler(build_info, runtime_info)
        .or(node_config_handler(config))
        .recover(recover_fn)
        .boxed()
}

#[utoipa::path(get, tag = "Node Info", path = "/version")]
fn node_version_handler(
    build_info: &'static BuildInfo,
    runtime_info: &'static RuntimeInfo,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path("version")
        .and(warp::path::end())
        .and(with_arg(build_info))
        .and(with_arg(runtime_info))
        .then(get_version)
}

async fn get_version(
    build_info: &'static BuildInfo,
    runtime_info: &'static RuntimeInfo,
) -> impl warp::Reply {
    warp::reply::json(&json!({
        "build": build_info,
        "runtime": runtime_info,
    }))
}

#[utoipa::path(get, tag = "Node Info", path = "/config")]
fn node_config_handler(
    config: Arc<NodeConfig>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path("config")
        .and(warp::path::end())
        .and(with_arg(config))
        .then(get_config)
}

async fn get_config(config: Arc<NodeConfig>) -> impl warp::Reply {
    // We must redact sensitive information such as credentials.
    let mut config = (*config).clone();
    config.redact();
    warp::reply::json(&config)
}

#[cfg(test)]
mod tests {
    use assert_json_diff::assert_json_include;
    use quickwit_common::uri::Uri;
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::recover_fn;

    #[tokio::test]
    async fn test_rest_node_info() {
        let build_info = BuildInfo::get();
        let runtime_info = RuntimeInfo::get();
        let mut config = NodeConfig::for_test();
        config.metastore_uri = Uri::for_test("postgresql://username:password@db");
        let handler = node_info_handler(build_info, runtime_info, Arc::new(config.clone()))
            .recover(recover_fn);
        let resp = warp::test::request().path("/version").reply(&handler).await;
        assert_eq!(resp.status(), 200);
        let info_json: JsonValue = serde_json::from_slice(resp.body()).unwrap();
        let build_info_json = info_json.get("build").unwrap();
        let expected_build_info_json = serde_json::json!({
            "commit_date": build_info.commit_date,
            "version": build_info.version,
        });
        assert_json_include!(actual: build_info_json, expected: expected_build_info_json);

        let runtime_info_json = info_json.get("runtime").unwrap();
        let expected_runtime_info_json = serde_json::json!({
            "num_cpus": runtime_info.num_cpus,
        });
        assert_json_include!(
            actual: runtime_info_json,
            expected: expected_runtime_info_json
        );

        let resp = warp::test::request().path("/config").reply(&handler).await;
        assert_eq!(resp.status(), 200);
        let resp_json: JsonValue = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!({
            "node_id": config.node_id,
            "metastore_uri": "postgresql://username:***redacted***@db",
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);
    }
}
