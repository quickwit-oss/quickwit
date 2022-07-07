// Copyright (C) 2022 Quickwit, Inc.
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

use std::sync::Arc;

use quickwit_common::uri::Uri;
use quickwit_config::QuickwitConfig;
use warp::{Filter, Rejection};

use crate::{with_arg, QuickwitBuildInfo};

pub fn node_info_handler(
    build_info: Arc<QuickwitBuildInfo>,
    config: Arc<QuickwitConfig>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    node_version_handler(build_info).or(node_config_handler(config))
}

fn node_version_handler(
    build_info: Arc<QuickwitBuildInfo>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path("version")
        .and(warp::path::end())
        .and(with_arg(build_info))
        .and_then(get_version)
}

async fn get_version(build_info: Arc<QuickwitBuildInfo>) -> Result<impl warp::Reply, Rejection> {
    Ok(warp::reply::json(&*build_info))
}

fn node_config_handler(
    config: Arc<QuickwitConfig>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path("config")
        .and(warp::path::end())
        .and(with_arg(config))
        .and_then(get_config)
}

async fn get_config(config: Arc<QuickwitConfig>) -> Result<impl warp::Reply, Rejection> {
    // We need to hide sensitive information from metastore URI.
    let mut config_to_serialize = (*config).clone();
    config_to_serialize.metastore_uri = config_to_serialize
        .metastore_uri
        .map(|uri| uri.as_redacted_str().to_string())
        .map(Uri::new);
    Ok(warp::reply::json(&config_to_serialize))
}

#[cfg(test)]
mod tests {
    use assert_json_diff::assert_json_include;

    use super::*;
    use crate::recover_fn;

    #[tokio::test]
    async fn test_rest_node_info() -> anyhow::Result<()> {
        let build_info = QuickwitBuildInfo {
            commit_version_tag: "commit_version_tag",
            cargo_pkg_version: "cargo_pkg_version",
            cargo_build_target: "cargo_build_target",
            commit_short_hash: "commit_short_hash",
            commit_date: "commit_date",
            version: "version",
        };
        let mut config = QuickwitConfig::default();
        config.metastore_uri = Some(Uri::new("postgresql://username:password@db".to_string()));
        let handler =
            super::node_info_handler(Arc::new(build_info.clone()), Arc::new(config.clone()))
                .recover(recover_fn);
        let resp = warp::test::request().path("/version").reply(&handler).await;
        assert_eq!(resp.status(), 200);
        let resp_build_info_json: serde_json::Value = serde_json::from_slice(resp.body())?;
        let expected_build_json = serde_json::json!({
            "commit_date": "commit_date",
            "version": "version",
        });
        assert_json_include!(actual: resp_build_info_json, expected: expected_build_json);

        let resp = warp::test::request().path("/config").reply(&handler).await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!({
            "node_id": config.node_id,
            "metastore_uri": "postgresql://username:***redacted***@db",
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);

        Ok(())
    }
}
