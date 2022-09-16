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

use std::convert::Infallible;
use std::sync::Arc;

use quickwit_cluster::Cluster;
use warp::hyper::StatusCode;
use warp::reply::with_status;
use warp::{Filter, Rejection};

use crate::with_arg;

/// Health check handlers.
pub fn health_check_handlers(
    cluster: Arc<Cluster>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    liveness_handler().or(readyness_handler(cluster))
}

pub fn liveness_handler() -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("health" / "livez")
        .and(warp::path::end())
        .and(warp::get())
        .and_then(get_liveness)
}

pub fn readyness_handler(
    cluster: Arc<Cluster>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    warp::path!("health" / "readyz")
        .and(warp::path::end())
        .and(warp::get())
        .and(with_arg(cluster))
        .and_then(get_readyness)
}

async fn get_liveness() -> Result<impl warp::Reply, Infallible> {
    Ok(with_status(warp::reply::json(&true), StatusCode::OK))
}

async fn get_readyness(cluster: Arc<Cluster>) -> Result<impl warp::Reply, Infallible> {
    let is_ready = cluster.is_self_node_ready().await;
    let status_code = if is_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    Ok(with_status(warp::reply::json(&is_ready), status_code))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chitchat::transport::ChannelTransport;
    use quickwit_cluster::create_cluster_for_test;

    #[tokio::test]
    async fn test_rest_search_api_health_checks() {
        let transport = ChannelTransport::default();
        let cluster = Arc::new(
            create_cluster_for_test(Vec::new(), &[], &transport, false)
                .await
                .unwrap(),
        );
        let health_check_handler = super::health_check_handlers(cluster.clone());
        let resp = warp::test::request()
            .path("/health/livez")
            .reply(&health_check_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp = warp::test::request()
            .path("/health/readyz")
            .reply(&health_check_handler)
            .await;
        assert_eq!(resp.status(), 503);
        cluster.set_self_node_ready(true).await;
        let resp = warp::test::request()
            .path("/health/readyz")
            .reply(&health_check_handler)
            .await;
        assert_eq!(resp.status(), 200);
    }
}
