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

use quickwit_cluster::{Cluster, ClusterState};
use serde::Deserialize;
use warp::{Filter, Rejection};

use crate::Format;

/// Cluster handler.
pub fn cluster_handler(
    cluster: Arc<Cluster>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    cluster_state_filter()
        .and(warp::path::end().map(move || cluster.clone()))
        .and_then(get_cluster)
}

/// This struct represents the QueryString passed to
/// the rest API.
#[derive(Deserialize, Debug, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct ClusterStateQueryString {
    /// The output format requested.
    #[serde(default)]
    pub format: Format,
}

fn cluster_state_filter(
) -> impl Filter<Extract = (ClusterStateQueryString,), Error = Rejection> + Clone {
    warp::path!("cluster")
        .and(warp::path::end())
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

async fn get_cluster(
    request: ClusterStateQueryString,
    cluster: Arc<Cluster>,
) -> Result<impl warp::Reply, Infallible> {
    Ok(request
        .format
        .make_rest_reply_non_serializable_error(cluster_endpoint(cluster).await))
}

async fn cluster_endpoint(cluster: Arc<Cluster>) -> Result<ClusterState, Infallible> {
    Ok(cluster.state().await)
}
