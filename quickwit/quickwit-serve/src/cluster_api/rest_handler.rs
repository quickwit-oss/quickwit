// Copyright (C) 2023 Quickwit, Inc.
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

use quickwit_cluster::{Cluster, ClusterSnapshot, NodeIdSchema};
use warp::{Filter, Rejection};

use crate::format::{extract_format_from_qs, make_response};

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(get_cluster),
    components(schemas(ClusterSnapshot, NodeIdSchema,))
)]
pub struct ClusterApi;

/// Cluster handler.
pub fn cluster_handler(
    cluster: Arc<Cluster>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("cluster")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::path::end().map(move || cluster.clone()))
        .then(get_cluster)
        .and(extract_format_from_qs())
        .map(make_response)
}

#[utoipa::path(
    get,
    tag = "Cluster Info",
    path = "/cluster",
    responses(
        (status = 200, description = "Successfully fetched cluster information.", body = ClusterSnapshot)
    )
)]

/// Get cluster information.
async fn get_cluster(cluster: Arc<Cluster>) -> Result<ClusterSnapshot, Infallible> {
    let snapshot = cluster.snapshot().await;
    Ok(snapshot)
}
