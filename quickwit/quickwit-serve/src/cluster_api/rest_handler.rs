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

use std::convert::Infallible;

use quickwit_cluster::{Cluster, ClusterSnapshot, NodeIdSchema};
use warp::{Filter, Rejection};

use crate::format::extract_format_from_qs;
use crate::rest::recover_fn;
use crate::rest_api_response::into_rest_api_response;

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(get_cluster),
    components(schemas(ClusterSnapshot, NodeIdSchema,))
)]
pub struct ClusterApi;

/// Cluster handler.
pub fn cluster_handler(
    cluster: Cluster,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("cluster")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::path::end().map(move || cluster.clone()))
        .then(get_cluster)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .recover(recover_fn)
        .boxed()
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
async fn get_cluster(cluster: Cluster) -> Result<ClusterSnapshot, Infallible> {
    let snapshot = cluster.snapshot().await;
    Ok(snapshot)
}
