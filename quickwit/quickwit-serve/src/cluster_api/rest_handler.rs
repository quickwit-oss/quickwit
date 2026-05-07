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
use quickwit_proto::control_plane::{
    ControlPlaneError, ControlPlaneService, ControlPlaneServiceClient,
    DisableMaintenanceModeRequest, EnableMaintenanceModeRequest, EnableMaintenanceModeResponse,
    GetMaintenanceModeRequest, GetMaintenanceModeResponse,
};
use warp::{Filter, Rejection};

use crate::format::extract_format_from_qs;
use crate::rest::recover_fn;
use crate::rest_api_response::into_rest_api_response;

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        get_cluster,
        get_maintenance_endpoint,
        enable_maintenance_endpoint,
        disable_maintenance_endpoint
    ),
    components(schemas(
        ClusterSnapshot,
        NodeIdSchema,
        GetMaintenanceModeResponse,
        EnableMaintenanceModeResponse
    ))
)]
pub struct ClusterApi;

/// Cluster handler.
pub fn cluster_handler(
    cluster: Cluster,
    control_plane_client: ControlPlaneServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    let cluster_info_handler = warp::path!("cluster")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::path::end().map(move || cluster.clone()))
        .then(get_cluster)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed();

    let maintenance_routes = maintenance_handler(control_plane_client);

    cluster_info_handler
        .or(maintenance_routes)
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

#[utoipa::path(
    get,
    tag = "Cluster Info",
    path = "/cluster/maintenance",
    responses(
        (status = 200, description = "Successfully fetched maintenance mode status.", body = GetMaintenanceModeResponse)
    )
)]
async fn get_maintenance_endpoint(
    control_plane_client: ControlPlaneServiceClient,
) -> Result<GetMaintenanceModeResponse, ControlPlaneError> {
    control_plane_client
        .get_maintenance_mode(GetMaintenanceModeRequest {})
        .await
}

#[utoipa::path(
    put,
    tag = "Cluster Info",
    path = "/cluster/maintenance",
    responses(
        (status = 200, description = "Successfully enabled maintenance mode.", body = EnableMaintenanceModeResponse)
    )
)]
async fn enable_maintenance_endpoint(
    control_plane_client: ControlPlaneServiceClient,
) -> Result<EnableMaintenanceModeResponse, ControlPlaneError> {
    control_plane_client
        .enable_maintenance_mode(EnableMaintenanceModeRequest {})
        .await
}

#[utoipa::path(
    delete,
    tag = "Cluster Info",
    path = "/cluster/maintenance",
    responses(
        (status = 200, description = "Successfully disabled maintenance mode.")
    )
)]
async fn disable_maintenance_endpoint(
    control_plane_client: ControlPlaneServiceClient,
) -> Result<(), ControlPlaneError> {
    control_plane_client
        .disable_maintenance_mode(DisableMaintenanceModeRequest {})
        .await?;
    Ok(())
}

fn maintenance_get_filter() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path!("cluster" / "maintenance").and(warp::get())
}

fn maintenance_put_filter() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path!("cluster" / "maintenance").and(warp::put())
}

fn maintenance_delete_filter() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path!("cluster" / "maintenance").and(warp::delete())
}

/// Maintenance mode endpoints handler.
///
/// - `GET /api/v1/cluster/maintenance` — get maintenance status
/// - `PUT /api/v1/cluster/maintenance` — enable maintenance mode
/// - `DELETE /api/v1/cluster/maintenance` — disable maintenance mode
fn maintenance_handler(
    control_plane_client: ControlPlaneServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    let get_client = control_plane_client.clone();
    let put_client = control_plane_client.clone();
    let delete_client = control_plane_client;

    let get_handler = maintenance_get_filter()
        .and(warp::any().map(move || get_client.clone()))
        .then(get_maintenance_endpoint)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed();

    let put_handler = maintenance_put_filter()
        .and(warp::any().map(move || put_client.clone()))
        .then(enable_maintenance_endpoint)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed();

    let delete_handler = maintenance_delete_filter()
        .and(warp::any().map(move || delete_client.clone()))
        .then(disable_maintenance_endpoint)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed();

    get_handler.or(put_handler).or(delete_handler).boxed()
}
