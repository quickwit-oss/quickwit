// Copyright (C) 2024 Quickwit, Inc.
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

use quickwit_proto::control_plane::{
    ControlPlaneService, ControlPlaneServiceClient, GetDebugStateRequest,
};

#[derive(utoipa::OpenApi)]
#[openapi(paths(debugging_handler))]
/// Endpoints which are weirdly tied to another crate with no
/// other bits of information attached.
///
/// If a crate plans to encompass different schemas, handlers, etc...
/// Then it should have it's own specific API group.
pub struct DebugApi;

#[utoipa::path(
    get,
    tag = "Get debug information for node",
    path = "/",
    responses(
        (status = 200, description = "Successfully fetched debugging info.", body = GetDebugStateRequestResponse),
    ),
)]
/// Get Node debug information.
///
/// The format is not guaranteed to ever be stable, and is meant to provide some introspection to
/// help with debugging.
pub async fn debugging_handler(
    mut control_plane_service_client: ControlPlaneServiceClient,
) -> impl warp::Reply {
    let debug_info = control_plane_service_client
        .get_debug_state(GetDebugStateRequest {})
        .await;
    crate::json_api_response::JsonApiResponse::new(
        &debug_info,
        // TODO error code on error
        hyper::StatusCode::OK,
        &crate::format::BodyFormat::PrettyJson,
    )
}
