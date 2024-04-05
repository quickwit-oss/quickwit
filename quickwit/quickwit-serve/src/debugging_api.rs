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

use std::sync::Arc;

use quickwit_ingest::Ingester;
use quickwit_proto::control_plane::{
    ControlPlaneService, ControlPlaneServiceClient, GetDebugStateRequest,
};
use warp::{Filter, Rejection};

use crate::{with_arg, QuickwitServices};

#[derive(utoipa::OpenApi)]
#[openapi(paths(control_plane_debugging_handler, mrecordlog_debugging_handler,))]
/// Endpoints which are weirdly tied to another crate with no
/// other bits of information attached.
///
/// If a crate plans to encompass different schemas, handlers, etc...
/// Then it should have it's own specific API group.
pub struct DebugApi;

pub(crate) fn debugging_routes(
    quickwit_services: Arc<QuickwitServices>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    let debugging_routes = warp::path!("api" / "debugging" / ..);

    debugging_routes.and(
        control_plane_debugging_handler(quickwit_services.control_plane_service.clone()).or(
            mrecordlog_debugging_handler(quickwit_services.ingester_opt.clone()),
        ),
    )
}

#[utoipa::path(
    get,
    tag = "Get debug informations about the control plane from the node viewpoint",
    path = "/control_plane",
    responses(
        (status = 200, description = "Successfully fetched debugging info.", body = GetDebugStateRequestResponse),
    ),
)]
/// Get control plane related debug information.
///
/// The format is not guaranteed to ever be stable, and is meant to provide some introspection to
/// help with debugging.
pub fn control_plane_debugging_handler(
    control_plane_service_client: ControlPlaneServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path("control_plane")
        .and(warp::path::end())
        .and(with_arg(control_plane_service_client))
        .then(
            |mut control_plane_service_client: ControlPlaneServiceClient| async move {
                let debug_info = control_plane_service_client
                    .get_debug_state(GetDebugStateRequest {})
                    .await;
                crate::rest_api_response::RestApiResponse::new(
                    &debug_info,
                    // TODO error code on error
                    hyper::StatusCode::OK,
                    crate::format::BodyFormat::PrettyJson,
                )
            },
        )
}

#[utoipa::path(
    get,
    tag = "Get debug informations about the mrecordlog of the node",
    path = "/mrecordlog",
    responses(
        (status = 200, description = "Successfully fetched debugging info.", body = MRecordlogSummaryResponse),
    ),
)]
/// Get mrecordlog related debug information information.
///
/// The format is not guaranteed to ever be stable, and is meant to provide some introspection to
/// help with debugging.
pub fn mrecordlog_debugging_handler(
    ingester_opt: Option<Ingester>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path("mrecordlog")
        .and(warp::path::end())
        .and(with_arg(ingester_opt))
        .then(|ingester_opt: Option<Ingester>| async move {
            let Some(mut ingester) = ingester_opt else {
                return crate::rest_api_response::RestApiResponse::new(
                    &Result::<&str, &str>::Err("ingester disabled"),
                    hyper::StatusCode::MISDIRECTED_REQUEST,
                    crate::format::BodyFormat::PrettyJson,
                );
            };
            let debug_info = ingester.mrecordlog_summary().await;
            crate::rest_api_response::RestApiResponse::new(
                &debug_info,
                // TODO error code on error
                hyper::StatusCode::OK,
                crate::format::BodyFormat::PrettyJson,
            )
        })
}
