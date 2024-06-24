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

use hyper::StatusCode;
use quickwit_proto::control_plane::{
    ControlPlaneResult, ControlPlaneService, ControlPlaneServiceClient, RebuildPlanRequest,
    RebuildPlanResponse,
};
use serde::{Deserialize, Serialize};
use tracing::error;
use warp::Filter;

use crate::with_arg;

#[derive(Deserialize)]
struct RebuildPlanParams {
    #[serde(default)]
    reset: bool,
}

#[derive(Serialize)]
struct RebuildPlanResp {
    previous_solution: serde_json::Value,
    problem: serde_json::Value,
    new_solution: serde_json::Value,
}

pub fn rebuild_plan_handler(
    control_plane_client: ControlPlaneServiceClient,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("rebuild-plan")
        .and(warp::get())
        .and(with_arg(control_plane_client))
        .and(warp::query::<RebuildPlanParams>())
        .then(
            |control_plane_client: ControlPlaneServiceClient,
             rebuild_plan_params: RebuildPlanParams| async move {
                let rebuild_plan_request = RebuildPlanRequest {
                    reset: rebuild_plan_params.reset,
                    debug: true,
                };
                let rebuild_plan_result: ControlPlaneResult<RebuildPlanResponse> =
                    control_plane_client
                        .rebuild_plan(rebuild_plan_request)
                        .await;
                match rebuild_plan_result {
                    Ok(RebuildPlanResponse {
                        previous_solution_json,
                        problem_json,
                        new_solution_json,
                    }) => {
                        let rebuild_plan_resp = RebuildPlanResp {
                            previous_solution: serde_json::from_str(&previous_solution_json)
                                .unwrap_or(serde_json::Value::Null),
                            problem: serde_json::from_str(&problem_json)
                                .unwrap_or(serde_json::Value::Null),
                            new_solution: serde_json::from_str(&new_solution_json)
                                .unwrap_or(serde_json::Value::Null),
                        };
                        warp::reply::with_status(
                            warp::reply::json(&rebuild_plan_resp),
                            StatusCode::OK,
                        )
                    }
                    Err(err) => {
                        error!("control plane error on rebuild plane: {:?}", err);
                        warp::reply::with_status(
                            warp::reply::json(
                                &serde_json::json!({"error_msg": "error rebuilding plan"}),
                            ),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        )
                    }
                }
            },
        )
}
