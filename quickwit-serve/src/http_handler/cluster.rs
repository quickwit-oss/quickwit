/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::convert::Infallible;
use std::sync::Arc;

use warp::hyper::header::CONTENT_TYPE;
use warp::hyper::StatusCode;
use warp::reply;
use warp::Filter;
use warp::Rejection;
use warp::Reply;

use quickwit_cluster::service::ClusterService;

use crate::ApiError;

/// cluster handler.
pub fn cluster_handler<TClusterService: ClusterService>(
    cluster_service: Arc<TClusterService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    members_filter()
        .and(warp::any().map(move || cluster_service.clone()))
        .and_then(members)
}

fn members_filter() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path!("cluster" / "members").and(warp::get())
}

async fn members<TClusterService: ClusterService>(
    cluster_service: Arc<TClusterService>,
) -> Result<impl warp::Reply, Infallible> {
    Ok(make_reply(members_endpoint(&*cluster_service).await))
}

async fn members_endpoint<TClusterService: ClusterService>(
    cluster_service: &TClusterService,
) -> Result<quickwit_proto::MembersResult, ApiError> {
    let members_request = quickwit_proto::MembersRequest {};
    let members_result = cluster_service.members(members_request).await?;
    Ok(members_result)
}

fn make_reply<T: serde::Serialize>(result: Result<T, ApiError>) -> impl Reply {
    let status_code: StatusCode;
    let body_json = match result {
        Ok(success) => {
            status_code = StatusCode::OK;
            serde_json::to_string(&success)
        }
        Err(err) => {
            status_code = err.http_status_code();
            serde_json::to_string(&err)
        }
    }
    .unwrap_or_else(|_| {
        tracing::error!("Error: the response serialization failed.");
        "Error: Failed to serialize response.".to_string()
    });
    let reply_with_header = reply::with_header(body_json, CONTENT_TYPE, "application/json");
    reply::with_status(reply_with_header, status_code)
}
