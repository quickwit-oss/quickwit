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

use serde::Deserialize;
use warp::Filter;
use warp::Rejection;

use quickwit_cluster::service::ClusterService;

use crate::rest::Format;
use crate::ApiError;

/// This struct represents the QueryString passed to
/// the rest API.
#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MembersRequestQueryString {
    /// The output format.
    #[serde(default)]
    pub format: Format,
}

/// cluster handler.
pub fn cluster_handler<TClusterService: ClusterService>(
    cluster_service: Arc<TClusterService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    members_filter()
        .and(warp::any().map(move || cluster_service.clone()))
        .and_then(members)
}

fn members_filter() -> impl Filter<Extract = (MembersRequestQueryString,), Error = Rejection> + Clone
{
    warp::path!("cluster" / "members")
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

async fn members<TClusterService: ClusterService>(
    request: MembersRequestQueryString,
    cluster_service: Arc<TClusterService>,
) -> Result<impl warp::Reply, Infallible> {
    Ok(request
        .format
        .make_reply(members_endpoint(&*cluster_service).await))
}

async fn members_endpoint<TClusterService: ClusterService>(
    cluster_service: &TClusterService,
) -> Result<quickwit_proto::MembersResult, ApiError> {
    let members_request = quickwit_proto::MembersRequest {};
    let members_result = cluster_service.members(members_request).await?;
    Ok(members_result)
}
