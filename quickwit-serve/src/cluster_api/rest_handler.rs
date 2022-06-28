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

use quickwit_cluster::{ClusterError, ClusterService};
use serde::Deserialize;
use warp::{Filter, Rejection};

use crate::Format;

/// Cluster handler.
pub fn cluster_handler(
    cluster_service: Arc<dyn ClusterService>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    list_members_filter()
        .and(warp::path::end().map(move || cluster_service.clone()))
        .and_then(list_members)
}

/// This struct represents the QueryString passed to
/// the rest API.
#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ListMembersRequestQueryString {
    /// The output format requested.
    #[serde(default)]
    pub format: Format,
}

fn list_members_filter(
) -> impl Filter<Extract = (ListMembersRequestQueryString,), Error = Rejection> + Clone {
    warp::path!("cluster" / "members")
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

async fn list_members(
    request: ListMembersRequestQueryString,
    cluster_service: Arc<dyn ClusterService>,
) -> Result<impl warp::Reply, Infallible> {
    Ok(request
        .format
        .make_rest_reply(list_members_endpoint(&*cluster_service).await))
}

async fn list_members_endpoint(
    cluster_service: &dyn ClusterService,
) -> Result<quickwit_proto::ListMembersResponse, ClusterError> {
    let list_members_req = quickwit_proto::ListMembersRequest {};
    let list_members_resp = cluster_service.list_members(list_members_req).await?;
    Ok(list_members_resp)
}
