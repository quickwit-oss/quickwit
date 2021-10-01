// Copyright (C) 2021 Quickwit, Inc.
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

use std::net::SocketAddr;

use quickwit_common::metrics;
use tracing::info;
use warp::Filter;

/// Start REST service given a HTTP address and a search service.
pub async fn start_rest_service(
    rest_addr: SocketAddr,
) -> anyhow::Result<()> {
    info!(rest_addr=?rest_addr, "Starting REST service.");
    let metrics_service = warp::path("metrics")
        .and(warp::get())
        .map(metrics::metrics_handler);
    warp::serve(metrics_service).run(rest_addr).await;
    Ok(())
}
