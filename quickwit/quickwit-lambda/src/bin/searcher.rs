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

use quickwit_lambda::logger;
use quickwit_lambda::searcher::{searcher_api, warp_lambda};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logger::setup_lambda_tracer(tracing::Level::INFO)?;
    let routes = searcher_api().await?;
    let warp_service = warp::service(routes);
    warp_lambda::run(warp_service)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}
