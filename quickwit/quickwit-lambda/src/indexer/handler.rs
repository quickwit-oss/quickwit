// Copyright (C) 2023 Quickwit, Inc.
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

use lambda_runtime::{Error, LambdaEvent};
use serde_json::Value;
use tracing::{debug, error, info};

use super::ingest::{ingest, IngestArgs};
use super::model::IndexerEvent;

pub async fn handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    debug!(payload = event.payload.to_string(), "Received event");
    let payload_res = serde_json::from_value::<IndexerEvent>(event.payload);

    if let Err(e) = payload_res {
        error!(err=?e, "Failed to parse payload");
        return Err(e.into());
    }

    let ingest_res = ingest(IngestArgs {
        index_config_uri: std::env::var("INDEX_CONFIG_URI")?,
        index_id: std::env::var("INDEX_ID")?,
        input_path: payload_res.unwrap().uri(),
        input_format: quickwit_config::SourceInputFormat::Json,
        overwrite: false,
        vrl_script: None,
        clear_cache: true,
    })
    .await;

    match ingest_res {
        Ok(stats) => {
            info!(stats=?stats, "Indexing succeeded");
            Ok(serde_json::to_value(stats)?)
        }
        Err(e) => {
            error!(err=?e, "Indexing failed");
            Err(anyhow::anyhow!("Indexing failed").into())
        }
    }
}
