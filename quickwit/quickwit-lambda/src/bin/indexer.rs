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

use std::path::PathBuf;

use lambda_runtime::{service_fn, Error, LambdaEvent};
use quickwit_lambda::{ingest, setup_lambda_tracer, IngestArgs};
use serde_json::Value;
use tracing::{error, info};

pub async fn handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let source_uri = if let Some(source_uri) = event.payload["source_uri"].as_str() {
        source_uri
    } else {
        println!("Missing source_uri");
        return Err(anyhow::anyhow!("Missing source_uri").into());
    };
    let ingest_res = ingest(IngestArgs {
        index_config_uri: std::env::var("INDEX_CONFIG_URI")?,
        index_id: std::env::var("INDEX_ID")?,
        input_path: PathBuf::from(source_uri),
        input_format: quickwit_config::SourceInputFormat::Json,
        overwrite: true,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_lambda_tracer()?;
    let func = service_fn(handler);
    lambda_runtime::run(func)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}
