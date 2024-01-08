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
use tracing::{debug_span, error, info, info_span, Instrument};

use super::environment::{DISABLE_MERGE, INDEX_CONFIG_URI, INDEX_ID};
use super::ingest::{ingest, IngestArgs};
use super::model::IndexerEvent;
use crate::logger;
use crate::utils::LambdaContainerContext;

async fn indexer_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let container_ctx = LambdaContainerContext::load();
    let memory = event.context.env_config.memory;
    let payload = serde_json::from_value::<IndexerEvent>(event.payload)?;

    let ingest_res = ingest(IngestArgs {
        input_path: payload.uri(),
        input_format: quickwit_config::SourceInputFormat::Json,
        overwrite: false,
        vrl_script: None,
        clear_cache: true,
    })
    .instrument(debug_span!(
        "ingest",
        memory,
        env.INDEX_CONFIG_URI = *INDEX_CONFIG_URI,
        env.INDEX_ID = *INDEX_ID,
        env.DISABLE_MERGE = *DISABLE_MERGE,
        cold = container_ctx.cold,
        container_id = container_ctx.container_id,
    ))
    .await;

    let result = match ingest_res {
        Ok(stats) => {
            info!(stats=?stats, "Indexing succeeded");
            Ok(serde_json::to_value(stats)?)
        }
        Err(e) => {
            error!(err=?e, "Indexing failed");
            Err(anyhow::anyhow!("Indexing failed").into())
        }
    };
    result
}

pub async fn handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let request_id = event.context.request_id.clone();
    let mut response = indexer_handler(event)
        .instrument(info_span!("indexer_handler", request_id))
        .await;
    if let Err(e) = &response {
        error!(err=?e, "Handler failed");
    }
    if let Ok(Value::Object(ref mut map)) = response {
        map.insert("request_id".to_string(), Value::String(request_id));
    }
    logger::flush_tracer();
    response
}
