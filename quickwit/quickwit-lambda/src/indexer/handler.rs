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

use std::sync::atomic::Ordering::SeqCst;

use lambda_runtime::{Error, LambdaEvent};
use serde_json::Value;
use tracing::{debug, error, info, span, Instrument, Level};

use super::environment::{DISABLE_MERGE, INDEX_CONFIG_URI, INDEX_ID};
use super::ingest::{ingest, IngestArgs};
use super::model::IndexerEvent;
use crate::logger;
use crate::utils::ALREADY_EXECUTED;

async fn indexer_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    debug!(payload = event.payload.to_string(), "Handler start");
    let payload_res = serde_json::from_value::<IndexerEvent>(event.payload);

    if let Err(e) = payload_res {
        error!(err=?e, "Failed to parse payload");
        return Err(e.into());
    }

    let ingest_res = ingest(IngestArgs {
        input_path: payload_res.unwrap().uri(),
        input_format: quickwit_config::SourceInputFormat::Json,
        overwrite: false,
        vrl_script: None,
        clear_cache: true,
    })
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
    let cold = !ALREADY_EXECUTED.swap(true, SeqCst);
    let memory = event.context.env_config.memory;
    let result = indexer_handler(event)
        .instrument(span!(
            parent: &span!(Level::INFO, "indexer_handler", cold),
            Level::TRACE,
            logger::RUNTIME_CONTEXT_SPAN,
            memory,
            env.INDEX_CONFIG_URI = *INDEX_CONFIG_URI,
            env.INDEX_ID = *INDEX_ID,
            env.DISABLE_MERGE = *DISABLE_MERGE
        ))
        .await;
    logger::flush_tracer();
    result
}
