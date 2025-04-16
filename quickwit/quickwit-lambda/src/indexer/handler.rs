// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use lambda_runtime::{Error, LambdaEvent};
use serde_json::Value;
use tracing::{Instrument, debug_span, error, info, info_span};

use super::environment::{DISABLE_JANITOR, DISABLE_MERGE, INDEX_CONFIG_URI};
use super::ingest::{IngestArgs, ingest};
use super::model::IndexerEvent;
use crate::environment::INDEX_ID;
use crate::logger;
use crate::utils::LambdaContainerContext;

async fn indexer_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let container_ctx = LambdaContainerContext::load();
    let memory = event.context.env_config.memory;
    let payload = serde_json::from_value::<IndexerEvent>(event.payload)?;

    let ingest_res = ingest(IngestArgs {
        input_path: payload.uri()?,
        input_format: quickwit_config::SourceInputFormat::Json,
        vrl_script: None,
        // TODO: instead of clearing the cache, we use a cache and set its max
        // size with indexer_config.split_store_max_num_bytes
        clear_cache: true,
    })
    .instrument(debug_span!(
        "ingest",
        memory,
        env.INDEX_CONFIG_URI = *INDEX_CONFIG_URI,
        env.INDEX_ID = *INDEX_ID,
        env.DISABLE_MERGE = *DISABLE_MERGE,
        env.DISABLE_JANITOR = *DISABLE_JANITOR,
        cold = container_ctx.cold,
        container_id = container_ctx.container_id,
    ))
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
