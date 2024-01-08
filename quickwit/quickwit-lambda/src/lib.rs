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

use lambda_runtime::{Error, LambdaEvent};
use quickwit_cli::logger::setup_logging_and_tracing;
use quickwit_cli::tool::{
    local_ingest_docs_cli, local_search_cli, LocalIngestDocsArgs, LocalSearchArgs,
};
use quickwit_common::uri::Uri;
use quickwit_serve::BuildInfo;
use serde_json::{json, Value};
use tracing::Level;

pub fn setup_lambda_tracer() -> anyhow::Result<()> {
    setup_logging_and_tracing(Level::INFO, false, BuildInfo::get())
}

pub async fn index_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let source_uri = if let Some(source_uri) = event.payload["source_uri"].as_str() {
        source_uri
    } else {
        println!("Missing source_uri");
        return Err(anyhow::anyhow!("Missing source_uri").into());
    };
    let ingest_res = local_ingest_docs_cli(LocalIngestDocsArgs {
        clear_cache: true,
        config_uri: Uri::from_well_formed("file:///var/task/config.yaml"),
        index_id: String::from("hdfs-logs"),
        input_format: quickwit_config::SourceInputFormat::Json,
        overwrite: true,
        input_path_opt: Some(PathBuf::from(source_uri)),
        vrl_script: None,
    })
    .await;
    if let Err(e) = ingest_res {
        println!("{:?}", e);
        return Err(anyhow::anyhow!("Indexing failed").into());
    }
    Ok(json!({
        "message": format!("Hello from Quickwit {}!", BuildInfo::get().version)
    }))
}

pub async fn search_handler(_event: LambdaEvent<Value>) -> Result<Value, Error> {
    let ingest_res = local_search_cli(LocalSearchArgs {
        config_uri: Uri::from_well_formed("file:///var/task/config.yaml"),
        index_id: String::from("hdfs-logs"),
        query: String::new(),
        aggregation: None,
        max_hits: 10,
        start_offset: 0,
        search_fields: None,
        snippet_fields: None,
        start_timestamp: None,
        end_timestamp: None,
        sort_by_field: None,
    })
    .await;
    if let Err(e) = ingest_res {
        println!("{:?}", e);
        return Err(anyhow::anyhow!("Query failed").into());
    }
    Ok(json!({
        "message": format!("Hello from Quickwit {}!", BuildInfo::get().version)
    }))
}
