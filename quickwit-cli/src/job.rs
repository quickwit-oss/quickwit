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

use std::path::PathBuf;

use anyhow::{bail, Context};
use byte_unit::Byte;
use clap::{value_t, ArgMatches};
use quickwit_actors::{ActorExitStatus, Universe};
use quickwit_indexing::actors::{
    IndexerParams, IndexingPipelineParams, IndexingPipelineSupervisor,
};
use quickwit_indexing::models::{CommitPolicy, IndexingDirectory};
use quickwit_indexing::source::SourceConfig;
use quickwit_metastore::MetastoreUriResolver;
use quickwit_storage::quickwit_storage_uri_resolver;
use serde_json::json;
use tracing::debug;

#[derive(Debug, Eq, PartialEq)]
pub struct DemuxJobArgs {
    metastore_uri: String,
    index_id: String,
    data_dir_path: PathBuf,
    demux_factor: usize,
}

impl DemuxJobArgs {
    pub fn new(
        metastore_uri: String,
        index_id: String,
        data_dir_path: PathBuf,
        demux_factor: usize,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            metastore_uri,
            index_id,
            data_dir_path,
            demux_factor,
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum JobCliSubCommand {
    DemuxJob(DemuxJobArgs),
}

impl JobCliSubCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches_opt) = matches.subcommand();
        let submatches =
            submatches_opt.ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "demux" => Self::parse_demux_job_args(submatches),
            _ => bail!("Job subcommand '{}' is not implemented", subcommand),
        }
    }

    fn parse_demux_job_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg")?
            .to_string();
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(|metastore_uri_str| metastore_uri_str.to_string())
            .context("'metastore-uri' is a required arg")?;
        let data_dir_path: PathBuf = matches
            .value_of("data-dir-path")
            .map(PathBuf::from)
            .expect("`data-dir-path` is a required arg.");
        let demux_factor = value_t!(matches, "demux-factor", usize)?;
        Ok(Self::DemuxJob(DemuxJobArgs::new(
            metastore_uri,
            index_id,
            data_dir_path,
            demux_factor,
        )?))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::DemuxJob(args) => demux_job_cli(args).await,
        }
    }
}

pub async fn demux_job_cli(args: DemuxJobArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "demux-job-start");
    let indexing_directory_path = args.data_dir_path.join(args.index_id.as_str());
    let indexing_directory = IndexingDirectory::create_in_dir(indexing_directory_path).await?;
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let source_config = SourceConfig {
        source_id: "empty-source".to_string(),
        source_type: "empty".to_string(),
        params: json!({}),
    };
    let indexer_params = IndexerParams {
        indexing_directory,
        heap_size: Byte::from_str("2GB")?,
        commit_policy: CommitPolicy::default(), //< TODO make the commit policy configurable
    };
    let indexing_pipeline_params = IndexingPipelineParams {
        index_id: args.index_id.clone(),
        source_config,
        indexer_params,
        metastore,
        storage_uri_resolver: storage_uri_resolver.clone(),
        merge_enabled: false,
        demux_enabled: true,
        demux_factor: Some(args.demux_factor),
    };
    let indexing_supervisor = IndexingPipelineSupervisor::new(indexing_pipeline_params);
    let universe = Universe::new();
    let (_supervisor_mailbox, supervisor_handler) =
        universe.spawn_actor(indexing_supervisor).spawn_async();
    let (supervisor_exit_status, _) = supervisor_handler.join().await;
    match supervisor_exit_status {
        ActorExitStatus::Success => {}
        ActorExitStatus::Quit
        | ActorExitStatus::DownstreamClosed
        | ActorExitStatus::Killed
        | ActorExitStatus::Panicked => {
            bail!(supervisor_exit_status)
        }
        ActorExitStatus::Failure(err) => {
            bail!(err);
        }
    }
    Ok(())
}
