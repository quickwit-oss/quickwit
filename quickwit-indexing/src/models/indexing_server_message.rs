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

use quickwit_config::SourceConfig;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct IndexingPipelineId {
    pub index_id: String,
    pub source_id: String,
}

/// Detaches a pipeline from the indexing server. The pipeline is no longer managed by the
/// server. This is mostly useful for ad-hoc indexing pipelines launched with `quickwit index
/// ingest ..` and testing.
#[derive(Debug)]
pub struct DetachPipeline {
    pub pipeline_id: IndexingPipelineId,
}

#[derive(Debug)]
pub struct ObservePipeline {
    pub pipeline_id: IndexingPipelineId,
}

#[derive(Debug)]
pub struct SpawnMergePipeline {
    pub index_id: String,
    pub merge_enabled: bool,
    pub demux_enabled: bool,
}

#[derive(Debug)]
pub struct SpawnPipelines {
    pub index_id: String,
}

#[derive(Debug)]
pub struct SpawnPipeline {
    pub index_id: String,
    pub source: SourceConfig,
}
