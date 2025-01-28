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

use quickwit_config::SourceConfig;
use quickwit_proto::indexing::{IndexingPipelineId, MergePipelineId};
use quickwit_proto::types::{IndexId, PipelineUid};

#[derive(Clone, Debug)]
pub struct SpawnPipeline {
    pub index_id: IndexId,
    pub source_config: SourceConfig,
    pub pipeline_uid: PipelineUid,
}

/// Detaches a pipeline from the indexing service. The pipeline is no longer managed by the
/// server. This is mostly useful for ad-hoc indexing pipelines launched with `quickwit index
/// ingest ..` and testing.
#[derive(Debug)]
pub struct DetachIndexingPipeline {
    pub pipeline_id: IndexingPipelineId,
}

/// Detaches a merge pipeline from the indexing service. The pipeline is no longer managed by the
/// server. This is mostly useful for preventing the server killing an existing merge pipeline
/// if a indexing pipeline is detached.
#[derive(Debug)]
pub struct DetachMergePipeline {
    pub pipeline_id: MergePipelineId,
}

#[derive(Debug)]
pub struct ObservePipeline {
    pub pipeline_id: IndexingPipelineId,
}
