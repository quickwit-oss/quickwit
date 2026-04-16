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

// This file is meant to "override" some behavior of the indexing service when
// the metrics feature flag is enabled.
//
// In that case, metrics index will be started as a metrics pipeline.

use quickwit_actors::ActorContext;
use quickwit_common::{is_metrics_index, temp_dir};
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_metastore::SplitMetadata;
use quickwit_proto::indexing::{IndexingError, IndexingPipelineId};

use crate::actors::pipeline_shared::ActorPipeline;
use crate::actors::{MetricsPipeline, MetricsPipelineParams};
use crate::{BoxedPipelineHandle, IndexingService};

impl IndexingService {
    async fn spawn_metrics_pipeline(
        &mut self,
        ctx: &ActorContext<Self>,
        indexing_pipeline_id: IndexingPipelineId,
        index_config: IndexConfig,
        source_config: SourceConfig,
        params_fingerprint: u64,
    ) -> Result<BoxedPipelineHandle, IndexingError> {
        let pipeline_uid_str = indexing_pipeline_id.pipeline_uid.to_string();
        let indexing_directory = temp_dir::Builder::default()
            .join(&indexing_pipeline_id.index_uid.index_id)
            .join(&indexing_pipeline_id.index_uid.incarnation_id.to_string())
            .join(&indexing_pipeline_id.source_id)
            .join(&pipeline_uid_str)
            .tempdir_in(&self.indexing_root_directory)
            .map_err(|error| {
                let message = format!("failed to create indexing directory: {error}");
                IndexingError::Internal(message)
            })?;
        let storage = self
            .storage_resolver
            .resolve(&index_config.index_uri)
            .await
            .map_err(|error| {
                let message = format!("failed to spawn metrics pipeline: {error}");
                IndexingError::Internal(message)
            })?;

        let pipeline_params = MetricsPipelineParams {
            pipeline_id: indexing_pipeline_id.clone(),
            metastore: self.metastore.clone(),
            storage,
            indexing_directory,
            indexing_settings: index_config.indexing_settings.clone(),
            max_concurrent_split_uploads: self.max_concurrent_split_uploads,
            source_config,
            ingester_pool: self.ingester_pool.clone(),
            queues_dir_path: self.queue_dir_path.clone(),
            source_storage_resolver: self.storage_resolver.clone(),
            params_fingerprint,
            event_broker: self.event_broker.clone(),
        };
        let pipeline = MetricsPipeline::new(pipeline_params);
        let (mailbox, handle) = ctx.spawn_actor().spawn(pipeline);
        Ok(Box::new(ActorPipeline {
            pipeline_id: indexing_pipeline_id,
            mailbox,
            handle,
        }))
    }

    pub(crate) async fn spawn_log_or_metrics_pipeline(
        &mut self,
        ctx: &ActorContext<Self>,
        indexing_pipeline_id: IndexingPipelineId,
        index_config: IndexConfig,
        source_config: SourceConfig,
        immature_splits_opt: Option<Vec<SplitMetadata>>,
        params_fingerprint: u64,
    ) -> Result<BoxedPipelineHandle, IndexingError> {
        if is_metrics_index(&indexing_pipeline_id.index_uid.index_id) {
            self.spawn_metrics_pipeline(
                ctx,
                indexing_pipeline_id.clone(),
                index_config,
                source_config,
                params_fingerprint,
            )
            .await
        } else {
            self.spawn_log_pipeline(
                ctx,
                indexing_pipeline_id.clone(),
                index_config,
                source_config,
                immature_splits_opt,
                params_fingerprint,
            )
            .await
        }
    }
}
