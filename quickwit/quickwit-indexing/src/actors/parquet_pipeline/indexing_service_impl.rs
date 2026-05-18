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
use quickwit_common::{is_parquet_pipeline_index, is_sketches_index, temp_dir};
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_doc_mapper::RoutingExpr;
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
        use_sketch_processors: bool,
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

        let partition_key_str = index_config
            .doc_mapping
            .partition_key
            .as_deref()
            .unwrap_or("");
        let partition_key = RoutingExpr::new(partition_key_str).map_err(|error| {
            IndexingError::Internal(format!("failed to parse partition_key: {error}"))
        })?;

        // Spawn the Parquet merge pipeline (or reuse an existing one for this
        // index). The planner mailbox is wired into the MetricsPipeline's
        // Publisher so newly ingested splits are fed back for merging.
        let merge_planner_mailbox = self.get_or_create_parquet_merge_pipeline(
            indexing_pipeline_id.index_uid.clone(),
            &index_config,
            storage.clone(),
            indexing_directory.clone(),
            // None here means the pipeline's fetch_immature_splits() will
            // query the metastore on first spawn (same path as respawn).
            None,
            ctx,
        )?;

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
            use_sketch_processors,
            partition_key,
            max_num_partitions: index_config.doc_mapping.max_num_partitions,
            parquet_merge_planner_mailbox_opt: Some(merge_planner_mailbox),
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
        let index_id = &indexing_pipeline_id.index_uid.index_id;
        if is_parquet_pipeline_index(index_id) {
            let use_sketch_processors = is_sketches_index(index_id);
            self.spawn_metrics_pipeline(
                ctx,
                indexing_pipeline_id.clone(),
                index_config,
                source_config,
                params_fingerprint,
                use_sketch_processors,
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
