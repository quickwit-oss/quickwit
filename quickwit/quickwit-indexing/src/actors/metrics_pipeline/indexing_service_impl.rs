use quickwit_actors::ActorContext;
use quickwit_common::temp_dir;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_proto::indexing::{IndexingError, IndexingPipelineId};

use crate::actors::pipeline_shared::ActorPipeline;
use crate::actors::{MetricsPipeline, MetricsPipelineParams};
use crate::{BoxPipelineHandle, IndexingService};

impl IndexingService {
    pub(crate) async fn spawn_metrics_pipeline(
        &mut self,
        ctx: &ActorContext<Self>,
        indexing_pipeline_id: IndexingPipelineId,
        index_config: IndexConfig,
        source_config: SourceConfig,
        params_fingerprint: u64,
    ) -> Result<BoxPipelineHandle, IndexingError> {
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
}
