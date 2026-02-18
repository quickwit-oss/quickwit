use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::prelude::SessionConfig;
use datafusion_distributed::{DistributedExt, WorkerQueryContext};
use tantivy_datafusion::{IndexOpener, OpenerFactoryExt, OpenerMetadata, TantivyCodec};

use crate::split_opener::{SplitIndexOpener, SplitRegistry};

/// Build a worker session builder for test contexts.
///
/// Sets the opener factory on the session config and registers the
/// stateless `TantivyCodec`. In production, `build_flight_service`
/// does the same thing.
pub fn build_worker_session_builder(
    registry: Arc<SplitRegistry>,
) -> impl Fn(WorkerQueryContext) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<SessionState, DataFusionError>> + Send>,
> + Clone
       + Send
       + Sync
       + 'static {
    move |ctx: WorkerQueryContext| {
        let registry = registry.clone();
        Box::pin(async move {
            let mut config = SessionConfig::new();
            config.set_opener_factory(Arc::new(move |meta: OpenerMetadata| {
                Arc::new(SplitIndexOpener::new(
                    meta.identifier,
                    registry.clone(),
                    meta.tantivy_schema,
                    meta.segment_sizes,
                )) as Arc<dyn IndexOpener>
            }));
            Ok(ctx
                .builder
                .with_config(config)
                .with_distributed_user_codec(TantivyCodec)
                .build())
        })
    }
}
