use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion_distributed::{DistributedExt, WorkerQueryContext};
use tantivy_datafusion::{OpenerMetadata, TantivyCodec};

use crate::split_opener::{SplitIndexOpener, SplitRegistry};

/// Build a worker session builder that registers tantivy-df's
/// [`TantivyCodec`] with the given [`SplitRegistry`].
///
/// Each worker gets its own registry populated with the splits it
/// should serve. The codec's opener factory reconstructs
/// [`SplitIndexOpener`]s from the serialized metadata.
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
            let codec = TantivyCodec::new(move |meta: OpenerMetadata| {
                Arc::new(SplitIndexOpener::new(
                    meta.identifier,
                    registry.clone(),
                    meta.tantivy_schema,
                    meta.segment_sizes,
                )) as Arc<dyn tantivy_datafusion::IndexOpener>
            });
            Ok(ctx.builder.with_distributed_user_codec(codec).build())
        })
    }
}
