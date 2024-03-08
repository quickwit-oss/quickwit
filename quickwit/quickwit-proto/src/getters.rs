use crate::control_plane::*;
use crate::indexing::*;
use crate::ingest::ingester::*;
use crate::ingest::router::*;
use crate::ingest::*;
use crate::metastore::*;
use crate::types::*;

macro_rules! generate_getters{
    (impl fn $field:ident() -> $type:ty {} for $($struct:ty),+) => {
        $(
        impl $struct {
            // we track caller so the reported line isn't the macro invocation below
            #[track_caller]
            pub fn $field(&self) -> &$type {
                self.$field
                    .as_ref()
                    .expect(concat!("`",
                    stringify!($field), "` should be a required field"))
            }
        }
        )*
    }
}

generate_getters! {
    impl fn index_uid() -> IndexUid {} for
    // Control Plane API
    GetOrCreateOpenShardsSuccess,

    // Indexing API
    IndexingTask,

    // Ingest API
    FetchEof,
    FetchPayload,
    IngestSuccess,
    OpenFetchStreamRequest,
    PersistFailure,
    PersistSubrequest,
    PersistSuccess,
    ReplicateFailure,
    ReplicateSubrequest,
    ReplicateSuccess,
    RetainShardsForSource,
    Shard,
    ShardIds,
    TruncateShardsSubrequest,

    // Metastore API
    AcquireShardsRequest,
    AddSourceRequest,
    CreateIndexResponse,
    DeleteIndexRequest,
    DeleteQuery,
    DeleteShardsRequest,
    DeleteSourceRequest,
    DeleteSplitsRequest,
    LastDeleteOpstampRequest,
    ListDeleteTasksRequest,
    ListShardsSubrequest,
    ListShardsSubresponse,
    ListStaleSplitsRequest,
    MarkSplitsForDeletionRequest,
    OpenShardsSubrequest,
    OpenShardsSubresponse,
    PublishSplitsRequest,
    ResetSourceCheckpointRequest,
    StageSplitsRequest,
    ToggleSourceRequest,
    UpdateSplitsDeleteOpstampRequest
}
