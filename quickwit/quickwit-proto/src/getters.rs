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
                    .expect(concat!("`", stringify!($field), "` should be a required field in `", stringify!($struct), "`"))
            }
        }
        )*
    }
}

generate_getters! {
    impl fn index_uid() -> IndexUid {} for
    GetOrCreateOpenShardsSuccess,
    IngestSuccess,
    IndexingTask,
    Shard, ShardIds,

    RetainShardsForSource, PersistSubrequest, PersistSuccess, PersistFailure, ReplicateSubrequest,
    ReplicateSuccess, ReplicateFailure, TruncateShardsSubrequest, OpenFetchStreamRequest,
    FetchPayload, FetchEof,

    CreateIndexResponse, DeleteIndexRequest, StageSplitsRequest, PublishSplitsRequest,
    MarkSplitsForDeletionRequest, DeleteSplitsRequest, AddSourceRequest, ToggleSourceRequest,
    DeleteSourceRequest, ResetSourceCheckpointRequest, DeleteQuery, UpdateSplitsDeleteOpstampRequest,
    LastDeleteOpstampRequest, ListStaleSplitsRequest, ListDeleteTasksRequest, OpenShardsSubrequest,
    OpenShardsSubresponse, AcquireShardsSubrequest, AcquireShardsSubresponse, DeleteShardsSubrequest,
    ListShardsSubrequest, ListShardsSubresponse
}
