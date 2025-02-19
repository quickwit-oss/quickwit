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

use crate::control_plane::*;
use crate::indexing::*;
use crate::ingest::ingester::*;
use crate::ingest::router::*;
use crate::ingest::*;
use crate::metastore::*;
use crate::types::*;

macro_rules! generate_getters {
    (impl fn $field:ident() -> $type:ty {} for $($struct:ty),+) => {
        $(
        impl $struct {
            // we track caller so the reported line isn't the macro invocation below
            #[track_caller]
            pub fn $field(&self) -> $type {
                self.$field
                    .as_ref()
                    .expect(concat!("`",
                    stringify!($field), "` should be a required field"))
            }
        }
        )*
    }
}

macro_rules! generate_clone_getters {
    (impl fn $field:ident() -> $type:ty {} for $($struct:ty),+) => {
        $(
        impl $struct {
            // we track caller so the reported line isn't the macro invocation below
            #[track_caller]
            pub fn $field(&self) -> $type {
                self.$field
                    .clone()
                    .expect(concat!("`",
                    stringify!($field), "` should be a required field"))
            }
        }
        )*
    }
}

macro_rules! generate_copy_getters {
    (impl fn $field:ident() -> $type:ty {} for $($struct:ty),+) => {
        $(
        impl $struct {
            // we track caller so the reported line isn't the macro invocation below
            #[track_caller]
            pub fn $field(&self) -> $type {
                self.$field
                    .expect(concat!("`",
                    stringify!($field), "` should be a required field"))
            }
        }
        )*
    }
}

// [`DocMappingUid`] getters
generate_copy_getters!(
    impl fn doc_mapping_uid() -> DocMappingUid {} for

    OpenShardSubrequest,
    Shard
);

// [`DocUid`] getters
generate_copy_getters! {
    impl fn doc_uid() -> DocUid {} for

    ParseFailure
}

// [`IndexUid`] getters
generate_getters! {
    impl fn index_uid() -> &IndexUid {} for
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
    ShardIdPositions,
    ShardIds,
    ShardPKey,
    TruncateShardsSubrequest,

    // Metastore API
    AcquireShardsRequest,
    AddSourceRequest,
    CreateIndexResponse,
    DeleteIndexRequest,
    DeleteQuery,
    DeleteShardsRequest,
    DeleteShardsResponse,
    DeleteSourceRequest,
    DeleteSplitsRequest,
    LastDeleteOpstampRequest,
    ListDeleteTasksRequest,
    ListShardsSubrequest,
    ListShardsSubresponse,
    ListStaleSplitsRequest,
    MarkSplitsForDeletionRequest,
    OpenShardSubrequest,
    PruneShardsRequest,
    PublishSplitsRequest,
    ResetSourceCheckpointRequest,
    StageSplitsRequest,
    ToggleSourceRequest,
    UpdateIndexRequest,
    UpdateSourceRequest,
    UpdateSplitsDeleteOpstampRequest
}

// [`PipelineUid`] getters
generate_copy_getters! {
    impl fn pipeline_uid() -> PipelineUid {} for

    IndexingTask
}

// [`Position`] getters. We use `clone` because `Position` is an `Arc` under the hood.
generate_clone_getters! {
    impl fn eof_position() -> Position {} for

    FetchEof
}

generate_clone_getters! {
    impl fn from_position_exclusive() -> Position {} for

    FetchPayload,
    OpenFetchStreamRequest,
    ReplicateSubrequest
}

generate_clone_getters! {
    impl fn to_position_inclusive() -> Position {} for

    FetchPayload
}

generate_clone_getters! {
    impl fn publish_position_inclusive() -> Position {} for

    Shard,
    ShardIdPosition
}

generate_clone_getters! {
    impl fn replication_position_inclusive() -> Position {} for

    ReplicateSuccess
}

generate_clone_getters! {
    impl fn truncate_up_to_position_inclusive() -> Position {} for

    TruncateShardsSubrequest
}

// [`Shard`] getters
generate_getters! {
    impl fn open_shard() -> &Shard {} for

    OpenShardSubresponse
}

generate_getters! {
    impl fn shard() -> &Shard {} for

    InitShardSubrequest,
    InitShardSuccess
}

// [`ShardId`] getters
generate_getters! {
    impl fn shard_id() -> &ShardId {} for

    FetchEof,
    FetchPayload,
    InitShardFailure,
    OpenFetchStreamRequest,
    OpenShardSubrequest,
    PersistFailure,
    PersistSubrequest,
    PersistSuccess,
    ReplicateFailure,
    ReplicateSubrequest,
    ReplicateSuccess,
    Shard,
    ShardIdPosition,
    ShardPKey,
    TruncateShardsSubrequest
}
