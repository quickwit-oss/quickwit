// Copyright (C) 2024 Quickwit, Inc.
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
