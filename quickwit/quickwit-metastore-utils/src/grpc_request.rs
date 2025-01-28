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

use quickwit_proto::metastore::*;

// The macros below are generating a req enum of the form
//
// ```
// enum GrpcRequest {
//    CreateIndexRequest(CreateIndexRequest),
//    IndexMetadataRequest(IndexMetadataRequest),
//    ...
// }
// ```
//
// And adds a From<SpecificRequest> implementation for
// every specific request.

macro_rules! build_req_enum {
    ( $($key:ident,)* ) => {
        use serde::{Serialize, Deserialize};
        #[derive(Serialize, Deserialize)]
        #[serde(tag="type")]
        pub enum GrpcRequest {
            $( $key($key), )*
        }
    }
}

macro_rules! generate_req_enum {
    ( $($key:ident,)* ) => {
        build_req_enum!($($key,)*);
        req_from_impls!($($key,)*);
    }
}

macro_rules! req_from_impls {
    ($name:ident,) => {
        impl From<$name> for GrpcRequest {
            fn from(req: $name) -> Self {
                GrpcRequest::$name(req)
            }
        }
    };
    ($name:ident, $($other:ident,)+) => {
        req_from_impls!($name,);
        req_from_impls!($($other,)+);
    }
}

generate_req_enum!(
    CreateIndexRequest,
    IndexMetadataRequest,
    ListIndexesMetadataRequest,
    DeleteIndexRequest,
    ListSplitsRequest,
    StageSplitsRequest,
    PublishSplitsRequest,
    MarkSplitsForDeletionRequest,
    DeleteSplitsRequest,
    AddSourceRequest,
    ToggleSourceRequest,
    DeleteSourceRequest,
    LastDeleteOpstampRequest,
    ResetSourceCheckpointRequest,
    DeleteQuery,
    UpdateSplitsDeleteOpstampRequest,
    ListDeleteTasksRequest,
    ListStaleSplitsRequest,
);
