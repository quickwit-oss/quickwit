// Copyright (C) 2022 Quickwit, Inc.
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

use quickwit_proto::metastore_api::*;

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
    ListIndexesMetadatasRequest,
    DeleteIndexRequest,
    ListAllSplitsRequest,
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
