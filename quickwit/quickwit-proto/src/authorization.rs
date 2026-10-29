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

//! Maps every gRPC request type to the [`RequestFamily`] checked by the authorization layer.
//!
//! Hidden contract: requests sent on behalf of a user (with the user token in context) must not
//! belong to the `Cluster` family, otherwise the user needs the `cluster` right. For instance, an
//! ingest request makes the router call `GetOrCreateOpenShards` and `Persist` with the token of
//! the user, so these requests belong to the `IndexWrite` family. Requests sent from actors and
//! background tasks run with the node token, which is granted every right.

use quickwit_authorize::{Authorization, RequestFamily};

use crate::cluster::FetchClusterStateRequest;
use crate::compaction::ReportStatusRequest;
use crate::control_plane::{AdviseResetShardsRequest, GetOrCreateOpenShardsRequest};
use crate::developer::GetDebugInfoRequest;
use crate::indexing::ApplyIndexingPlanRequest;
use crate::ingest::ingester::{
    CloseShardsRequest, DecommissionRequest, InitShardsRequest, OpenFetchStreamRequest,
    PersistRequest, RetainShardsRequest, TruncateShardsRequest,
};
use crate::ingest::router::IngestRequestV2;
use crate::metastore::*;
use crate::search::{
    FetchDocsRequest, GetKvRequest, GetLoadRequest, LeafListFieldsRequest, LeafListTermsRequest,
    LeafSearchRequest, ListFieldsRequest, ListTermsRequest, PutKvRequest, ReportSplitsRequest,
    ScrollRequest, SearchRequest,
};

macro_rules! impl_authorization {
    ($request_family:expr => $($request_type:ty),+ $(,)?) => {
        $(
            impl Authorization for $request_type {
                fn request_family(&self) -> RequestFamily {
                    $request_family
                }
            }
        )+
    };
}

impl_authorization!(RequestFamily::IndexRead =>
    // Metastore
    IndexMetadataRequest,
    IndexesMetadataRequest,
    ListIndexesMetadataRequest,
    ListIndexStatsRequest,
    ListSplitsRequest,
    LastDeleteOpstampRequest,
    ListDeleteTasksRequest,
    ListStaleSplitsRequest,
    ListShardsRequest,
    GetIndexTemplateRequest,
    FindIndexTemplateMatchesRequest,
    ListIndexTemplatesRequest,
    GetClusterIdentityRequest,
    // Ingester
    OpenFetchStreamRequest,
    // Search
    SearchRequest,
    LeafSearchRequest,
    FetchDocsRequest,
    ListTermsRequest,
    LeafListTermsRequest,
    ScrollRequest,
    ListFieldsRequest,
    LeafListFieldsRequest,
    // `PutKV`/`GetKV` store scroll contexts while serving a search request.
    PutKvRequest,
    GetKvRequest,
);

impl_authorization!(RequestFamily::IndexWrite =>
    // Metastore
    DeleteQuery,
    // Control plane
    GetOrCreateOpenShardsRequest,
    // Ingest
    IngestRequestV2,
    PersistRequest,
);

impl_authorization!(RequestFamily::IndexAdmin =>
    // Metastore and control plane
    CreateIndexRequest,
    UpdateIndexRequest,
    DeleteIndexRequest,
    AddSourceRequest,
    UpdateSourceRequest,
    ToggleSourceRequest,
    DeleteSourceRequest,
    ResetSourceCheckpointRequest,
    MarkSplitsForDeletionRequest,
    DeleteSplitsRequest,
    CreateIndexTemplateRequest,
    DeleteIndexTemplatesRequest,
);

impl_authorization!(RequestFamily::Cluster =>
    // Metastore
    StageSplitsRequest,
    PublishSplitsRequest,
    UpdateSplitsDeleteOpstampRequest,
    OpenShardsRequest,
    AcquireShardsRequest,
    DeleteShardsRequest,
    PruneShardsRequest,
    // Control plane
    AdviseResetShardsRequest,
    // Ingester
    InitShardsRequest,
    RetainShardsRequest,
    TruncateShardsRequest,
    CloseShardsRequest,
    DecommissionRequest,
    // Indexing, compaction, cluster, developer
    ApplyIndexingPlanRequest,
    ReportStatusRequest,
    FetchClusterStateRequest,
    GetDebugInfoRequest,
    // Search
    ReportSplitsRequest,
    GetLoadRequest,
);
