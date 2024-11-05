use std::time::{Duration, SystemTime};

pub use biscuit_auth;
pub use biscuit_auth::builder_ext::BuilderExt;
pub use biscuit_auth::macros::*;
use quickwit_authorize::{
    Authorization, AuthorizationError, AuthorizationToken, StreamAuthorization,
};

use crate::cluster::FetchClusterStateRequest;
use crate::control_plane::{AdviseResetShardsRequest, GetOrCreateOpenShardsRequest};
use crate::developer::GetDebugInfoRequest;
use crate::indexing::ApplyIndexingPlanRequest;
use crate::ingest::ingester::{
    CloseShardsRequest, DecommissionRequest, InitShardsRequest, OpenFetchStreamRequest,
    OpenObservationStreamRequest, PersistRequest, RetainShardsRequest, SynReplicationMessage,
    TruncateShardsRequest,
};
use crate::ingest::router::IngestRequestV2;
use crate::metastore::{
    DeleteQuery, GetIndexTemplateRequest, IndexMetadataRequest, LastDeleteOpstampRequest,
    ListDeleteTasksRequest, ListIndexTemplatesRequest, ListIndexesMetadataRequest,
    ListShardsRequest, ListSplitsRequest, ListStaleSplitsRequest, OpenShardsRequest,
    PruneShardsRequest, PublishSplitsRequest, StageSplitsRequest, UpdateSplitsDeleteOpstampRequest,
};

impl Authorization for crate::metastore::AcquireShardsRequest {}

impl Authorization for crate::metastore::AddSourceRequest {}

impl Authorization for crate::metastore::CreateIndexRequest {
    fn attenuate(
        &self,
        auth_token: AuthorizationToken,
    ) -> Result<AuthorizationToken, AuthorizationError> {
        let mut builder = block!(r#"check if operation("create_index");"#);
        builder.check_expiration_date(SystemTime::now() + Duration::from_secs(60));
        let biscuit = auth_token.into_biscuit();
        let new_auth_token = biscuit.append(builder)?;
        Ok(AuthorizationToken::from(new_auth_token))
    }
}

impl Authorization for crate::metastore::CreateIndexTemplateRequest {}

impl Authorization for crate::metastore::DeleteIndexRequest {}

impl Authorization for crate::metastore::DeleteIndexTemplatesRequest {}

impl Authorization for crate::metastore::DeleteShardsRequest {}

impl Authorization for crate::metastore::DeleteSourceRequest {}

impl Authorization for crate::metastore::DeleteSplitsRequest {}

impl Authorization for crate::metastore::FindIndexTemplateMatchesRequest {}

impl Authorization for crate::metastore::IndexesMetadataRequest {}

impl Authorization for crate::metastore::ToggleSourceRequest {}

impl Authorization for crate::metastore::MarkSplitsForDeletionRequest {}

impl Authorization for crate::metastore::ResetSourceCheckpointRequest {}

impl Authorization for crate::metastore::UpdateIndexRequest {}

impl Authorization for OpenObservationStreamRequest {}

impl Authorization for InitShardsRequest {}

impl Authorization for OpenShardsRequest {}

impl Authorization for FetchClusterStateRequest {}

impl Authorization for GetIndexTemplateRequest {}

impl Authorization for ListIndexTemplatesRequest {}

impl Authorization for PruneShardsRequest {}

impl Authorization for ListShardsRequest {}

impl Authorization for ListStaleSplitsRequest {}

impl Authorization for ListDeleteTasksRequest {}

impl Authorization for UpdateSplitsDeleteOpstampRequest {}

impl Authorization for LastDeleteOpstampRequest {}

impl Authorization for DeleteQuery {}

impl Authorization for GetOrCreateOpenShardsRequest {}

impl Authorization for AdviseResetShardsRequest {}

impl Authorization for GetDebugInfoRequest {}

impl Authorization for StageSplitsRequest {}

impl Authorization for ListSplitsRequest {}

impl Authorization for PublishSplitsRequest {}

impl Authorization for ListIndexesMetadataRequest {}

impl Authorization for TruncateShardsRequest {}

impl Authorization for CloseShardsRequest {}

impl Authorization for RetainShardsRequest {}

impl Authorization for ApplyIndexingPlanRequest {}

impl Authorization for PersistRequest {}

impl Authorization for IndexMetadataRequest {}

impl StreamAuthorization for SynReplicationMessage {}

impl Authorization for IngestRequestV2 {}

impl Authorization for OpenFetchStreamRequest {}

impl Authorization for DecommissionRequest {}
