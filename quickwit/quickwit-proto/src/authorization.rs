use std::time::Duration;
use std::time::SystemTime;

use biscuit_auth::builder_ext::BuilderExt;
use quickwit_auth::Authorization;
use quickwit_auth::AuthorizationError;
use quickwit_auth::AuthorizationToken;
use quickwit_auth::StreamAuthorization;
use crate::cluster::FetchClusterStateRequest;
use crate::control_plane::AdviseResetShardsRequest;
use crate::control_plane::GetOrCreateOpenShardsRequest;
use crate::developer::GetDebugInfoRequest;
use crate::indexing::ApplyIndexingPlanRequest;
use crate::ingest::ingester::CloseShardsRequest;
use crate::ingest::ingester::DecommissionRequest;
use crate::ingest::ingester::InitShardsRequest;
use crate::ingest::ingester::OpenFetchStreamRequest;
use crate::ingest::ingester::OpenObservationStreamRequest;
use crate::ingest::ingester::PersistRequest;
use crate::ingest::ingester::RetainShardsRequest;
use crate::ingest::ingester::SynReplicationMessage;
use crate::ingest::ingester::TruncateShardsRequest;
use crate::ingest::router::IngestRequestV2;
use crate::metastore::DeleteQuery;
use crate::metastore::GetIndexTemplateRequest;
use crate::metastore::IndexMetadataRequest;
use crate::metastore::LastDeleteOpstampRequest;
use crate::metastore::ListDeleteTasksRequest;
use crate::metastore::ListIndexTemplatesRequest;
use crate::metastore::ListIndexesMetadataRequest;
use crate::metastore::ListShardsRequest;
use crate::metastore::ListSplitsRequest;
use crate::metastore::ListStaleSplitsRequest;
use crate::metastore::OpenShardsRequest;
use crate::metastore::PruneShardsRequest;
use crate::metastore::PublishSplitsRequest;
use crate::metastore::StageSplitsRequest;
use crate::metastore::UpdateSplitsDeleteOpstampRequest;
use biscuit_auth::macros::*;

impl Authorization for crate::metastore::AcquireShardsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for crate::metastore::AddSourceRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}



impl Authorization for crate::metastore::CreateIndexRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        let mut builder = block!(r#"check if operation("create_index");"#);
        builder.check_expiration_date(SystemTime::now() + Duration::from_secs(60));
        let new_auth_token = auth_token.append(builder)?;
        Ok(new_auth_token)
    }
}

impl Authorization for crate::metastore::CreateIndexTemplateRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for crate::metastore::DeleteIndexRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for crate::metastore::DeleteIndexTemplatesRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for crate::metastore::DeleteShardsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for crate::metastore::DeleteSourceRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for crate::metastore::DeleteSplitsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for crate::metastore::FindIndexTemplateMatchesRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for crate::metastore::IndexesMetadataRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for crate::metastore::ToggleSourceRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for crate::metastore::MarkSplitsForDeletionRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for crate::metastore::ResetSourceCheckpointRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for crate::metastore::UpdateIndexRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for OpenObservationStreamRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for InitShardsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for OpenShardsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for FetchClusterStateRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for GetIndexTemplateRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for ListIndexTemplatesRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for PruneShardsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for ListShardsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for ListStaleSplitsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for ListDeleteTasksRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for UpdateSplitsDeleteOpstampRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}


impl Authorization for LastDeleteOpstampRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}


impl Authorization for DeleteQuery {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}


impl Authorization for GetOrCreateOpenShardsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}


impl Authorization for AdviseResetShardsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}


impl Authorization for GetDebugInfoRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}


impl Authorization for StageSplitsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}


impl Authorization for ListSplitsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}


impl Authorization for PublishSplitsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}


impl Authorization for ListIndexesMetadataRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for TruncateShardsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}


impl Authorization for CloseShardsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}


impl Authorization for RetainShardsRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for ApplyIndexingPlanRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for PersistRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for IndexMetadataRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl StreamAuthorization for SynReplicationMessage {
    fn attenuate(auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for IngestRequestV2 {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for OpenFetchStreamRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}


impl Authorization for DecommissionRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}
