use std::time::{Duration, SystemTime};

pub use biscuit_auth;
pub use biscuit_auth::builder_ext::BuilderExt;
pub use biscuit_auth::macros::*;
use quickwit_authorize::{
    Authorization, AuthorizationError, AuthorizationToken, RequestFamily, StreamAuthorization,
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

impl Authorization for crate::metastore::AcquireShardsRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexWrite
    }
}

impl Authorization for crate::metastore::AddSourceRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexAdmin
    }
}

impl Authorization for crate::metastore::CreateIndexRequest {
    fn attenuate(
        &self,
        auth_token: AuthorizationToken,
    ) -> Result<AuthorizationToken, AuthorizationError> {
        let mut builder = block!(r#"check if operation("create_index");"#);
        builder.check_expiration_date(SystemTime::now() + Duration::from_secs(60));
        let biscuit = auth_token.into_biscuit();
        let new_auth_token = biscuit
            .append(builder)
            .map_err(|_| AuthorizationError::PermissionDenied)?;
        Ok(AuthorizationToken::from(new_auth_token))
    }

    fn request_family() -> RequestFamily {
        RequestFamily::IndexAdmin
    }
}

impl Authorization for crate::metastore::CreateIndexTemplateRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexAdmin
    }
}

impl Authorization for crate::metastore::DeleteIndexRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexAdmin
    }
}

impl Authorization for crate::metastore::DeleteIndexTemplatesRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexAdmin
    }
}

impl Authorization for crate::metastore::DeleteShardsRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexAdmin
    }
}

impl Authorization for crate::metastore::DeleteSourceRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexAdmin
    }
}

impl Authorization for crate::metastore::DeleteSplitsRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexAdmin
    }
}

impl Authorization for crate::metastore::FindIndexTemplateMatchesRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexRead
    }
}

impl Authorization for crate::metastore::IndexesMetadataRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexRead
    }
}

impl Authorization for crate::metastore::ToggleSourceRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexAdmin
    }
}

impl Authorization for crate::metastore::MarkSplitsForDeletionRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexWrite
    }
}

impl Authorization for crate::metastore::ResetSourceCheckpointRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexAdmin
    }
}

impl Authorization for crate::metastore::UpdateIndexRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexAdmin
    }
}

impl Authorization for OpenObservationStreamRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexWrite
    }
}

impl Authorization for InitShardsRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexWrite
    }
}

impl Authorization for OpenShardsRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexWrite
    }
}

impl Authorization for FetchClusterStateRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::Cluster
    }
}

impl Authorization for GetIndexTemplateRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexRead
    }
}

impl Authorization for ListIndexTemplatesRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexRead
    }
}

impl Authorization for PruneShardsRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexRead
    }
}

impl Authorization for ListShardsRequest {
    fn request_family() -> RequestFamily {
        RequestFamily::IndexWrite
    }
}

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
