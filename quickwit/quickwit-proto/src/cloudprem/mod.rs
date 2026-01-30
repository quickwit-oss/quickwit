use quickwit_common::rate_limited_error;
use serde::{Deserialize, Serialize};

use crate::{GrpcServiceError, ServiceError, ServiceErrorCode};

include!("../codegen/cloudprem/calcfieldspb.rs");
include!("../codegen/cloudprem/cloudprem.rs");
include!("../codegen/cloudprem/queryparser_proto.rs");

pub mod index {
    include!("../codegen/cloudprem/cloudprem.index.rs");
}

pub mod metrics {
    include!("../codegen/cloudprem/cloudprem.metrics.rs");
}

pub const CLOUDPREM_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/cloudprem/descriptor.bin");

pub const CLOUDPREM_METRICS_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/cloudprem/metrics_descriptor.bin");

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudPremError {
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("service unavailable: {0}")]
    Unavailable(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("too many requests")]
    TooManyRequests,
    #[error("document not found id={id}, last known docaddr {split_id:?}/{doc_id:?}")]
    DocumentNotFound {
        id: String,
        split_id: Option<String>,
        doc_id: Option<u64>,
    },
    #[error("index already exists: {0}")]
    IndexAlreadyExists(String),
    #[error("index not found: {0}")]
    IndexNotFound(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("unimplemented")]
    Unimplemented,
}

pub type CloudPremResult<T> = Result<T, CloudPremError>;

impl ServiceError for CloudPremError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::InvalidQuery(_) => ServiceErrorCode::BadRequest,
            Self::Internal(error_msg) => {
                rate_limited_error!(limit_per_min = 6, "cloudprem internal error: {error_msg}");
                ServiceErrorCode::Internal
            }
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
            Self::DocumentNotFound { .. } => ServiceErrorCode::NotFound,
            Self::IndexAlreadyExists(_) => ServiceErrorCode::BadRequest,
            Self::IndexNotFound(_) => ServiceErrorCode::NotFound,
            Self::InvalidArgument(_) => ServiceErrorCode::BadRequest,
            Self::Unimplemented => ServiceErrorCode::Unimplemented,
        }
    }
}

impl GrpcServiceError for CloudPremError {
    fn new_internal(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_timeout(message: String) -> Self {
        Self::Timeout(message)
    }

    fn new_too_many_requests() -> Self {
        Self::TooManyRequests
    }

    fn new_unavailable(message: String) -> Self {
        Self::Unavailable(message)
    }
}

impl From<crate::metastore::MetastoreError> for CloudPremError {
    fn from(error: crate::metastore::MetastoreError) -> Self {
        use crate::metastore::MetastoreError;
        match error {
            MetastoreError::AlreadyExists(entity) => {
                CloudPremError::IndexAlreadyExists(format!("{} already exists", entity))
            }
            MetastoreError::NotFound(entity) => CloudPremError::IndexNotFound(entity.to_string()),
            MetastoreError::InvalidArgument { message } => CloudPremError::InvalidArgument(message),
            MetastoreError::Timeout(msg) => CloudPremError::Timeout(msg),
            MetastoreError::TooManyRequests => CloudPremError::TooManyRequests,
            MetastoreError::Unavailable(msg) => CloudPremError::Unavailable(msg),
            _ => CloudPremError::Internal(error.to_string()),
        }
    }
}

// Conversions between cloudprem and metastore IndexRoutingTable types.

impl From<crate::metastore::IndexRoutingRule> for IndexRoutingRule {
    fn from(rule: crate::metastore::IndexRoutingRule) -> Self {
        Self {
            filter: rule.filter,
            index_id: rule.index_id,
        }
    }
}

impl From<IndexRoutingRule> for crate::metastore::IndexRoutingRule {
    fn from(rule: IndexRoutingRule) -> Self {
        Self {
            filter: rule.filter,
            index_id: rule.index_id,
        }
    }
}
