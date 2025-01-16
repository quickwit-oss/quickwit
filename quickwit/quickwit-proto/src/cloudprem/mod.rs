use quickwit_common::rate_limited_error;
use serde::{Deserialize, Serialize};

use crate::{GrpcServiceError, ServiceError, ServiceErrorCode};

include!("../codegen/cloudprem/cloudprem.rs");
include!("../codegen/cloudprem/queryparser_proto.rs");

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudPremError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("service unavailable: {0}")]
    Unavailable(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("too many requests")]
    TooManyRequests,
    #[error("unimplemented")]
    Unimplemented,
}

pub type CloudPremResult<T> = Result<T, CloudPremError>;

impl ServiceError for CloudPremError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(error_msg) => {
                rate_limited_error!(limit_per_min = 6, "ingest internal error: {error_msg}");
                ServiceErrorCode::Internal
            }
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
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
