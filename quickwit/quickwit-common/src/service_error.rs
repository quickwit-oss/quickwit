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

use std::error::Error;
use std::fmt::Debug;

use bytes::Bytes;
use once_cell::sync::Lazy;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;
use tonic::metadata::MetadataValue;
use tonic::Status;
use tracing::error;

const QW_ERROR_HEADER_NAME: &str = "qw-error";

// TODO: Move to `quickwit-proto` and replace current `ServiceError` with this trait.
pub trait ProutServiceError:
    Error + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    fn grpc_status_code(&self) -> tonic::Code;

    fn http_status_code(&self) -> http::StatusCode {
        static CLIENT_CLOSED_REQUESTED: Lazy<http::StatusCode> = Lazy::new(|| {
            http::StatusCode::from_u16(499).expect("499 should be a valid HTTP status code")
        });

        // From <https://chromium.googlesource.com/external/github.com/grpc/grpc/+/refs/tags/v1.21.4-pre1/doc/statuscodes.md>:
        match self.grpc_status_code() {
            tonic::Code::Ok => http::StatusCode::OK,
            tonic::Code::Cancelled => *CLIENT_CLOSED_REQUESTED,
            tonic::Code::Unknown => http::StatusCode::INTERNAL_SERVER_ERROR,
            tonic::Code::InvalidArgument => http::StatusCode::BAD_REQUEST,
            tonic::Code::DeadlineExceeded => http::StatusCode::GATEWAY_TIMEOUT,
            tonic::Code::NotFound => http::StatusCode::NOT_FOUND,
            tonic::Code::AlreadyExists => http::StatusCode::CONFLICT,
            tonic::Code::PermissionDenied => http::StatusCode::FORBIDDEN,
            tonic::Code::ResourceExhausted => http::StatusCode::TOO_MANY_REQUESTS,
            tonic::Code::FailedPrecondition => http::StatusCode::BAD_REQUEST,
            tonic::Code::Aborted => http::StatusCode::CONFLICT,
            tonic::Code::OutOfRange => http::StatusCode::BAD_REQUEST,
            tonic::Code::Unimplemented => http::StatusCode::NOT_IMPLEMENTED,
            tonic::Code::Internal => http::StatusCode::INTERNAL_SERVER_ERROR,
            tonic::Code::Unavailable => http::StatusCode::SERVICE_UNAVAILABLE,
            tonic::Code::DataLoss => http::StatusCode::INTERNAL_SERVER_ERROR,
            tonic::Code::Unauthenticated => http::StatusCode::UNAUTHORIZED,
        }
    }

    fn new_internal(message: String) -> Self;

    fn new_timeout(message: String) -> Self;

    fn new_unavailable(message: String) -> Self;

    fn label_value(&self) -> &'static str;
}

// TODO: Percent encode + base64 encode the error.

/// Converts a service error into a gRPC status.
pub fn service_errror_to_grpc_status<E>(error: E) -> Status
where E: ProutServiceError {
    let code = error.grpc_status_code();
    let message = error.to_string();
    let mut status = Status::new(code, message);

    if let Ok(error_json) = serde_json::to_vec(&error) {
        if let Ok(header_value) = MetadataValue::try_from(Bytes::from(error_json)) {
            status
                .metadata_mut()
                .insert(QW_ERROR_HEADER_NAME, header_value);
        }
    }
    status
}

/// Converts a gRPC status into a service error.
pub fn grpc_status_to_service_error<E>(status: Status) -> E
where E: ProutServiceError {
    if let Some(header) = status.metadata().get(QW_ERROR_HEADER_NAME) {
        if let Ok(error) = serde_json::from_slice(header.as_bytes()) {
            return error;
        } else {
            return E::new_internal("failed to deserialize error".to_string());
        }
    }
    if let Some(source) = status.source() {
        error!(error = %source, "transport error");
    }
    match status.code() {
        tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => {
            E::new_timeout(status.message().to_string())
        }
        tonic::Code::Unavailable => E::new_unavailable(status.message().to_string()),
        _ => E::new_internal(status.message().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[test]
    fn test_service_error_roundtrip() {
        #[derive(Clone, Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
        #[error("internal error: {0}")]
        enum MyError {
            #[serde(rename = "internal")]
            Internal(String),
            #[serde(rename = "timeout")]
            Timeout(String),
            #[serde(rename = "unavailable")]
            Unavailable(String),
        }

        impl ProutServiceError for MyError {
            fn grpc_status_code(&self) -> tonic::Code {
                match self {
                    MyError::Internal(_) => tonic::Code::Internal,
                    MyError::Timeout(_) => tonic::Code::DeadlineExceeded,
                    MyError::Unavailable(_) => tonic::Code::Unavailable,
                }
            }

            fn new_internal(message: String) -> Self {
                MyError::Internal(message)
            }

            fn new_timeout(message: String) -> Self {
                MyError::Timeout(message)
            }

            fn new_unavailable(message: String) -> Self {
                MyError::Unavailable(message)
            }

            fn label_value(&self) -> &'static str {
                match self {
                    MyError::Internal(_) => "internal",
                    MyError::Timeout(_) => "timeout",
                    MyError::Unavailable(_) => "unavailable",
                }
            }
        }

        let error = MyError::new_internal("test".to_string());
        let status = service_errror_to_grpc_status(error.clone());
        let expected_error: MyError = grpc_status_to_service_error(status);
        assert_eq!(error, expected_error);
    }
}
