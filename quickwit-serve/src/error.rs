// Copyright (C) 2021 Quickwit, Inc.
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

use std::convert::Infallible;
use std::fmt;

use quickwit_actors::AskError;
use quickwit_cluster::ClusterError;
use quickwit_indexing::IndexingServerError;
use quickwit_proto::tonic;
use quickwit_pushapi::PushApiError;
use quickwit_search::SearchError;
use warp::http;

/// This enum serves as a Rosetta stone of
/// gRPC and Http status code.
///
/// It is voluntarily a restricted subset.
#[derive(Clone, Copy)]
pub enum ServiceErrorCode {
    NotFound,
    Internal,
    BadRequest,
    PermissionDenied,
}

impl ServiceErrorCode {
    pub(crate) fn to_grpc_status_code(self) -> tonic::Code {
        match self {
            ServiceErrorCode::NotFound => tonic::Code::NotFound,
            ServiceErrorCode::Internal => tonic::Code::Internal,
            ServiceErrorCode::BadRequest => tonic::Code::InvalidArgument,
            ServiceErrorCode::PermissionDenied => tonic::Code::PermissionDenied,
        }
    }
    pub(crate) fn to_http_status_code(self) -> http::StatusCode {
        match self {
            ServiceErrorCode::NotFound => http::StatusCode::NOT_FOUND,
            ServiceErrorCode::Internal => http::StatusCode::INTERNAL_SERVER_ERROR,
            ServiceErrorCode::BadRequest => http::StatusCode::BAD_REQUEST,
            ServiceErrorCode::PermissionDenied => http::StatusCode::FORBIDDEN,
        }
    }
}

impl ServiceError for SearchError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            SearchError::IndexDoesNotExist { .. } => ServiceErrorCode::NotFound,
            SearchError::InternalError(_) => ServiceErrorCode::Internal,
            SearchError::StorageResolverError(_) => ServiceErrorCode::BadRequest,
            SearchError::InvalidQuery(_) => ServiceErrorCode::BadRequest,
        }
    }
}

impl ServiceError for ClusterError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            ClusterError::CreateClusterError { .. } => ServiceErrorCode::Internal,
            ClusterError::UDPPortBindingError { .. } => ServiceErrorCode::PermissionDenied,
            ClusterError::ReadHostIdError { .. } => ServiceErrorCode::Internal,
            ClusterError::WriteHostIdError { .. } => ServiceErrorCode::Internal,
            ClusterError::ClusterStateError { .. } => ServiceErrorCode::Internal,
        }
    }
}

impl ServiceError for IndexingServerError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            Self::MissingPipeline { .. } => ServiceErrorCode::NotFound,
            Self::PipelineAlreadyExists { .. } => ServiceErrorCode::BadRequest,
            Self::StorageError(_) => ServiceErrorCode::Internal,
            Self::MetastoreError(_) => ServiceErrorCode::Internal,
            Self::InvalidParams(_) => ServiceErrorCode::BadRequest,
        }
    }
}

impl ServiceError for PushApiError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            PushApiError::Corruption { .. } => ServiceErrorCode::Internal,
            PushApiError::QueueDoesNotExist { .. } => ServiceErrorCode::NotFound,
            PushApiError::QueueAlreadyExists { .. } => ServiceErrorCode::BadRequest,
            PushApiError::PushAPIServiceDown => ServiceErrorCode::Internal,
        }
    }
}

impl<E: fmt::Debug + ServiceError> ServiceError for AskError<E> {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            AskError::MessageNotDelivered => ServiceErrorCode::Internal,
            AskError::ProcessMessageError => ServiceErrorCode::Internal,
            AskError::ErrorReply(err) => err.status_code(),
        }
    }
}

impl ServiceError for Infallible {
    fn status_code(&self) -> ServiceErrorCode {
        unreachable!()
    }
}

pub(crate) trait ServiceError: ToString {
    fn grpc_error(&self) -> tonic::Status {
        let grpc_code = self.status_code().to_grpc_status_code();
        let error_msg = self.to_string();
        tonic::Status::new(grpc_code, error_msg)
    }

    fn status_code(&self) -> ServiceErrorCode;
}

pub(crate) fn convert_to_grpc_result<T, E: ServiceError>(
    res: Result<T, E>,
) -> Result<tonic::Response<T>, tonic::Status> {
    res.map(|outcome| tonic::Response::new(outcome))
        .map_err(|err| err.grpc_error())
}
