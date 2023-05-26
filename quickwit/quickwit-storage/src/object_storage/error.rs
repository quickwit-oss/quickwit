// Copyright (C) 2023 Quickwit, Inc.
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

use aws_sdk_s3::error::{DisplayErrorContext, SdkError};
use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use hyper::http::StatusCode;
use quickwit_aws::retry::Retryable;

use crate::{StorageError, StorageErrorKind};

impl<E> From<SdkError<E>> for StorageError
where E: Send + Sync + std::error::Error + 'static + ToStorageErrorKind + Retryable
{
    fn from(error: SdkError<E>) -> StorageError {
        let error_kind = match &error {
            SdkError::ConstructionFailure(_) => StorageErrorKind::InternalError,
            SdkError::DispatchFailure(failure) => {
                if failure.is_io() {
                    StorageErrorKind::Io
                } else if failure.is_timeout() {
                    StorageErrorKind::Timeout
                } else {
                    StorageErrorKind::InternalError
                }
            }
            SdkError::ResponseError(response_error) => {
                let response = response_error.raw().http();
                match response.status() {
                    StatusCode::NOT_FOUND => StorageErrorKind::NotFound,
                    StatusCode::UNAUTHORIZED => StorageErrorKind::Unauthorized,
                    _ => StorageErrorKind::InternalError,
                }
            }
            SdkError::ServiceError(service_error) => service_error.err().to_storage_error_kind(),
            SdkError::TimeoutError(_) => StorageErrorKind::Timeout,
            _ => StorageErrorKind::InternalError,
        };
        let source = anyhow::anyhow!("{}", DisplayErrorContext(error));
        error_kind.with_error(source)
    }
}

pub trait ToStorageErrorKind {
    fn to_storage_error_kind(&self) -> StorageErrorKind;
}

impl ToStorageErrorKind for GetObjectError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        match self {
            GetObjectError::InvalidObjectState(_) => StorageErrorKind::Service,
            GetObjectError::NoSuchKey(_) => StorageErrorKind::NotFound,
            GetObjectError::Unhandled(_) => StorageErrorKind::Service,
            _ => StorageErrorKind::Service,
        }
    }
}

impl ToStorageErrorKind for DeleteObjectError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        StorageErrorKind::Service
    }
}

impl ToStorageErrorKind for DeleteObjectsError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        StorageErrorKind::Service
    }
}

impl ToStorageErrorKind for UploadPartError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        StorageErrorKind::Service
    }
}

impl ToStorageErrorKind for CompleteMultipartUploadError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        StorageErrorKind::Service
    }
}

impl ToStorageErrorKind for AbortMultipartUploadError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        match self {
            AbortMultipartUploadError::NoSuchUpload(_) => StorageErrorKind::InternalError,
            AbortMultipartUploadError::Unhandled(_) => StorageErrorKind::Service,
            _ => StorageErrorKind::Service,
        }
    }
}

impl ToStorageErrorKind for CreateMultipartUploadError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        StorageErrorKind::Service
    }
}

impl ToStorageErrorKind for PutObjectError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        StorageErrorKind::Service
    }
}

impl ToStorageErrorKind for HeadObjectError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        match self {
            HeadObjectError::NotFound(_) => StorageErrorKind::NotFound,
            HeadObjectError::Unhandled(_) => StorageErrorKind::Service,
            _ => StorageErrorKind::Service,
        }
    }
}
