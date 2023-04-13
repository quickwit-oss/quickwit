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

use aws_sdk_s3::error::SdkError;
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
use quickwit_aws::error::SdkErrorWrapper;
use quickwit_aws::retry::Retryable;

use crate::{StorageError, StorageErrorKind};

impl<E> From<SdkErrorWrapper<E>> for StorageError
where E: Send + Sync + std::error::Error + 'static + ToStorageErrorKind + Retryable
{
    fn from(err: SdkErrorWrapper<E>) -> StorageError {
        match err {
            SdkErrorWrapper::Io(e) => StorageErrorKind::Io.with_error(e),
            SdkErrorWrapper::Sdk(e) => StorageError::from(e),
        }
    }
}

impl<E> From<SdkError<E>> for StorageError
where E: Send + Sync + std::error::Error + 'static + ToStorageErrorKind + Retryable
{
    fn from(err: SdkError<E>) -> StorageError {
        let error_kind = match &err {
            SdkError::ConstructionFailure(_) => StorageErrorKind::InternalError,
            SdkError::TimeoutError(_) => StorageErrorKind::Timeout,
            SdkError::DispatchFailure(e) => match e {
                e if e.is_io() => StorageErrorKind::Io,
                e if e.is_timeout() => StorageErrorKind::Timeout,
                e if e.is_other().is_some() => StorageErrorKind::InternalError,
                e if e.is_user() => StorageErrorKind::InternalError,
                _ => StorageErrorKind::InternalError,
            },
            SdkError::ResponseError(e) => {
                let resp = e.raw().http();

                match resp.status() {
                    StatusCode::UNAUTHORIZED => StorageErrorKind::Unauthorized,
                    StatusCode::NOT_FOUND => StorageErrorKind::DoesNotExist,
                    _ => StorageErrorKind::InternalError,
                }
            }
            SdkError::ServiceError(e) => e.err().to_storage_error_kind(),
            _ => StorageErrorKind::InternalError,
        };
        error_kind.with_error(err)
    }
}

pub trait ToStorageErrorKind {
    fn to_storage_error_kind(&self) -> StorageErrorKind;
}

impl ToStorageErrorKind for GetObjectError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        match self {
            GetObjectError::InvalidObjectState(_) => StorageErrorKind::Service,
            GetObjectError::NoSuchKey(_) => StorageErrorKind::DoesNotExist,
            GetObjectError::Unhandled(_) => StorageErrorKind::InternalError,
            _ => StorageErrorKind::InternalError,
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
        StorageErrorKind::Service
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
        StorageErrorKind::Service
    }
}
