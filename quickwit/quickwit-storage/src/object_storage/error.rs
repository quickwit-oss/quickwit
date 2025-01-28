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

use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;

use crate::{StorageError, StorageErrorKind};

impl<E> From<SdkError<E>> for StorageError
where E: std::error::Error + ToStorageErrorKind + Send + Sync + 'static
{
    fn from(error: SdkError<E>) -> StorageError {
        let error_kind = match &error {
            SdkError::ConstructionFailure(_) => StorageErrorKind::Internal,
            SdkError::DispatchFailure(failure) => {
                if failure.is_io() {
                    StorageErrorKind::Io
                } else if failure.is_timeout() {
                    StorageErrorKind::Timeout
                } else {
                    StorageErrorKind::Internal
                }
            }
            SdkError::ResponseError(response_error) => {
                match response_error.raw().status().as_u16() {
                    404 /* NOT_FOUND */ => StorageErrorKind::NotFound,
                    403 /* UNAUTHORIZED */ => StorageErrorKind::Unauthorized,
                    _ => StorageErrorKind::Internal,
                }
            }
            SdkError::ServiceError(service_error) => service_error.err().to_storage_error_kind(),
            SdkError::TimeoutError(_) => StorageErrorKind::Timeout,
            _ => StorageErrorKind::Internal,
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
        let error_code = self.code().unwrap_or("unknown");
        crate::STORAGE_METRICS
            .object_storage_get_errors_total
            .with_label_values([error_code])
            .inc();
        match self {
            GetObjectError::InvalidObjectState(_) => StorageErrorKind::Service,
            GetObjectError::NoSuchKey(_) => StorageErrorKind::NotFound,
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
            AbortMultipartUploadError::NoSuchUpload(_) => StorageErrorKind::Internal,
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
            _ => StorageErrorKind::Service,
        }
    }
}
