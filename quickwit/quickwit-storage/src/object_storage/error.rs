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

use std::time::Duration;

use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use quickwit_aws::error::retry_after_from_sdk_error;
use quickwit_aws::retry::AwsRetryable;
use quickwit_metrics::counter;

use crate::metrics::OBJECT_STORAGE_GET_ERRORS_TOTAL;
use crate::{StorageError, StorageErrorKind};

/// Maps the HTTP status reported by an S3-compatible store to a storage error kind.
fn storage_error_kind_from_status(status: u16) -> StorageErrorKind {
    match status {
        404 /* NOT_FOUND */ => StorageErrorKind::NotFound,
        403 /* UNAUTHORIZED */ => StorageErrorKind::Unauthorized,
        412 /* PRECONDITION_FAILED */ => StorageErrorKind::PreconditionFailed,
        _ => StorageErrorKind::Internal,
    }
}

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
                storage_error_kind_from_status(response_error.raw().status().as_u16())
            }
            SdkError::ServiceError(service_error) => service_error.err().to_storage_error_kind(),
            SdkError::TimeoutError(_) => StorageErrorKind::Timeout,
            _ => StorageErrorKind::Internal,
        };
        // Extract the server-suggested retry delay before consuming the error.
        let retry_after = retry_after_from_sdk_error(&error);
        let source = anyhow::anyhow!("{}", DisplayErrorContext(error));
        error_kind.with_error(source).with_retry_after(retry_after)
    }
}

impl AwsRetryable for StorageError {
    fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }
}

pub trait ToStorageErrorKind {
    fn to_storage_error_kind(&self) -> StorageErrorKind;
}

impl ToStorageErrorKind for GetObjectError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        let error_code = self.code().unwrap_or("unknown").to_string();
        counter!(
            parent: OBJECT_STORAGE_GET_ERRORS_TOTAL,
            "code" => error_code,
        )
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
        // Conditional writes (`If-None-Match` / `If-Match`) are rejected with this code, and losing
        // the race is an expected outcome that callers have to retry, not an internal failure.
        if self.code() == Some("PreconditionFailed") {
            return StorageErrorKind::PreconditionFailed;
        }
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

impl ToStorageErrorKind for ListObjectsV2Error {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        match self {
            ListObjectsV2Error::NoSuchBucket(_) => StorageErrorKind::NotFound,
            _ => StorageErrorKind::Service,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn test_list_objects_v2_timeout_error_is_preserved() {
        let sdk_error = SdkError::<ListObjectsV2Error>::timeout_error(io::Error::other("timeout"));
        let storage_error = StorageError::from(sdk_error);

        assert_eq!(storage_error.kind(), StorageErrorKind::Timeout);
    }

    #[test]
    fn test_precondition_failed_status_is_preserved() {
        // A conditional write that loses the race is reported as HTTP 412. Mapping it to
        // `Internal` would make callers treat a normal lost race as a service failure, and mapping
        // it to `Service` would hide it from the retry loop that has to re-read and try again.
        assert_eq!(
            storage_error_kind_from_status(412),
            StorageErrorKind::PreconditionFailed
        );
        assert_eq!(
            storage_error_kind_from_status(404),
            StorageErrorKind::NotFound
        );
        assert_eq!(
            storage_error_kind_from_status(403),
            StorageErrorKind::Unauthorized
        );
        assert_eq!(
            storage_error_kind_from_status(500),
            StorageErrorKind::Internal
        );
    }
}
