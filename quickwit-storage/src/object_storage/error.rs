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

use std::error::Error as StdError;
use std::{fmt, io};

use rusoto_core::RusotoError;
use rusoto_s3::{
    AbortMultipartUploadError, CompleteMultipartUploadError, CreateMultipartUploadError,
    DeleteObjectError, GetObjectError, HeadObjectError, PutObjectError, UploadPartError,
};

use crate::retry::IsRetryable;
use crate::{StorageError, StorageErrorKind};

pub struct RusotoErrorWrapper<T: StdError>(pub RusotoError<T>);

impl<T: StdError> From<RusotoError<T>> for RusotoErrorWrapper<T> {
    fn from(err: RusotoError<T>) -> Self {
        RusotoErrorWrapper(err)
    }
}

impl<T: StdError + 'static> StdError for RusotoErrorWrapper<T> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.0)
    }
}

impl<T: StdError> fmt::Debug for RusotoErrorWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl<T: StdError + 'static> fmt::Display for RusotoErrorWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T: StdError> From<io::Error> for RusotoErrorWrapper<T> {
    fn from(err: io::Error) -> Self {
        RusotoErrorWrapper::from(RusotoError::from(err))
    }
}

impl<T: StdError> IsRetryable for RusotoErrorWrapper<T> {
    fn is_retryable(&self) -> bool {
        match &self.0 {
            RusotoError::HttpDispatch(_) => true,
            RusotoError::Service(_) => false,
            RusotoError::Unknown(http_resp) => http_resp.status.is_server_error(),
            _ => false,
        }
    }
}

impl<T> From<RusotoErrorWrapper<T>> for StorageError
where
    T: Send + Sync + std::error::Error + 'static + ToStorageErrorKind,
{
    fn from(err: RusotoErrorWrapper<T>) -> StorageError {
        let error_kind = match &err.0 {
            RusotoError::Credentials(_) => StorageErrorKind::Unauthorized,
            RusotoError::Service(err) => err.to_storage_error_kind(),
            RusotoError::Unknown(http_resp) => match http_resp.status.as_u16() {
                403 => StorageErrorKind::Unauthorized,
                404 => StorageErrorKind::DoesNotExist,
                _ => StorageErrorKind::InternalError,
            },
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
        }
    }
}

impl ToStorageErrorKind for DeleteObjectError {
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
