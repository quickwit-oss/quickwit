/*
    quickwit
    copyright (c) 2021 quickwit inc.

    quickwit is offered under the agpl v3.0 and as commercial software.
    for commercial licensing, contact us at hello@quickwit.io.

    agpl:
    this program is free software: you can redistribute it and/or modify
    it under the terms of the gnu affero general public license as
    published by the free software foundation, either version 3 of the
    license, or (at your option) any later version.

    this program is distributed in the hope that it will be useful,
    but without any warranty; without even the implied warranty of
    merchantability or fitness for a particular purpose.  see the
    gnu affero general public license for more details.

    you should have received a copy of the gnu affero general public license
    along with this program.  if not, see <http://www.gnu.org/licenses/>.
*/

use std::error::Error as StdError;
use std::fmt;
use std::io;

use rusoto_core::RusotoError;
use rusoto_s3::{
    AbortMultipartUploadError, CompleteMultipartUploadError, CreateMultipartUploadError,
    DeleteObjectError, GetObjectError, PutObjectError, UploadPartError,
};

use crate::retry::IsRetryable;
use crate::{StoreError, StoreErrorKind};

pub struct RusotoErrorWrapper<T: StdError>(RusotoError<T>);
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

impl<T> From<RusotoErrorWrapper<T>> for StoreError
where
    T: Send + Sync + std::error::Error + 'static + ToStoreErrorKind,
{
    fn from(err: RusotoErrorWrapper<T>) -> StoreError {
        let error_kind = match &err.0 {
            RusotoError::Credentials(_) => StoreErrorKind::Unauthorized,
            RusotoError::Service(err) => {
                dbg!(&err);
                err.to_store_error_kind()
                // StoreErrorKind::Service
            }
            RusotoError::Unknown(http_resp) => {
                if http_resp.status == 404 {
                    StoreErrorKind::DoesNotExist
                } else {
                    StoreErrorKind::InternalError
                }
            }
            _ => StoreErrorKind::InternalError,
        };
        error_kind.with_error(err)
    }
}

pub trait ToStoreErrorKind {
    fn to_store_error_kind(&self) -> StoreErrorKind;
}

impl ToStoreErrorKind for GetObjectError {
    fn to_store_error_kind(&self) -> StoreErrorKind {
        match self {
            GetObjectError::InvalidObjectState(_) => StoreErrorKind::Service,
            GetObjectError::NoSuchKey(_) => StoreErrorKind::DoesNotExist,
        }
    }
}

impl ToStoreErrorKind for DeleteObjectError {
    fn to_store_error_kind(&self) -> StoreErrorKind {
        StoreErrorKind::Service
    }
}

impl ToStoreErrorKind for UploadPartError {
    fn to_store_error_kind(&self) -> StoreErrorKind {
        StoreErrorKind::Service
    }
}

impl ToStoreErrorKind for CompleteMultipartUploadError {
    fn to_store_error_kind(&self) -> StoreErrorKind {
        StoreErrorKind::Service
    }
}

impl ToStoreErrorKind for AbortMultipartUploadError {
    fn to_store_error_kind(&self) -> StoreErrorKind {
        StoreErrorKind::Service
    }
}

impl ToStoreErrorKind for CreateMultipartUploadError {
    fn to_store_error_kind(&self) -> StoreErrorKind {
        StoreErrorKind::Service
    }
}

impl ToStoreErrorKind for PutObjectError {
    fn to_store_error_kind(&self) -> StoreErrorKind {
        StoreErrorKind::Service
    }
}
