/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use rusoto_core::RusotoError;
use rusoto_s3::{
    AbortMultipartUploadError, CompleteMultipartUploadError, CreateMultipartUploadError,
    DeleteObjectError, GetObjectError, PutObjectError, UploadPartError,
};
use std::error::Error as StdErr;
use std::{fmt, io};
use thiserror::Error;

#[derive(Debug, Clone, Copy)]
pub enum StoreErrorKind {
    DoesNotExist,
    Unauthorized,
    Service,
    InternalError,
    Io,
    URIError,
}

#[derive(Error, Debug)]
pub enum StorageFromURIError {
    #[error("Invalid URI: `{0}`")]
    InvalidURI(String),
    #[error("Create client failure: `{0}`")]
    Other(anyhow::Error),
}

impl StoreErrorKind {
    pub fn with_error<E>(self, source: E) -> StoreError
    where
        anyhow::Error: From<E>,
    {
        StoreError {
            kind: self,
            source: From::from(source),
        }
    }
}

// impl Into<io::Error> for StoreError {
//     fn into(self) -> io::Error {
//         let io_error_kind = match self.kind() {
//             StoreErrorKind::DoesNotExist => io::ErrorKind::NotFound,
//             _ => io::ErrorKind::Other,
//         };
//         io::Error::new(io_error_kind, self.source)
//     }
// }

impl From<StorageFromURIError> for StoreError {
    fn from(e: StorageFromURIError) -> Self {
        StoreError {
            kind: StoreErrorKind::URIError,
            source: anyhow::Error::from(e),
        }
    }
}

impl From<StoreError> for io::Error {
    fn from(store_err: StoreError) -> Self {
        let io_error_kind = match store_err.kind() {
            StoreErrorKind::DoesNotExist => io::ErrorKind::NotFound,
            _ => io::ErrorKind::Other,
        };
        io::Error::new(io_error_kind, store_err.source)
    }
}

#[derive(Error, Debug)]
#[error("StoreError(kind={kind:?}, source={source})")]
pub struct StoreError {
    kind: StoreErrorKind,
    #[source]
    source: anyhow::Error,
}

impl StoreError {
    pub fn add_context<C>(self, ctx: C) -> Self
    where
        C: fmt::Display + Send + Sync + 'static,
    {
        StoreError {
            kind: self.kind,
            source: self.source.context(ctx),
        }
    }
}

pub type StoreResult<T> = Result<T, StoreError>;

impl StoreError {
    pub fn kind(&self) -> StoreErrorKind {
        self.kind
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

impl<T> From<RusotoError<T>> for StoreError
where
    T: Send + Sync + StdErr + 'static + ToStoreErrorKind,
{
    fn from(err: RusotoError<T>) -> StoreError {
        let error_kind = match &err {
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

impl From<io::Error> for StoreError {
    fn from(err: io::Error) -> StoreError {
        StoreError {
            kind: StoreErrorKind::Io,
            source: anyhow::Error::from(err),
        }
    }
}
