// Copyright (C) 2022 Quickwit, Inc.
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

#![allow(clippy::match_like_matches_macro)]

use std::error::Error as StdError;
use std::{fmt, io};

use rusoto_core::RusotoError;
#[cfg(feature = "kinesis")]
use rusoto_kinesis::{
    CreateStreamError, DeleteStreamError, DescribeStreamError, GetRecordsError,
    GetShardIteratorError, ListShardsError, ListStreamsError, MergeShardsError, SplitShardError,
};
use rusoto_s3::{
    AbortMultipartUploadError, CompleteMultipartUploadError, CreateMultipartUploadError,
    DeleteObjectError, GetObjectError, HeadObjectError, PutObjectError, UploadPartError,
};

use crate::retry::Retryable;

pub struct RusotoErrorWrapper<T: Retryable + StdError>(pub RusotoError<T>);

impl<T: Retryable + StdError> From<RusotoError<T>> for RusotoErrorWrapper<T> {
    fn from(err: RusotoError<T>) -> Self {
        RusotoErrorWrapper(err)
    }
}

impl<T: Retryable + StdError + 'static> StdError for RusotoErrorWrapper<T> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.0)
    }
}

impl<T: Retryable + StdError> fmt::Debug for RusotoErrorWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl<T: Retryable + StdError + 'static> fmt::Display for RusotoErrorWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T: Retryable + StdError> From<io::Error> for RusotoErrorWrapper<T> {
    fn from(err: io::Error) -> Self {
        RusotoErrorWrapper::from(RusotoError::from(err))
    }
}

impl<T: Retryable + StdError> Retryable for RusotoErrorWrapper<T> {
    fn is_retryable(&self) -> bool {
        match &self.0 {
            RusotoError::HttpDispatch(_) => true,
            RusotoError::Service(service_error) => service_error.is_retryable(),
            RusotoError::Unknown(http_resp) => http_resp.status.is_server_error(),
            _ => false,
        }
    }
}

impl Retryable for GetObjectError {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for DeleteObjectError {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for UploadPartError {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for CompleteMultipartUploadError {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for AbortMultipartUploadError {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for CreateMultipartUploadError {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for PutObjectError {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for HeadObjectError {
    fn is_retryable(&self) -> bool {
        false
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for GetRecordsError {
    fn is_retryable(&self) -> bool {
        match self {
            GetRecordsError::KMSThrottling(_) => true,
            GetRecordsError::ProvisionedThroughputExceeded(_) => true,
            _ => false,
        }
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for GetShardIteratorError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            GetShardIteratorError::ProvisionedThroughputExceeded(_)
        )
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for ListShardsError {
    fn is_retryable(&self) -> bool {
        false
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for CreateStreamError {
    fn is_retryable(&self) -> bool {
        false
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for DeleteStreamError {
    fn is_retryable(&self) -> bool {
        false
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for DescribeStreamError {
    fn is_retryable(&self) -> bool {
        false
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for ListStreamsError {
    fn is_retryable(&self) -> bool {
        false
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for MergeShardsError {
    fn is_retryable(&self) -> bool {
        false
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for SplitShardError {
    fn is_retryable(&self) -> bool {
        false
    }
}
