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

#![allow(clippy::match_like_matches_macro)]

#[cfg(feature = "kinesis")]
use aws_sdk_kinesis::operation::{
    create_stream::CreateStreamError, delete_stream::DeleteStreamError,
    describe_stream::DescribeStreamError, get_records::GetRecordsError,
    get_shard_iterator::GetShardIteratorError, list_shards::ListShardsError,
    list_streams::ListStreamsError, merge_shards::MergeShardsError, split_shard::SplitShardError,
};
use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_smithy_client::SdkError;

use crate::retry::Retryable;

impl<E> Retryable for SdkError<E>
where E: Retryable
{
    fn is_retryable(&self) -> bool {
        match self {
            SdkError::ConstructionFailure(_) => false,
            SdkError::TimeoutError(_) => true,
            SdkError::DispatchFailure(_) => false,
            SdkError::ResponseError(_) => true,
            SdkError::ServiceError(error) => error.err().is_retryable(),
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

impl Retryable for DeleteObjectsError {
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
            GetRecordsError::KmsThrottlingException(_) => true,
            GetRecordsError::ProvisionedThroughputExceededException(_) => true,
            _ => false,
        }
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for GetShardIteratorError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            GetShardIteratorError::ProvisionedThroughputExceededException(_)
        )
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for ListShardsError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            ListShardsError::ResourceInUseException(_) | ListShardsError::LimitExceededException(_)
        )
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for CreateStreamError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            CreateStreamError::ResourceInUseException(_)
                | CreateStreamError::LimitExceededException(_)
        )
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for DeleteStreamError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            DeleteStreamError::ResourceInUseException(_)
                | DeleteStreamError::LimitExceededException(_)
        )
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for DescribeStreamError {
    fn is_retryable(&self) -> bool {
        matches!(self, DescribeStreamError::LimitExceededException(_))
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for ListStreamsError {
    fn is_retryable(&self) -> bool {
        matches!(self, ListStreamsError::LimitExceededException(_))
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for MergeShardsError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            MergeShardsError::ResourceInUseException(_)
                | MergeShardsError::LimitExceededException(_)
        )
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for SplitShardError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            SplitShardError::ResourceInUseException(_) | SplitShardError::LimitExceededException(_)
        )
    }
}
