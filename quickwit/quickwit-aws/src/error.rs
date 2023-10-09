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
use quickwit_common::retry::Retryable;

pub struct RetryableWrapper<T>(T);

impl<E> Retryable for RetryableWrapper<SdkError<E>>
where E: Retryable
{
    fn is_retryable(&self) -> bool {
        match &self.0 {
            SdkError::ConstructionFailure(_) => false,
            SdkError::TimeoutError(_) => true,
            SdkError::DispatchFailure(_) => false,
            SdkError::ResponseError(_) => true,
            SdkError::ServiceError(error) => error.err().is_retryable(),
            _ => false,
        }
    }
}

impl Retryable for RetryableWrapper<GetObjectError> {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for RetryableWrapper<DeleteObjectError> {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for RetryableWrapper<DeleteObjectsError> {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for RetryableWrapper<UploadPartError> {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for RetryableWrapper<CompleteMultipartUploadError> {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for RetryableWrapper<AbortMultipartUploadError> {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for RetryableWrapper<CreateMultipartUploadError> {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for RetryableWrapper<PutObjectError> {
    fn is_retryable(&self) -> bool {
        false
    }
}

impl Retryable for RetryableWrapper<HeadObjectError> {
    fn is_retryable(&self) -> bool {
        false
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for RetryableWrapper<GetRecordsError> {
    fn is_retryable(&self) -> bool {
        match &self.0 {
            GetRecordsError::KmsThrottlingException(_) => true,
            GetRecordsError::ProvisionedThroughputExceededException(_) => true,
            _ => false,
        }
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for RetryableWrapper<GetShardIteratorError> {
    fn is_retryable(&self) -> bool {
        matches!(
            &self.0,
            GetShardIteratorError::ProvisionedThroughputExceededException(_)
        )
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for RetryableWrapper<ListShardsError> {
    fn is_retryable(&self) -> bool {
        matches!(
            &self.0,
            ListShardsError::ResourceInUseException(_) | ListShardsError::LimitExceededException(_)
        )
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for RetryableWrapper<CreateStreamError> {
    fn is_retryable(&self) -> bool {
        matches!(
            &self.0,
            CreateStreamError::ResourceInUseException(_)
                | CreateStreamError::LimitExceededException(_)
        )
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for RetryableWrapper<DeleteStreamError> {
    fn is_retryable(&self) -> bool {
        matches!(
            &self.0,
            DeleteStreamError::ResourceInUseException(_)
                | DeleteStreamError::LimitExceededException(_)
        )
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for RetryableWrapper<DescribeStreamError> {
    fn is_retryable(&self) -> bool {
        matches!(&self.0, DescribeStreamError::LimitExceededException(_))
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for RetryableWrapper<ListStreamsError> {
    fn is_retryable(&self) -> bool {
        matches!(&self.0, ListStreamsError::LimitExceededException(_))
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for RetryableWrapper<MergeShardsError> {
    fn is_retryable(&self) -> bool {
        matches!(
            &self.0,
            MergeShardsError::ResourceInUseException(_)
                | MergeShardsError::LimitExceededException(_)
        )
    }
}

#[cfg(feature = "kinesis")]
impl Retryable for RetryableWrapper<SplitShardError> {
    fn is_retryable(&self) -> bool {
        matches!(
            &self.0,
            SplitShardError::ResourceInUseException(_) | SplitShardError::LimitExceededException(_)
        )
    }
}
