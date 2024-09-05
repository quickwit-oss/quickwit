// Copyright (C) 2024 Quickwit, Inc.
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

use aws_runtime::retries::classifiers::{THROTTLING_ERRORS, TRANSIENT_ERRORS};
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

use crate::retry::AwsRetryable;

impl<E> AwsRetryable for SdkError<E>
where E: AwsRetryable
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

fn is_retryable(meta: &aws_sdk_s3::error::ErrorMetadata) -> bool {
    if let Some(code) = meta.code() {
        THROTTLING_ERRORS.contains(&code) || TRANSIENT_ERRORS.contains(&code)
    } else {
        false
    }
}

impl AwsRetryable for GetObjectError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for DeleteObjectError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for DeleteObjectsError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for UploadPartError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for CompleteMultipartUploadError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for AbortMultipartUploadError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for CreateMultipartUploadError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for PutObjectError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

impl AwsRetryable for HeadObjectError {
    fn is_retryable(&self) -> bool {
        is_retryable(self.meta())
    }
}

#[cfg(feature = "kinesis")]
mod kinesis {
    use aws_sdk_kinesis::operation::create_stream::CreateStreamError;
    use aws_sdk_kinesis::operation::delete_stream::DeleteStreamError;
    use aws_sdk_kinesis::operation::describe_stream::DescribeStreamError;
    use aws_sdk_kinesis::operation::get_records::GetRecordsError;
    use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorError;
    use aws_sdk_kinesis::operation::list_shards::ListShardsError;
    use aws_sdk_kinesis::operation::list_streams::ListStreamsError;
    use aws_sdk_kinesis::operation::merge_shards::MergeShardsError;
    use aws_sdk_kinesis::operation::split_shard::SplitShardError;

    use super::*;

    impl AwsRetryable for GetRecordsError {
        fn is_retryable(&self) -> bool {
            match self {
                GetRecordsError::KmsThrottlingException(_) => true,
                GetRecordsError::ProvisionedThroughputExceededException(_) => true,
                _ => false,
            }
        }
    }

    impl AwsRetryable for GetShardIteratorError {
        fn is_retryable(&self) -> bool {
            matches!(
                self,
                GetShardIteratorError::ProvisionedThroughputExceededException(_)
            )
        }
    }

    impl AwsRetryable for ListShardsError {
        fn is_retryable(&self) -> bool {
            matches!(
                self,
                ListShardsError::ResourceInUseException(_)
                    | ListShardsError::LimitExceededException(_)
            )
        }
    }

    impl AwsRetryable for CreateStreamError {
        fn is_retryable(&self) -> bool {
            matches!(
                self,
                CreateStreamError::ResourceInUseException(_)
                    | CreateStreamError::LimitExceededException(_)
            )
        }
    }

    impl AwsRetryable for DeleteStreamError {
        fn is_retryable(&self) -> bool {
            matches!(
                self,
                DeleteStreamError::ResourceInUseException(_)
                    | DeleteStreamError::LimitExceededException(_)
            )
        }
    }

    impl AwsRetryable for DescribeStreamError {
        fn is_retryable(&self) -> bool {
            matches!(self, DescribeStreamError::LimitExceededException(_))
        }
    }

    impl AwsRetryable for ListStreamsError {
        fn is_retryable(&self) -> bool {
            matches!(self, ListStreamsError::LimitExceededException(_))
        }
    }

    impl AwsRetryable for MergeShardsError {
        fn is_retryable(&self) -> bool {
            matches!(
                self,
                MergeShardsError::ResourceInUseException(_)
                    | MergeShardsError::LimitExceededException(_)
            )
        }
    }

    impl AwsRetryable for SplitShardError {
        fn is_retryable(&self) -> bool {
            matches!(
                self,
                SplitShardError::ResourceInUseException(_)
                    | SplitShardError::LimitExceededException(_)
            )
        }
    }
}

#[cfg(feature = "sqs")]
mod sqs {
    use aws_sdk_sqs::operation::change_message_visibility::ChangeMessageVisibilityError;
    use aws_sdk_sqs::operation::delete_message_batch::DeleteMessageBatchError;
    use aws_sdk_sqs::operation::receive_message::ReceiveMessageError;

    use super::*;

    impl AwsRetryable for ReceiveMessageError {
        fn is_retryable(&self) -> bool {
            false
        }
    }

    impl AwsRetryable for DeleteMessageBatchError {
        fn is_retryable(&self) -> bool {
            false
        }
    }

    impl AwsRetryable for ChangeMessageVisibilityError {
        fn is_retryable(&self) -> bool {
            false
        }
    }
}
