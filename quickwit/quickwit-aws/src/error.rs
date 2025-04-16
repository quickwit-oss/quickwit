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
        THROTTLING_ERRORS.contains(&code)
            || TRANSIENT_ERRORS.contains(&code)
            || code == "InternalError" // this is somehow not considered transient, despite the
    // associated error message containing "Please try again."
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
