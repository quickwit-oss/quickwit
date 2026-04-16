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

pub mod index_router;
pub(crate) mod log_msg_accessors;
pub mod rest_handler;

pub use index_router::IndexRouter;
#[cfg(any(test, feature = "testsuite"))]
pub use log_msg_accessors::{custom_field_accessor, tag_accessor};
use quickwit_proto::ServiceErrorCode;
use quickwit_proto::ingest::router::IngestFailureReason;
pub use rest_handler::DatadogApi;
pub(crate) use rest_handler::datadog_api_handlers;

// FIXME: This should be upstreamed to quickwit-proto.
pub(crate) fn get_error_code(failure_reason: IngestFailureReason) -> ServiceErrorCode {
    quickwit_common::rate_limited_error!(limit_per_min = 6, error = failure_reason.as_str_name());

    match failure_reason {
        IngestFailureReason::Unspecified => ServiceErrorCode::Internal,
        IngestFailureReason::IndexNotFound => ServiceErrorCode::NotFound,
        IngestFailureReason::SourceNotFound => ServiceErrorCode::NotFound,
        IngestFailureReason::Internal => ServiceErrorCode::Internal,
        IngestFailureReason::NoShardsAvailable => ServiceErrorCode::TooManyRequests,
        IngestFailureReason::ShardRateLimited => ServiceErrorCode::TooManyRequests,
        IngestFailureReason::WalFull => ServiceErrorCode::Internal,
        IngestFailureReason::Timeout => ServiceErrorCode::Timeout,
        IngestFailureReason::RouterLoadShedding => ServiceErrorCode::Internal,
        IngestFailureReason::LoadShedding => ServiceErrorCode::Internal,
        IngestFailureReason::CircuitBreaker => ServiceErrorCode::Internal,
    }
}

pub(crate) fn get_error_code_and_message(
    failure_reason: IngestFailureReason,
) -> (ServiceErrorCode, &'static str) {
    match failure_reason {
        IngestFailureReason::Unspecified => (ServiceErrorCode::Internal, "unknown error"),
        IngestFailureReason::IndexNotFound => (ServiceErrorCode::NotFound, "index not found"),
        IngestFailureReason::SourceNotFound => (ServiceErrorCode::NotFound, "source not found"),
        IngestFailureReason::Internal => (ServiceErrorCode::Internal, "internal error"),
        IngestFailureReason::NoShardsAvailable => (
            ServiceErrorCode::TooManyRequests,
            "too many requests (no shards available)",
        ),
        IngestFailureReason::ShardRateLimited => (
            ServiceErrorCode::TooManyRequests,
            "too many requests (rate limiting)",
        ),
        IngestFailureReason::WalFull => (ServiceErrorCode::Internal, "WAL full"),
        IngestFailureReason::Timeout => (ServiceErrorCode::Timeout, "request timed out"),
        IngestFailureReason::RouterLoadShedding => {
            (ServiceErrorCode::Internal, "router load shedding")
        }
        IngestFailureReason::LoadShedding => (ServiceErrorCode::Internal, "load shedding)"),
        IngestFailureReason::CircuitBreaker => (ServiceErrorCode::Internal, "circuit breaker)"),
    }
}
