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

use anyhow::Context as _;
use async_trait::async_trait;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_lambda::error::{DisplayErrorContext, SdkError};
use aws_sdk_lambda::operation::invoke::InvokeError;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::InvocationType;
use base64::prelude::*;
use prost::Message;
use quickwit_common::retry::{RetryParams, retry};
use quickwit_lambda_server::{LambdaSearchRequestPayload, LambdaSearchResponsePayload};
use quickwit_proto::search::{LambdaSearchResponses, LambdaSingleSplitResult, LeafSearchRequest};
use quickwit_search::{LambdaLeafSearchInvoker, SearchError};
use tracing::{debug, info, instrument};

use crate::metrics::LAMBDA_METRICS;

/// Maps an AWS Lambda SDK invocation error to a `SearchError`.
fn invoke_error_to_search_error(error: SdkError<InvokeError>) -> SearchError {
    if let SdkError::ServiceError(ref service_error) = error
        && matches!(
            service_error.err(),
            InvokeError::TooManyRequestsException(_)
                | InvokeError::EniLimitReachedException(_)
                | InvokeError::SubnetIpAddressLimitReachedException(_)
                | InvokeError::Ec2ThrottledException(_)
                | InvokeError::ResourceConflictException(_)
        )
    {
        return SearchError::TooManyRequests;
    }

    let is_timeout = match &error {
        SdkError::TimeoutError(_) => true,
        SdkError::DispatchFailure(failure) => failure.is_timeout(),
        SdkError::ServiceError(service_error) => matches!(
            service_error.err(),
            InvokeError::EfsMountTimeoutException(_) | InvokeError::SnapStartTimeoutException(_)
        ),
        _ => false,
    };

    let error_msg = format!("lambda invocation failed: {}", DisplayErrorContext(&error));

    if is_timeout {
        SearchError::Timeout(error_msg)
    } else {
        SearchError::Internal(error_msg)
    }
}

/// Create a Lambda invoker for a specific version.
///
/// The version number is used as the qualifier when invoking, ensuring we call
/// the exact published version (not $LATEST).
pub(crate) async fn create_lambda_invoker_for_version(
    function_name: String,
    version: String,
) -> anyhow::Result<AwsLambdaInvoker> {
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = LambdaClient::new(&aws_config);
    let invoker = AwsLambdaInvoker {
        client,
        function_name,
        version,
    };
    invoker.validate().await?;
    Ok(invoker)
}

/// AWS Lambda implementation of RemoteFunctionInvoker.
pub(crate) struct AwsLambdaInvoker {
    client: LambdaClient,
    function_name: String,
    /// The version number to invoke (e.g., "7", "12").
    version: String,
}

impl AwsLambdaInvoker {
    /// Validate that the Lambda function version exists and is invocable.
    /// Uses DryRun invocation type - validates without executing.
    async fn validate(&self) -> anyhow::Result<()> {
        info!("lambda invoker dry run");
        let request = self
            .client
            .invoke()
            .function_name(&self.function_name)
            .qualifier(&self.version)
            .invocation_type(InvocationType::DryRun);

        request.send().await.with_context(|| {
            format!(
                "failed to validate Lambda function '{}:{}'",
                self.function_name, self.version
            )
        })?;

        info!("the lambda invoker dry run was successful");
        Ok(())
    }
}

const LAMBDA_RETRY_PARAMS: RetryParams = RetryParams {
    base_delay: std::time::Duration::from_secs(1),
    max_delay: std::time::Duration::from_secs(10),
    max_attempts: 3,
};

#[async_trait]
impl LambdaLeafSearchInvoker for AwsLambdaInvoker {
    #[instrument(skip(self, request), fields(function_name = %self.function_name, version = %self.version))]
    async fn invoke_leaf_search(
        &self,
        request: LeafSearchRequest,
    ) -> Result<Vec<LambdaSingleSplitResult>, SearchError> {
        let start = std::time::Instant::now();

        let result = retry(&LAMBDA_RETRY_PARAMS, || {
            self.invoke_leaf_search_inner(request.clone())
        })
        .await;

        let elapsed = start.elapsed().as_secs_f64();
        let status = if result.is_ok() { "success" } else { "error" };
        LAMBDA_METRICS
            .leaf_search_requests_total
            .with_label_values([status])
            .inc();
        LAMBDA_METRICS
            .leaf_search_duration_seconds
            .with_label_values([status])
            .observe(elapsed);

        result
    }
}

impl AwsLambdaInvoker {
    async fn invoke_leaf_search_inner(
        &self,
        request: LeafSearchRequest,
    ) -> Result<Vec<LambdaSingleSplitResult>, SearchError> {
        // Serialize request to protobuf bytes, then base64 encode
        let request_bytes = request.encode_to_vec();
        let payload = LambdaSearchRequestPayload {
            payload: BASE64_STANDARD.encode(&request_bytes),
        };

        let payload_json = serde_json::to_vec(&payload)
            .map_err(|e| SearchError::Internal(format!("JSON serialization error: {}", e)))?;

        LAMBDA_METRICS
            .leaf_search_request_payload_size_bytes
            .observe(payload_json.len() as f64);

        debug!(
            payload_size = payload_json.len(),
            version = %self.version,
            "invoking Lambda function"
        );

        // Invoke the specific version
        let invoke_builder = self
            .client
            .invoke()
            .function_name(&self.function_name)
            .qualifier(&self.version)
            .invocation_type(InvocationType::RequestResponse)
            .payload(Blob::new(payload_json));

        let response = invoke_builder
            .send()
            .await
            .map_err(invoke_error_to_search_error)?;

        // Check for function error
        if let Some(error) = response.function_error() {
            let error_payload = response
                .payload()
                .map(|b| String::from_utf8_lossy(b.as_ref()).to_string())
                .unwrap_or_default();
            return Err(SearchError::Internal(format!(
                "lambda function error: {}: {}",
                error, error_payload
            )));
        }

        // Deserialize response
        let response_payload = response
            .payload()
            .ok_or_else(|| SearchError::Internal("no response payload from Lambda".into()))?;

        LAMBDA_METRICS
            .leaf_search_response_payload_size_bytes
            .observe(response_payload.as_ref().len() as f64);

        let lambda_response: LambdaSearchResponsePayload =
            serde_json::from_slice(response_payload.as_ref())
                .map_err(|e| SearchError::Internal(format!("json deserialization error: {}", e)))?;

        let response_bytes = BASE64_STANDARD
            .decode(&lambda_response.payload)
            .map_err(|e| SearchError::Internal(format!("base64 decode error: {}", e)))?;

        let leaf_responses = LambdaSearchResponses::decode(&response_bytes[..])
            .map_err(|e| SearchError::Internal(format!("protobuf decode error: {}", e)))?;

        debug!(
            num_results = leaf_responses.split_results.len(),
            "lambda invocation completed"
        );

        Ok(leaf_responses.split_results)
    }
}
