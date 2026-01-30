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

use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::InvocationType;
use base64::prelude::*;
use prost::Message;
use quickwit_config::LambdaConfig;
use quickwit_lambda_server::{LeafSearchPayload, LeafSearchResponsePayload};
use quickwit_proto::search::{LeafSearchRequest, LeafSearchResponse};
use quickwit_search::{RemoteFunctionInvoker, SearchError};
use tracing::{debug, instrument};

use crate::error::{LambdaClientError, LambdaClientResult};

/// Create a Lambda invoker for remote leaf search execution.
///
/// This creates and validates an AWS Lambda invoker that implements `RemoteFunctionInvoker`.
pub async fn create_lambda_invoker(
    config: &LambdaConfig,
) -> LambdaClientResult<Arc<dyn RemoteFunctionInvoker>> {
    let invoker = AwsLambdaInvoker::new(config).await?;
    invoker.validate().await?;
    Ok(Arc::new(invoker))
}

/// AWS Lambda implementation of RemoteFunctionInvoker.
struct AwsLambdaInvoker {
    client: LambdaClient,
    function_name: String,
}

impl AwsLambdaInvoker {
    /// Create a new AWS Lambda invoker with the given configuration.
    async fn new(config: &LambdaConfig) -> LambdaClientResult<Self> {
        let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = LambdaClient::new(&aws_config);

        Ok(Self {
            client,
            function_name: config.function_name.clone(),
        })
    }

    /// Validate that the Lambda function exists and is invocable.
    /// Uses DryRun invocation type - validates without executing.
    async fn validate(&self) -> LambdaClientResult<()> {
        let request = self
            .client
            .invoke()
            .function_name(&self.function_name)
            .invocation_type(InvocationType::DryRun);
        request.send().await.map_err(|e| {
            LambdaClientError::Configuration(format!(
                "Failed to validate Lambda function '{}': {}",
                self.function_name, e
            ))
        })?;

        Ok(())
    }
}

#[async_trait]
impl RemoteFunctionInvoker for AwsLambdaInvoker {
    #[instrument(skip(self, request), fields(function_name = %self.function_name))]
    async fn invoke_leaf_search(
        &self,
        request: LeafSearchRequest,
    ) -> Result<LeafSearchResponse, SearchError> {
        // Serialize request to protobuf bytes, then base64 encode
        let request_bytes = request.encode_to_vec();
        let payload = LeafSearchPayload {
            payload: BASE64_STANDARD.encode(&request_bytes),
        };

        let payload_json = serde_json::to_vec(&payload)
            .map_err(|e| SearchError::Internal(format!("JSON serialization error: {}", e)))?;

        debug!(
            payload_size = payload_json.len(),
            "Invoking Lambda function"
        );

        // Invoke Lambda synchronously (RequestResponse)
        let invoke_builder = self
            .client
            .invoke()
            .function_name(&self.function_name)
            .invocation_type(InvocationType::RequestResponse)
            .payload(Blob::new(payload_json));
        let response = invoke_builder
            .send()
            .await
            .map_err(|e| SearchError::Internal(format!("Lambda invocation error: {}", e)))?;

        // Check for function error
        if let Some(error) = response.function_error() {
            let error_payload = response
                .payload()
                .map(|b| String::from_utf8_lossy(b.as_ref()).to_string())
                .unwrap_or_default();
            return Err(SearchError::Internal(format!(
                "Lambda function error: {}: {}",
                error, error_payload
            )));
        }

        // Deserialize response
        let response_payload = response
            .payload()
            .ok_or_else(|| SearchError::Internal("No response payload from Lambda".into()))?;

        let lambda_response: LeafSearchResponsePayload =
            serde_json::from_slice(response_payload.as_ref())
                .map_err(|e| SearchError::Internal(format!("JSON deserialization error: {}", e)))?;

        let response_bytes = BASE64_STANDARD
            .decode(&lambda_response.payload)
            .map_err(|e| SearchError::Internal(format!("Base64 decode error: {}", e)))?;

        let leaf_response = LeafSearchResponse::decode(&response_bytes[..])
            .map_err(|e| SearchError::Internal(format!("Protobuf decode error: {}", e)))?;

        debug!(
            num_hits = leaf_response.num_hits,
            "Lambda invocation completed"
        );

        Ok(leaf_response)
    }
}
