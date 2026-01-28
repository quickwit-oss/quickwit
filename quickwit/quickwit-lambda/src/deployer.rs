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

//! Lambda function deployment for auto-deploy feature.
//!
//! This module provides functionality to automatically deploy or update
//! the Lambda function used for leaf search operations.

#[cfg(feature = "auto-deploy")]
use std::collections::HashMap;

use aws_sdk_lambda::Client as LambdaClient;
#[cfg(feature = "auto-deploy")]
use aws_sdk_lambda::error::SdkError;
#[cfg(feature = "auto-deploy")]
use aws_sdk_lambda::operation::create_function::CreateFunctionError;
#[cfg(feature = "auto-deploy")]
use aws_sdk_lambda::operation::get_function::GetFunctionOutput;
#[cfg(feature = "auto-deploy")]
use aws_sdk_lambda::primitives::Blob;
#[cfg(feature = "auto-deploy")]
use aws_sdk_lambda::types::{Architecture, Environment, FunctionCode, Runtime};
#[cfg(feature = "auto-deploy")]
use tracing::{debug, info, warn};

use crate::config::LambdaDeployConfig;
use crate::error::{LambdaError, LambdaResult};

/// Embedded Lambda binary (arm64, compressed).
/// This is included at compile time when the `auto-deploy` feature is enabled.
#[cfg(feature = "auto-deploy")]
const LAMBDA_BINARY: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/lambda_bootstrap.zip"));

/// Version tag key used to track deployed Quickwit version.
#[cfg(feature = "auto-deploy")]
const VERSION_TAG_KEY: &str = "quickwit_version";

/// Description prefix for auto-deployed Lambda functions.
#[cfg(feature = "auto-deploy")]
const FUNCTION_DESCRIPTION: &str = "Quickwit Lambda leaf search handler";

/// Lambda function deployer.
///
/// Handles creating and updating Lambda functions for the auto-deploy feature.
/// Safe for concurrent calls from multiple Quickwit nodes - CreateFunction is idempotent.
pub struct LambdaDeployer {
    #[allow(dead_code)]
    client: LambdaClient,
}

impl LambdaDeployer {
    /// Create a new Lambda deployer using default AWS configuration.
    pub async fn new() -> LambdaResult<Self> {
        let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = LambdaClient::new(&aws_config);
        Ok(Self { client })
    }

    /// Create a new Lambda deployer with a custom client.
    #[allow(dead_code)]
    pub fn with_client(client: LambdaClient) -> Self {
        Self { client }
    }

    /// Deploy or update the Lambda function.
    ///
    /// Safe for concurrent calls from multiple Quickwit nodes - CreateFunction is idempotent.
    /// Returns the function ARN.
    #[cfg(feature = "auto-deploy")]
    pub async fn deploy(
        &self,
        function_name: &str,
        deploy_config: &LambdaDeployConfig,
    ) -> LambdaResult<String> {
        let role_arn = &deploy_config.execution_role_arn;

        match self.get_function(function_name).await {
            Ok(existing) => {
                self.update_function_if_needed(function_name, &existing, deploy_config)
                    .await
            }
            Err(LambdaError::NotFound(_)) => {
                // Function doesn't exist, try to create it
                match self
                    .create_function(function_name, role_arn, deploy_config)
                    .await
                {
                    Ok(arn) => Ok(arn),
                    Err(LambdaError::ResourceConflict) => {
                        // Another node created the function concurrently, update instead
                        info!(
                            function_name = %function_name,
                            "function was created concurrently by another node, updating instead"
                        );
                        let existing = self.get_function(function_name).await?;
                        self.update_function_if_needed(function_name, &existing, deploy_config)
                            .await
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Deploy is a no-op when auto-deploy feature is not enabled.
    #[cfg(not(feature = "auto-deploy"))]
    pub async fn deploy(
        &self,
        _function_name: &str,
        _deploy_config: &LambdaDeployConfig,
    ) -> LambdaResult<String> {
        Err(LambdaError::Configuration(
            "auto-deploy feature is not enabled at compile time".into(),
        ))
    }

    /// Create the Lambda function.
    ///
    /// Note: CreateFunction is idempotent - if the function already exists, AWS returns
    /// ResourceConflictException. Multiple Quickwit nodes starting simultaneously is safe;
    /// one will succeed and others will fall back to update_function.
    #[cfg(feature = "auto-deploy")]
    async fn create_function(
        &self,
        name: &str,
        role: &str,
        config: &LambdaDeployConfig,
    ) -> LambdaResult<String> {
        info!(
            function_name = %name,
            role = %role,
            memory_mb = config.memory_size_mb,
            timeout_secs = config.invocation_timeout_secs,
            "creating Lambda function"
        );

        let environment = self.build_environment();
        let tags = self.build_tags();

        let result = self
            .client
            .create_function()
            .function_name(name)
            .runtime(Runtime::Providedal2023)
            .architectures(Architecture::Arm64)
            .handler("bootstrap")
            .role(role)
            .code(
                FunctionCode::builder()
                    .zip_file(Blob::new(LAMBDA_BINARY))
                    .build(),
            )
            .memory_size(config.memory_size_mb as i32)
            .timeout(config.invocation_timeout_secs as i32)
            .environment(environment)
            .description(format!(
                "{} (v{})",
                FUNCTION_DESCRIPTION,
                env!("CARGO_PKG_VERSION")
            ))
            .set_tags(Some(tags))
            .send()
            .await;

        match result {
            Ok(output) => {
                let arn = output
                    .function_arn()
                    .ok_or_else(|| LambdaError::Deployment("no function ARN returned".into()))?
                    .to_string();
                info!(function_arn = %arn, "Lambda function created successfully");
                Ok(arn)
            }
            Err(SdkError::ServiceError(err))
                if matches!(err.err(), CreateFunctionError::ResourceConflictException(_)) =>
            {
                Err(LambdaError::ResourceConflict)
            }
            Err(e) => Err(LambdaError::Deployment(format!(
                "failed to create function: {}",
                e
            ))),
        }
    }

    /// Update the Lambda function if needed.
    ///
    /// Compares the deployed version tag with the current Quickwit version
    /// and updates if they differ.
    #[cfg(feature = "auto-deploy")]
    async fn update_function_if_needed(
        &self,
        name: &str,
        existing: &GetFunctionOutput,
        config: &LambdaDeployConfig,
    ) -> LambdaResult<String> {
        let function_arn = existing
            .configuration()
            .and_then(|c| c.function_arn())
            .ok_or_else(|| LambdaError::Deployment("no function ARN in existing config".into()))?
            .to_string();

        if !self.needs_update(existing) {
            debug!(
                function_name = %name,
                "Lambda function is up to date, skipping update"
            );
            return Ok(function_arn);
        }

        info!(
            function_name = %name,
            "updating Lambda function to version {}",
            env!("CARGO_PKG_VERSION")
        );

        // Update function code
        self.client
            .update_function_code()
            .function_name(name)
            .zip_file(Blob::new(LAMBDA_BINARY))
            .architectures(Architecture::Arm64)
            .send()
            .await
            .map_err(|e| {
                LambdaError::Deployment(format!("failed to update function code: {}", e))
            })?;

        // Wait for the update to complete before updating configuration
        self.wait_for_update_complete(name).await?;

        // Update function configuration
        self.client
            .update_function_configuration()
            .function_name(name)
            .memory_size(config.memory_size_mb as i32)
            .timeout(config.invocation_timeout_secs as i32)
            .environment(self.build_environment())
            .description(format!(
                "{} (v{})",
                FUNCTION_DESCRIPTION,
                env!("CARGO_PKG_VERSION")
            ))
            .send()
            .await
            .map_err(|e| {
                LambdaError::Deployment(format!("failed to update function configuration: {}", e))
            })?;

        // Wait for config update to complete before updating tags
        self.wait_for_update_complete(name).await?;

        // Update tags
        self.client
            .tag_resource()
            .resource(function_arn.clone())
            .set_tags(Some(self.build_tags()))
            .send()
            .await
            .map_err(|e| {
                LambdaError::Deployment(format!("failed to update function tags: {}", e))
            })?;

        info!(function_arn = %function_arn, "Lambda function updated successfully");
        Ok(function_arn)
    }

    /// Wait for function update to complete.
    #[cfg(feature = "auto-deploy")]
    async fn wait_for_update_complete(&self, name: &str) -> LambdaResult<()> {
        // Poll until the function state is Active and LastUpdateStatus is Successful
        for _ in 0..60 {
            let output = self.get_function(name).await?;
            if let Some(config) = output.configuration() {
                let state = config.state();
                let last_update_status = config.last_update_status();

                use aws_sdk_lambda::types::{LastUpdateStatus, State};
                match (state, last_update_status) {
                    (Some(State::Active), Some(LastUpdateStatus::Successful)) => {
                        return Ok(());
                    }
                    (Some(State::Failed), _) | (_, Some(LastUpdateStatus::Failed)) => {
                        let reason = config
                            .last_update_status_reason()
                            .unwrap_or("unknown reason");
                        return Err(LambdaError::Deployment(format!(
                            "function update failed: {}",
                            reason
                        )));
                    }
                    _ => {
                        debug!(
                            function_name = %name,
                            state = ?state,
                            last_update_status = ?last_update_status,
                            "waiting for function update to complete"
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }
        }
        Err(LambdaError::Deployment(
            "timeout waiting for function update to complete".into(),
        ))
    }

    /// Check if the function needs to be updated based on version tag.
    #[cfg(feature = "auto-deploy")]
    fn needs_update(&self, existing: &GetFunctionOutput) -> bool {
        let current_version = env!("CARGO_PKG_VERSION");

        let deployed_version = existing.tags().and_then(|t| t.get(VERSION_TAG_KEY));

        match deployed_version {
            Some(version) if version == current_version => {
                debug!(
                    deployed_version = %version,
                    current_version = %current_version,
                    "versions match, no update needed"
                );
                false
            }
            Some(version) => {
                info!(
                    deployed_version = %version,
                    current_version = %current_version,
                    "version mismatch, update needed"
                );
                true
            }
            None => {
                warn!("no version tag found on deployed function, update needed");
                true
            }
        }
    }

    /// Get function details from AWS.
    #[cfg(feature = "auto-deploy")]
    async fn get_function(&self, name: &str) -> LambdaResult<GetFunctionOutput> {
        self.client
            .get_function()
            .function_name(name)
            .send()
            .await
            .map_err(|e| {
                // Check if it's a not found error
                if e.to_string().contains("ResourceNotFoundException")
                    || e.to_string().contains("Function not found")
                {
                    LambdaError::NotFound(name.to_string())
                } else {
                    LambdaError::Deployment(format!("failed to get function: {}", e))
                }
            })
    }

    /// Build environment variables for the Lambda function.
    #[cfg(feature = "auto-deploy")]
    fn build_environment(&self) -> Environment {
        let mut env_vars = HashMap::new();
        // Set reasonable defaults for logging
        env_vars.insert("RUST_LOG".to_string(), "info".to_string());
        env_vars.insert("RUST_BACKTRACE".to_string(), "1".to_string());

        Environment::builder().set_variables(Some(env_vars)).build()
    }

    /// Build tags for the Lambda function.
    #[cfg(feature = "auto-deploy")]
    fn build_tags(&self) -> HashMap<String, String> {
        let mut tags = HashMap::new();
        tags.insert(
            VERSION_TAG_KEY.to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        );
        tags.insert("managed_by".to_string(), "quickwit".to_string());
        tags
    }
}

#[cfg(all(test, feature = "auto-deploy"))]
mod tests {
    use super::*;

    #[test]
    fn test_build_tags() {
        let deployer = LambdaDeployer {
            client: LambdaClient::from_conf(
                aws_sdk_lambda::Config::builder()
                    .behavior_version(aws_sdk_lambda::config::BehaviorVersion::latest())
                    .build(),
            ),
        };
        let tags = deployer.build_tags();
        assert!(tags.contains_key(VERSION_TAG_KEY));
        assert_eq!(tags.get("managed_by"), Some(&"quickwit".to_string()));
    }

    #[test]
    fn test_build_environment() {
        let deployer = LambdaDeployer {
            client: LambdaClient::from_conf(
                aws_sdk_lambda::Config::builder()
                    .behavior_version(aws_sdk_lambda::config::BehaviorVersion::latest())
                    .build(),
            ),
        };
        let env = deployer.build_environment();
        let vars = env.variables().unwrap();
        assert_eq!(vars.get("RUST_LOG"), Some(&"info".to_string()));
        assert_eq!(vars.get("RUST_BACKTRACE"), Some(&"1".to_string()));
    }
}
