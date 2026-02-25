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
//!
//! # Versioning Strategy
//!
//! We use AWS Lambda published versions with description-based identification:
//! - Each published version has a description like `quickwit:0_8_0-fa752891`
//! - We list versions to find one matching our qualifier
//! - We invoke the specific version number (not $LATEST)
//! - Old versions are garbage collected (keep current + top 5 most recent)

use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::{Context, anyhow};
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_lambda::error::SdkError;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{
    Architecture, Environment, FunctionCode, LastUpdateStatus, Runtime, State,
};
use quickwit_config::{LambdaConfig, LambdaDeployConfig};
use quickwit_search::LambdaLeafSearchInvoker;
use tracing::{debug, info};

use crate::invoker::create_lambda_invoker_for_version;

/// Embedded Lambda binary (arm64, compressed).
/// This is included at compile time.
const LAMBDA_BINARY: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/lambda_bootstrap.zip"));

/// Prefix for version descriptions to identify Quickwit-managed versions.
const VERSION_DESCRIPTION_PREFIX: &str = "quickwit";

/// Number of recent versions to keep during garbage collection (in addition to current).
const GC_KEEP_RECENT_VERSIONS: usize = 5;

/// Returns the Lambda qualifier combining version and binary hash.
/// Format: "{quickwit_version}-{hash_short}" with dots replaced by underscores.
/// Example: "0_8_0-fa752891"
fn lambda_qualifier() -> &'static str {
    static LAMBDA_QUALIFIER: OnceLock<String> = OnceLock::new();
    LAMBDA_QUALIFIER
        .get_or_init(|| {
            format!(
                "{}_{}",
                env!("CARGO_PKG_VERSION").replace('.', "_"),
                env!("LAMBDA_BINARY_HASH")
            )
        })
        .as_str()
}

/// Returns the version description for our qualifier.
///
/// We also pass the deploy config, as we want the function to be redeployed
/// if the deploy config is changed.
fn version_description(deploy_config_opt: Option<&LambdaDeployConfig>) -> String {
    if let Some(deploy_config) = deploy_config_opt {
        let memory_size_mib = deploy_config.memory_size.as_mib() as u64;
        let execution_role_arn_digest: String = format!(
            "{:x}",
            md5::compute(deploy_config.execution_role_arn.as_bytes())
        );
        format!(
            "{}_{}_{}_{}s_{}",
            VERSION_DESCRIPTION_PREFIX,
            lambda_qualifier(),
            memory_size_mib,
            deploy_config.invocation_timeout_secs,
            &execution_role_arn_digest[..5]
        )
    } else {
        format!(
            "{}_{}_nodeploy",
            VERSION_DESCRIPTION_PREFIX,
            lambda_qualifier()
        )
    }
}

/// Get or deploy the Lambda function and return an invoker.
///
/// This function:
/// 1. Lists existing Lambda versions to find one matching our description
/// 2. If not found, (and if a deploy config is provided) attempt to deploy the embedded Lambda
///    binary
/// 3. Garbage collects old versions (keeps current + 5 most recent)
/// 4. Returns an invoker configured to call the specific version
///
/// The qualifier is computed from the Quickwit version and Lambda binary hash,
/// ensuring the deployed Lambda matches the embedded binary.
pub async fn try_get_or_deploy_invoker(
    lambda_config: &LambdaConfig,
) -> anyhow::Result<impl LambdaLeafSearchInvoker> {
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = LambdaClient::new(&aws_config);
    let function_name = &lambda_config.function_name;
    let target_description = version_description(lambda_config.auto_deploy.as_ref());

    info!(
        function_name = %function_name,
        qualifier = %lambda_qualifier(),
        "looking for Lambda function version"
    );

    let version = find_or_deploy_version(
        &client,
        function_name,
        &target_description,
        lambda_config.auto_deploy.as_ref(),
    )
    .await?;

    // Spawn background garbage collection (best effort, non-blocking)
    let gc_client = client.clone();
    let gc_function_name = function_name.clone();
    let gc_version = version.clone();
    tokio::spawn(async move {
        if let Err(e) =
            garbage_collect_old_versions(&gc_client, &gc_function_name, &gc_version).await
        {
            info!(error = %e, "failed to garbage collect old Lambda versions");
        }
    });

    // Create and return the invoker
    let invoker = create_lambda_invoker_for_version(function_name.clone(), version)
        .await
        .context("failed to create Lambda invoker")?;

    info!("created the lambda invoker");

    Ok(invoker)
}

/// Find a Lambda version with a description matching our qualifier.
///
/// If none is found and a deploy config is provided, attempt to deploy a new version.
///
/// Returns the version number as a string (because it is a string on AWS side, e.g.: "7") if found.
async fn find_or_deploy_version(
    client: &LambdaClient,
    function_name: &str,
    target_description: &str,
    deploy_config: Option<&LambdaDeployConfig>,
) -> anyhow::Result<String> {
    if let Some(version) = find_matching_version(client, function_name, target_description).await? {
        info!(
            function_name = %function_name,
            version = %version,
            "found existing Lambda version"
        );
        return Ok(version);
    }

    let deploy_config = deploy_config.with_context(|| {
        format!(
            "no Lambda version found with description '{}' and auto_deploy is not configured. \
             Either deploy the Lambda function manually or enable auto_deploy.",
            target_description
        )
    })?;

    info!(
        function_name = %function_name,
        "no matching version found, deploying Lambda function"
    );

    deploy_lambda_function(client, function_name, deploy_config).await
}

async fn find_matching_version(
    client: &LambdaClient,
    function_name: &str,
    target_description: &str,
) -> anyhow::Result<Option<String>> {
    let mut marker: Option<String> = None;

    loop {
        let mut request = client
            .list_versions_by_function()
            .function_name(function_name);

        if let Some(m) = marker {
            request = request.marker(m);
        }

        let response = match request.send().await {
            Ok(resp) => resp,
            Err(SdkError::ServiceError(err)) if err.err().is_resource_not_found_exception() => {
                info!(
                    function_name = %function_name,
                    "lambda function does not exist yet"
                );
                return Ok(None);
            }
            Err(e) => {
                return Err(anyhow!(
                    "failed to list Lambda versions for '{}': {}",
                    function_name,
                    e
                ));
            }
        };

        for version in response.versions() {
            if let Some(description) = version.description()
                && description == target_description
                && let Some(ver) = version.version()
                && ver != "$LATEST"
            {
                return Ok(Some(ver.to_string()));
            }
        }

        marker = response.next_marker().map(|s| s.to_string());
        if marker.is_none() {
            break;
        }
    }

    Ok(None)
}

/// Deploy the Lambda function and publish a new version.
/// AWS's API is pretty terrible.
///
/// Lambda's version are integer generated by AWS (we don't have control over them).
/// To publish a new version, we need to implement two paths:
/// - If the function doesn't exist yet, `create_function(publish=true)` atomically creates it and
///   publishes a version in one call.
/// - If the function already exists, we first update the code. We do not publish because strangely
///   the API call does not make it possible to change the description. Updating the code has the
///   effect of create a version $LATEST.
/// - We publish the version $LATEST. That's the moment AWS attributes a version number. That call
///   allows us to change the description. We pass the sha256 hash of the code to ensure that
///   $LATEST has not been overwritten by another concurrent update.
async fn deploy_lambda_function(
    client: &LambdaClient,
    function_name: &str,
    deploy_config: &LambdaDeployConfig,
) -> anyhow::Result<String> {
    // This looks overly complicated but this is not AI slop.
    // The AWS API forces us to go through a bunch of hoops to update our function
    // in a safe manner.
    let description = version_description(Some(deploy_config));

    // Fast path: if the function does not exist, we can create and publish the function atomically.
    if let Some(version) =
        try_create_function(client, function_name, deploy_config, &description).await?
    {
        return Ok(version);
    }

    // Function already exists — we need to update the code.
    // This will create or update a version called "$LATEST" (that's the actual string)
    //
    // We cannot directly publish here, because updating the function code does not allow
    // us to pass a different description.
    let code_sha256 = update_function_code(client, function_name).await?;

    // We can now publish that new uploaded version.
    // We pass the code_sha256 guard to make sure a race condition does not cause
    // us to publish a different version.
    //
    // Publishing will create an actual version (a number as a string) and return it.
    publish_version(client, function_name, &code_sha256, &description).await
}

/// Try to create the Lambda function with `publish=true`.
///
/// Returns `Some(version)` if the function was created and published.
/// Returns `None` if the function already exists (`ResourceConflictException`).
async fn try_create_function(
    client: &LambdaClient,
    function_name: &str,
    deploy_config: &LambdaDeployConfig,
    description: &str,
) -> anyhow::Result<Option<String>> {
    let memory_size_mb = deploy_config
        .memory_size
        .as_u64()
        .div_ceil(1024u64 * 1024u64) as i32;
    let timeout_secs = deploy_config.invocation_timeout_secs as i32;

    info!(
        function_name = %function_name,
        memory_size_mb = memory_size_mb,
        timeout_secs = timeout_secs,
        "attempting to create Lambda function"
    );

    let function_code = FunctionCode::builder()
        .zip_file(Blob::new(LAMBDA_BINARY))
        .build();

    let create_result = client
        .create_function()
        .function_name(function_name)
        .runtime(Runtime::Providedal2023)
        .role(&deploy_config.execution_role_arn)
        .handler("bootstrap")
        .description(description)
        .code(function_code)
        .architectures(Architecture::Arm64)
        .memory_size(memory_size_mb)
        .timeout(timeout_secs)
        .environment(build_environment())
        .set_tags(Some(build_tags()))
        .publish(true)
        .send()
        .await;

    match create_result {
        Ok(output) => {
            let version = output
                .version()
                .ok_or_else(|| anyhow!("created function has no version number"))?
                .to_string();
            info!(
                function_name = %function_name,
                version = %version,
                "lambda function created and published"
            );
            Ok(Some(version))
        }
        Err(SdkError::ServiceError(err)) if err.err().is_resource_conflict_exception() => {
            debug!(
                function_name = %function_name,
                "lambda function already exists"
            );
            Ok(None)
        }
        Err(e) => Err(anyhow!(
            "failed to create Lambda function '{}': {}",
            function_name,
            e
        )),
    }
}

/// Update `$LATEST` to our embedded binary.
///
/// Returns the `code_sha256` of the uploaded code, to be used as a guard
/// when publishing the version (detects if another process overwrote `$LATEST`
/// between our update and publish).
async fn update_function_code(
    client: &LambdaClient,
    function_name: &str,
) -> anyhow::Result<String> {
    info!(
        function_name = %function_name,
        "updating Lambda function code to current binary"
    );

    let response = client
        .update_function_code()
        .function_name(function_name)
        .zip_file(Blob::new(LAMBDA_BINARY))
        .architectures(Architecture::Arm64)
        .send()
        .await
        .context("failed to update Lambda function code")?;

    let code_sha256 = response
        .code_sha256()
        .ok_or_else(|| anyhow!("update_function_code response missing code_sha256"))?
        .to_string();

    wait_for_function_ready(client, function_name).await?;

    Ok(code_sha256)
}

/// Publish a new immutable version from `$LATEST` with our description.
///
/// The `code_sha256` parameter guards against races: if another process
/// overwrote `$LATEST` since our `update_function_code` call, AWS will
/// reject the publish.
///
/// Returns the version number (e.g., "8").
async fn publish_version(
    client: &LambdaClient,
    function_name: &str,
    code_sha256: &str,
    description: &str,
) -> anyhow::Result<String> {
    info!(
        function_name = %function_name,
        description = %description,
        "publishing new Lambda version"
    );

    let publish_response = client
        .publish_version()
        .function_name(function_name)
        .description(description)
        .code_sha256(code_sha256)
        .send()
        .await
        .context(
            "failed to publish Lambda version (code_sha256 mismatch means a concurrent deploy \
             race)",
        )?;

    let version = publish_response
        .version()
        .context("published version has no version number")?
        .to_string();

    info!(
        function_name = %function_name,
        version = %version,
        "lambda version published successfully"
    );

    Ok(version)
}

/// Wait for the Lambda function to be ready.
///
/// "Ready" means `State == Active` and no update is in progress
/// (`LastUpdateStatus` is absent or `Successful`).
///
/// This matters because:
/// - After `create_function`: `State` transitions `Pending → Active`
/// - After `update_function_code`: `State` stays `Active` but `LastUpdateStatus` transitions
///   `InProgress → Successful`
async fn wait_for_function_ready(client: &LambdaClient, function_name: &str) -> anyhow::Result<()> {
    const MAX_WAIT_ATTEMPTS: u32 = 30;
    const WAIT_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_secs(1);

    let mut interval = tokio::time::interval(WAIT_INTERVAL);

    for attempt in 0..MAX_WAIT_ATTEMPTS {
        interval.tick().await;

        let response = client
            .get_function()
            .function_name(function_name)
            .send()
            .await
            .context("failed to get function status")?;

        let Some(config) = response.configuration() else {
            continue;
        };

        // Check for terminal failure states.
        if config.state() == Some(&State::Failed) {
            let reason = config.state_reason().unwrap_or("unknown reason");
            anyhow::bail!(
                "lambda function '{}' is in Failed state: {}",
                function_name,
                reason
            );
        }

        let last_update_status: &LastUpdateStatus = config
            .last_update_status()
            .unwrap_or(&LastUpdateStatus::Successful);

        if last_update_status == &LastUpdateStatus::Failed {
            let reason = config
                .last_update_status_reason()
                .unwrap_or("unknown reason");
            anyhow::bail!(
                "lambda function '{}' last update failed: {}",
                function_name,
                reason
            );
        }

        // Ready = Active state with no update in progress.
        let is_active = config.state() == Some(&State::Active);
        if is_active && last_update_status == &LastUpdateStatus::Successful {
            info!(
                function_name = %function_name,
                attempts = attempt + 1,
                "lambda function is ready"
            );
            return Ok(());
        }

        info!(
            function_name = %function_name,
            state = ?config.state(),
            last_update_status = ?config.last_update_status(),
            attempt = attempt + 1,
            "waiting for Lambda function to be ready"
        );
    }

    anyhow::bail!(
        "lambda function '{}' did not become ready within {} seconds",
        function_name,
        MAX_WAIT_ATTEMPTS as u64 * WAIT_INTERVAL.as_secs()
    )
}

/// Garbage collect old Lambda versions, keeping the current + 5 most recent.
async fn garbage_collect_old_versions(
    client: &LambdaClient,
    function_name: &str,
    current_version: &str,
) -> anyhow::Result<()> {
    let mut quickwit_lambda_versions: Vec<(u64, String)> = Vec::new();
    let mut marker: Option<String> = None;

    // Collect all Quickwit-managed versions
    loop {
        let mut request = client
            .list_versions_by_function()
            .function_name(function_name);

        if let Some(m) = marker {
            request = request.marker(m);
        }

        let response = request
            .send()
            .await
            .context("failed to list Lambda versions for garbage collection")?;

        for version in response.versions() {
            let Some(version_str) = version.version() else {
                continue;
            };
            if version_str == "$LATEST" {
                continue;
            }
            // Only consider Quickwit-managed versions
            let Some(description) = version.description() else {
                continue;
            };
            if description.starts_with(VERSION_DESCRIPTION_PREFIX)
                && let Ok(version_num) = version_str.parse::<u64>()
            {
                quickwit_lambda_versions.push((version_num, version_str.to_string()));
            }
        }

        marker = response.next_marker().map(ToString::to_string);
        if marker.is_none() {
            break;
        }
    }

    // Sort by version number ascending (oldest first)
    quickwit_lambda_versions.sort();

    // We keep the last 5 versions.
    quickwit_lambda_versions.truncate(
        quickwit_lambda_versions
            .len()
            .saturating_sub(GC_KEEP_RECENT_VERSIONS),
    );

    if let Some(pos) = quickwit_lambda_versions
        .iter()
        .position(|(_version, version_str)| version_str == current_version)
    {
        quickwit_lambda_versions.swap_remove(pos);
    }

    // Delete old versions
    for (version, version_str) in quickwit_lambda_versions {
        info!(
            function_name = %function_name,
            version = %version_str,
            "deleting old Lambda version"
        );

        if let Err(e) = client
            .delete_function()
            .function_name(function_name)
            .qualifier(&version_str)
            .send()
            .await
        {
            info!(
                function_name = %function_name,
                version = %version,
                error = %e,
                "failed to delete old Lambda version"
            );
        }
    }

    Ok(())
}

/// Build environment variables for the Lambda function.
fn build_environment() -> Environment {
    let mut env_vars = HashMap::new();
    env_vars.insert("RUST_LOG".to_string(), "info".to_string());
    env_vars.insert("RUST_BACKTRACE".to_string(), "1".to_string());
    Environment::builder().set_variables(Some(env_vars)).build()
}

/// Build tags for the Lambda function.
fn build_tags() -> HashMap<String, String> {
    let mut tags = HashMap::new();
    tags.insert("managed_by".to_string(), "quickwit".to_string());
    tags
}

#[cfg(test)]
mod tests {
    use aws_sdk_lambda::operation::create_function::{CreateFunctionError, CreateFunctionOutput};
    use aws_sdk_lambda::operation::delete_function::DeleteFunctionOutput;
    use aws_sdk_lambda::operation::get_function::GetFunctionOutput;
    use aws_sdk_lambda::operation::list_versions_by_function::{
        ListVersionsByFunctionError, ListVersionsByFunctionOutput,
    };
    use aws_sdk_lambda::operation::publish_version::PublishVersionOutput;
    use aws_sdk_lambda::operation::update_function_code::UpdateFunctionCodeOutput;
    use aws_sdk_lambda::types::FunctionConfiguration;
    use aws_sdk_lambda::types::error::{ResourceConflictException, ResourceNotFoundException};
    use aws_smithy_mocks::{RuleMode, mock, mock_client};
    use bytesize::ByteSize;

    use super::*;

    fn make_version(version: &str, description: &str) -> FunctionConfiguration {
        FunctionConfiguration::builder()
            .version(version)
            .description(description)
            .build()
    }

    fn test_deploy_config() -> LambdaDeployConfig {
        LambdaDeployConfig {
            execution_role_arn: "arn:aws:iam::123456789:role/test-role".to_string(),
            memory_size: ByteSize::gib(5),
            invocation_timeout_secs: 60,
        }
    }

    fn test_description() -> String {
        version_description(None)
    }

    #[test]
    fn test_version_description() {
        let lambda_deploy_config = test_deploy_config();
        let description = version_description(Some(&lambda_deploy_config));
        assert!(description.ends_with("_60s_6c3b2"));
        let description = version_description(None);
        assert!(description.ends_with("_nodeploy"));
    }

    // --- find_matching_version tests ---

    #[tokio::test]
    async fn test_find_matching_version_found() {
        let target = "quickwit:test_version";
        let rule = mock!(aws_sdk_lambda::Client::list_versions_by_function).then_output(|| {
            ListVersionsByFunctionOutput::builder()
                .versions(make_version("$LATEST", ""))
                .versions(make_version("1", "quickwit:old_version"))
                .versions(make_version("7", "quickwit:test_version"))
                .build()
        });
        let client = mock_client!(aws_sdk_lambda, [&rule]);

        let matching_version_opt = find_matching_version(&client, "my-fn", target)
            .await
            .unwrap();
        assert_eq!(matching_version_opt, Some("7".to_string()));
    }

    #[tokio::test]
    async fn test_find_matching_version_not_found() {
        let rule = mock!(aws_sdk_lambda::Client::list_versions_by_function).then_output(|| {
            ListVersionsByFunctionOutput::builder()
                .versions(make_version("$LATEST", ""))
                .versions(make_version("1", "quickwit:other"))
                .build()
        });
        let client = mock_client!(aws_sdk_lambda, [&rule]);

        let result = find_matching_version(&client, "my-fn", "quickwit:no_match")
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_find_matching_version_function_does_not_exist() {
        let rule = mock!(aws_sdk_lambda::Client::list_versions_by_function).then_error(|| {
            ListVersionsByFunctionError::ResourceNotFoundException(
                ResourceNotFoundException::builder().build(),
            )
        });
        let client = mock_client!(aws_sdk_lambda, [&rule]);

        let result = find_matching_version(&client, "no-such-fn", "quickwit:x")
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_find_matching_version_skips_latest_even_if_description_matches() {
        let rule = mock!(aws_sdk_lambda::Client::list_versions_by_function).then_output(|| {
            ListVersionsByFunctionOutput::builder()
                .versions(make_version("$LATEST", "quickwit:match"))
                .build()
        });
        let client = mock_client!(aws_sdk_lambda, [&rule]);

        let result = find_matching_version(&client, "my-fn", "quickwit:match")
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    // --- try_create_function tests ---

    #[tokio::test]
    async fn test_try_create_function_success() {
        let rule = mock!(aws_sdk_lambda::Client::create_function).then_output(|| {
            CreateFunctionOutput::builder()
                .version("1")
                .function_name("my-fn")
                .build()
        });
        let client = mock_client!(aws_sdk_lambda, [&rule]);
        let config = test_deploy_config();

        let result = try_create_function(&client, "my-fn", &config, &test_description())
            .await
            .unwrap();
        assert_eq!(result, Some("1".to_string()));
    }

    #[tokio::test]
    async fn test_try_create_function_already_exists() {
        let rule = mock!(aws_sdk_lambda::Client::create_function).then_error(|| {
            CreateFunctionError::ResourceConflictException(
                ResourceConflictException::builder().build(),
            )
        });
        let client = mock_client!(aws_sdk_lambda, [&rule]);
        let config = test_deploy_config();

        let result = try_create_function(&client, "my-fn", &config, &test_description())
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    // --- deploy (update path) tests ---

    #[tokio::test]
    async fn test_deploy_update_path() {
        // create_function → conflict (function exists)
        let create_rule = mock!(aws_sdk_lambda::Client::create_function).then_error(|| {
            CreateFunctionError::ResourceConflictException(
                ResourceConflictException::builder().build(),
            )
        });
        // update_function_code → success with code_sha256
        let update_rule = mock!(aws_sdk_lambda::Client::update_function_code).then_output(|| {
            UpdateFunctionCodeOutput::builder()
                .code_sha256("abc123hash")
                .build()
        });
        // get_function → active and ready (for wait_for_function_ready)
        let get_rule = mock!(aws_sdk_lambda::Client::get_function).then_output(|| {
            GetFunctionOutput::builder()
                .configuration(
                    FunctionConfiguration::builder()
                        .state(State::Active)
                        .last_update_status(LastUpdateStatus::Successful)
                        .build(),
                )
                .build()
        });
        // publish_version → success
        let publish_rule = mock!(aws_sdk_lambda::Client::publish_version)
            .then_output(|| PublishVersionOutput::builder().version("8").build());

        let client = mock_client!(
            aws_sdk_lambda,
            RuleMode::MatchAny,
            [&create_rule, &update_rule, &get_rule, &publish_rule]
        );
        let config = test_deploy_config();

        tokio::time::pause();
        let version = deploy_lambda_function(&client, "my-fn", &config)
            .await
            .unwrap();
        assert_eq!(version, "8");
    }

    // --- wait_for_function_ready tests ---

    #[tokio::test]
    async fn test_wait_for_function_ready_immediate() {
        let rule = mock!(aws_sdk_lambda::Client::get_function).then_output(|| {
            GetFunctionOutput::builder()
                .configuration(
                    FunctionConfiguration::builder()
                        .state(State::Active)
                        .last_update_status(LastUpdateStatus::Successful)
                        .build(),
                )
                .build()
        });
        let client = mock_client!(aws_sdk_lambda, [&rule]);

        tokio::time::pause();
        wait_for_function_ready(&client, "my-fn").await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_function_ready_after_update_in_progress() {
        let rule = mock!(aws_sdk_lambda::Client::get_function)
            .sequence()
            .output(|| {
                GetFunctionOutput::builder()
                    .configuration(
                        FunctionConfiguration::builder()
                            .state(State::Active)
                            .last_update_status(LastUpdateStatus::InProgress)
                            .build(),
                    )
                    .build()
            })
            .output(|| {
                GetFunctionOutput::builder()
                    .configuration(
                        FunctionConfiguration::builder()
                            .state(State::Active)
                            .last_update_status(LastUpdateStatus::Successful)
                            .build(),
                    )
                    .build()
            })
            .build();
        let client = mock_client!(aws_sdk_lambda, RuleMode::Sequential, [&rule]);

        tokio::time::pause();
        wait_for_function_ready(&client, "my-fn").await.unwrap();
        assert_eq!(rule.num_calls(), 2);
    }

    #[tokio::test]
    async fn test_wait_for_function_ready_fails_on_failed_state() {
        let rule = mock!(aws_sdk_lambda::Client::get_function).then_output(|| {
            GetFunctionOutput::builder()
                .configuration(
                    FunctionConfiguration::builder()
                        .state(State::Failed)
                        .state_reason("Something broke")
                        .build(),
                )
                .build()
        });
        let client = mock_client!(aws_sdk_lambda, [&rule]);

        tokio::time::pause();
        let err = wait_for_function_ready(&client, "my-fn").await.unwrap_err();
        assert!(
            err.to_string().contains("Failed state"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_wait_for_function_ready_fails_on_last_update_failed() {
        let rule = mock!(aws_sdk_lambda::Client::get_function).then_output(|| {
            GetFunctionOutput::builder()
                .configuration(
                    FunctionConfiguration::builder()
                        .state(State::Active)
                        .last_update_status(LastUpdateStatus::Failed)
                        .last_update_status_reason("Update broke")
                        .build(),
                )
                .build()
        });
        let client = mock_client!(aws_sdk_lambda, [&rule]);

        tokio::time::pause();
        let err = wait_for_function_ready(&client, "my-fn").await.unwrap_err();
        assert!(
            err.to_string().contains("last update failed"),
            "unexpected error: {}",
            err
        );
    }

    // --- garbage_collect_old_versions tests ---

    #[tokio::test]
    async fn test_gc_deletes_old_versions_keeps_recent() {
        // 8 quickwit versions (1..=8) + $LATEST + one non-quickwit version
        let list_rule =
            mock!(aws_sdk_lambda::Client::list_versions_by_function).then_output(|| {
                let mut builder = ListVersionsByFunctionOutput::builder()
                    .versions(make_version("$LATEST", ""))
                    .versions(make_version("99", "not-quickwit"));
                for i in 1..=8 {
                    builder = builder
                        .versions(make_version(&i.to_string(), &format!("quickwit:ver_{}", i)));
                }
                builder.build()
            });

        let delete_rule = mock!(aws_sdk_lambda::Client::delete_function)
            .then_output(|| DeleteFunctionOutput::builder().build());

        let client = mock_client!(
            aws_sdk_lambda,
            RuleMode::MatchAny,
            [&list_rule, &delete_rule]
        );

        // Current version is "7", so keep 7 + the 5 most recent (4,5,6,7,8).
        // Should delete versions 1, 2, 3.
        garbage_collect_old_versions(&client, "my-fn", "7")
            .await
            .unwrap();

        assert_eq!(delete_rule.num_calls(), 3);
    }

    #[tokio::test]
    async fn test_gc_nothing_to_delete() {
        // Only 3 quickwit versions — below the GC_KEEP_RECENT_VERSIONS threshold.
        let list_rule =
            mock!(aws_sdk_lambda::Client::list_versions_by_function).then_output(|| {
                ListVersionsByFunctionOutput::builder()
                    .versions(make_version("$LATEST", ""))
                    .versions(make_version("1", "quickwit:v1"))
                    .versions(make_version("2", "quickwit:v2"))
                    .versions(make_version("3", "quickwit:v3"))
                    .build()
            });

        let delete_rule = mock!(aws_sdk_lambda::Client::delete_function)
            .then_output(|| DeleteFunctionOutput::builder().build());

        let client = mock_client!(
            aws_sdk_lambda,
            RuleMode::MatchAny,
            [&list_rule, &delete_rule]
        );

        garbage_collect_old_versions(&client, "my-fn", "3")
            .await
            .unwrap();

        assert_eq!(delete_rule.num_calls(), 0);
    }

    #[tokio::test]
    async fn test_gc_does_not_delete_current_version() {
        // 7 quickwit versions, current is "1" (the oldest).
        // Without the current-version guard, version 1 would be deleted.
        let list_rule =
            mock!(aws_sdk_lambda::Client::list_versions_by_function).then_output(|| {
                let mut builder =
                    ListVersionsByFunctionOutput::builder().versions(make_version("$LATEST", ""));
                for i in 1..=7 {
                    builder = builder
                        .versions(make_version(&i.to_string(), &format!("quickwit:ver_{}", i)));
                }
                builder.build()
            });

        let delete_rule = mock!(aws_sdk_lambda::Client::delete_function)
            .then_output(|| DeleteFunctionOutput::builder().build());

        let client = mock_client!(
            aws_sdk_lambda,
            RuleMode::MatchAny,
            [&list_rule, &delete_rule]
        );

        // Current version is "1". Without guard: would delete 1,2. With guard: only deletes 2.
        garbage_collect_old_versions(&client, "my-fn", "1")
            .await
            .unwrap();

        assert_eq!(delete_rule.num_calls(), 1);
    }
}
