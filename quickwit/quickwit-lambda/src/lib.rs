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

//! AWS Lambda support for Quickwit leaf search operations.
//!
//! This crate provides:
//! - A Lambda handler that executes leaf search requests
//! - An AWS Lambda implementation of the `RemoteFunctionInvoker` trait
//! - Auto-deployment functionality for Lambda functions

mod config;
mod context;
mod error;
mod handler;
mod invoker;

pub use context::LambdaSearcherContext;

#[cfg(feature = "auto-deploy")]
mod deployer;
#[cfg(feature = "auto-deploy")]
pub use deployer::deploy;
pub use error::{LambdaError, LambdaResult};
pub use handler::{LeafSearchPayload, LeafSearchResponsePayload, handle_leaf_search};
pub use invoker::create_lambda_invoker;

/// Deploy is a no-op when auto-deploy feature is not enabled.
#[cfg(not(feature = "auto-deploy"))]
pub async fn deploy(
    _function_name: &str,
    _deploy_config: &quickwit_config::LambdaDeployConfig,
) -> LambdaResult<String> {
    Err(LambdaError::Configuration(
        "auto-deploy feature is not enabled at compile time".into(),
    ))
}
