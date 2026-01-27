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

//! AWS Lambda client for Quickwit leaf search operations.
//!
//! This crate provides:
//! - An AWS Lambda implementation of the `RemoteFunctionInvoker` trait
//! - Auto-deployment functionality for Lambda functions
//!
//! # Usage
//!
//! Use `get_or_deploy_invoker` to get an invoker that will automatically deploy
//! the Lambda function if needed:
//!
//! ```ignore
//! let invoker = get_or_deploy_invoker(&function_name, &deploy_config).await?;
//! ```

mod deploy;
mod invoker;
mod metrics;

pub use deploy::try_get_or_deploy_invoker;
pub use metrics::LAMBDA_METRICS;
// Re-export payload types from server crate for convenience
pub use quickwit_lambda_server::{LeafSearchRequestPayload, LeafSearchResponsePayload};
