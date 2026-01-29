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

mod deployer;
mod error;
mod invoker;

pub use deployer::deploy;
pub use error::{LambdaClientError, LambdaClientResult};
pub use invoker::create_lambda_invoker;
// Re-export payload types from server crate for convenience
pub use quickwit_lambda_server::{LeafSearchPayload, LeafSearchResponsePayload};
