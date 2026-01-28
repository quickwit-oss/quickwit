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

mod config;
mod context;
mod error;
mod handler;
mod invoker;

pub use config::{LambdaConfig, LambdaSearcherConfig};
pub use context::LambdaSearcherContext;
pub use error::{LambdaError, LambdaResult};
pub use handler::{LeafSearchPayload, LeafSearchResponsePayload, handle_leaf_search};
pub use invoker::AwsLambdaInvoker;
