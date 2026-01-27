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

//! Remote function invoker abstraction for serverless leaf search execution.

mod aws_lambda;

pub use aws_lambda::AwsLambdaInvoker;

use async_trait::async_trait;
use quickwit_proto::search::{LeafSearchRequest, LeafSearchResponse};

use crate::error::LambdaResult;

/// Trait for invoking remote serverless functions for leaf search.
///
/// This abstraction allows different cloud providers to be supported
/// (AWS Lambda, GCP Cloud Functions, Azure Functions, etc.)
#[async_trait]
pub trait RemoteFunctionInvoker: Send + Sync + 'static {
    /// Invoke the remote function with a LeafSearchRequest.
    ///
    /// The request is serialized to protobuf, sent to the function,
    /// and the response is deserialized from protobuf.
    async fn invoke_leaf_search(
        &self,
        request: LeafSearchRequest,
    ) -> LambdaResult<LeafSearchResponse>;

    /// Check if the invoker is properly configured and available.
    async fn health_check(&self) -> LambdaResult<()>;
}
