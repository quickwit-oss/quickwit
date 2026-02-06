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

//! Trait for invoking remote serverless functions for leaf search.

use async_trait::async_trait;
use quickwit_proto::search::{LeafSearchRequest, LeafSearchResponse};

use crate::SearchError;

/// Trait for invoking remote serverless functions (e.g., AWS Lambda) for leaf search.
///
/// This abstraction allows different cloud providers to be supported.
/// Implementations are provided by the `quickwit-lambda` crate.
#[async_trait]
pub trait LambdaLeafSearchInvoker: Send + Sync + 'static {
    /// Invoke the remote function with a LeafSearchRequest.
    ///
    /// Returns one `LeafSearchResponse` per split in the request.
    async fn invoke_leaf_search(
        &self,
        request: LeafSearchRequest,
    ) -> Result<Vec<LeafSearchResponse>, SearchError>;
}
