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

//! AWS Lambda binary entry point for Quickwit leaf search.

use std::sync::Arc;

use lambda_runtime::{Error, LambdaEvent, service_fn};
use quickwit_lambda::{LambdaSearcherContext, LeafSearchPayload, handle_leaf_search};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize tracing with JSON output for CloudWatch
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    // Initialize context on cold start (wrapped in Arc for sharing across invocations)
    let context = Arc::new(LambdaSearcherContext::try_from_env()?);

    info!("lambda context initialized, starting handler loop");

    // Run the Lambda handler
    lambda_runtime::run(service_fn(|event: LambdaEvent<LeafSearchPayload>| {
        let ctx = Arc::clone(&context);
        async move {
            let (payload, _event_ctx) = event.into_parts();
            handle_leaf_search(payload, &ctx)
                .await
                .map_err(|e| lambda_runtime::Error::from(e.to_string()))
        }
    }))
    .await
}
