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

use lambda_runtime::{service_fn, Error, LambdaEvent};
use quickwit_lambda::{handle_leaf_search, LambdaSearcherContext, LeafSearchPayload};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize tracing with JSON output for CloudWatch
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    info!("Starting Quickwit Lambda leaf search handler");

    // Pre-initialize context on cold start
    let context = LambdaSearcherContext::get_or_init().await;

    info!("Lambda context initialized, starting handler loop");

    // Run the Lambda handler
    lambda_runtime::run(service_fn(|event: LambdaEvent<LeafSearchPayload>| async move {
        let (payload, _lambda_ctx) = event.into_parts();
        handle_leaf_search(payload, context)
            .await
            .map_err(|e| lambda_runtime::Error::from(e.to_string()))
    }))
    .await
}
