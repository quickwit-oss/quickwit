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

use std::convert::Infallible;

use axum::routing::get;
use axum::{Extension, Router};
use quickwit_actors::{AskError, Mailbox, Observe};
use quickwit_indexing::actors::{IndexingService, IndexingServiceCounters};

use crate::rest_api_response::into_rest_api_response;

#[derive(utoipa::OpenApi)]
#[openapi(paths(indexing_endpoint))]
pub struct IndexingApi;

#[utoipa::path(
    get,
    tag = "Indexing",
    path = "/indexing",
    responses(
        (status = 200, description = "Successfully observed indexing pipelines.", body = IndexingStatistics)
    ),
)]
/// Observe Indexing Pipeline
async fn indexing_endpoint(
    indexing_service_mailbox: Mailbox<IndexingService>,
) -> Result<IndexingServiceCounters, AskError<Infallible>> {
    let counters = indexing_service_mailbox.ask(Observe).await?;
    indexing_service_mailbox.ask(Observe).await?;
    Ok(counters)
}

async fn indexing_handler_axum(
    Extension(indexing_service_mailbox_opt): Extension<Option<Mailbox<IndexingService>>>,
) -> impl axum::response::IntoResponse {
    let result = match indexing_service_mailbox_opt {
        Some(mailbox) => indexing_endpoint(mailbox).await,
        None => {
            // Return a service unavailable error
            Err(quickwit_actors::AskError::MessageNotDelivered)
        }
    };

    into_rest_api_response(result, crate::format::BodyFormat::default())
}

pub fn indexing_routes() -> Router {
    Router::new().route("/indexing", get(indexing_handler_axum))
}
