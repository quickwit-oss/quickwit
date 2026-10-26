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

use futures::{Stream, StreamExt};
use quickwit_search::SearchError;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;
use warp::reply::Response;
use warp::{Filter, Rejection, Reply};

use super::coordinator::{plan_live_tail, run_live_tail};
use super::{LiveTailContext, LiveTailEvent};
use crate::rest_api_response::into_rest_api_response;
use crate::search_api::extract_index_id_patterns;
use crate::{BodyFormat, with_arg};

#[derive(utoipa::OpenApi)]
#[openapi(paths(live_tail_handler), components(schemas(LiveTailQueryParams)))]
pub struct LiveTailApi;

#[derive(
    Debug, Clone, PartialEq, Eq, Deserialize, Serialize, utoipa::IntoParams, utoipa::ToSchema,
)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct LiveTailQueryParams {
    /// Query in the Quickwit query language. Matches all documents by default.
    #[serde(default = "LiveTailQueryParams::default_query")]
    pub query: String,
    /// Maximum number of documents sent per second by each ingester, capped by the
    /// `live_tail.max_docs_per_sec` setting of its node. Extra documents are reported in
    /// `dropped` events.
    #[serde(default = "LiveTailQueryParams::default_max_docs_per_sec")]
    pub max_docs_per_sec: u32,
}

impl LiveTailQueryParams {
    fn default_query() -> String {
        "*".to_string()
    }

    fn default_max_docs_per_sec() -> u32 {
        200
    }

    fn validate(&self) -> Result<(), SearchError> {
        if self.max_docs_per_sec == 0 {
            return Err(SearchError::InvalidArgument(
                "`max_docs_per_sec` must be positive".to_string(),
            ));
        }
        Ok(())
    }
}

fn live_tail_filter()
-> impl Filter<Extract = (Vec<String>, LiveTailQueryParams), Error = Rejection> + Clone {
    warp::path!(String / "tail")
        .and_then(extract_index_id_patterns)
        .and(warp::get())
        .and(warp::query())
}

#[utoipa::path(
    get,
    tag = "Search",
    path = "/{index_id}/tail",
    responses(
        (status = 200, description = "Stream of Server-Sent Events: `hits`, `dropped` and `error`.", content_type = "text/event-stream")
    ),
    params(
        LiveTailQueryParams,
        ("index_id" = String, Path, description = "The index ID(s) or pattern(s) to tail."),
    )
)]
/// Live Tail
///
/// Streams the documents matching a query as they are ingested, as Server-Sent Events.
pub(crate) fn live_tail_handler(
    context: LiveTailContext,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    live_tail_filter().and(with_arg(context)).then(live_tail)
}

async fn live_tail(
    index_id_patterns: Vec<String>,
    params: LiveTailQueryParams,
    context: LiveTailContext,
) -> Response {
    info!(index_id_patterns=?index_id_patterns, params=?params, "live-tail");
    match open_event_stream(index_id_patterns, params, context).await {
        Ok(event_stream) => {
            let sse_stream = event_stream.map(|event| Ok::<_, Infallible>(event.into_sse_event()));
            warp::sse::reply(warp::sse::keep_alive().stream(sse_stream)).into_response()
        }
        Err(search_error) => {
            into_rest_api_response::<(), SearchError>(Err(search_error), BodyFormat::default())
                .into_response()
        }
    }
}

/// Validates the request and opens the stream of events. Errors occurring after this point are
/// sent as `error` events since the HTTP status is already sent. When the client disconnects, the
/// stream is dropped, which ends the tail.
async fn open_event_stream(
    index_id_patterns: Vec<String>,
    params: LiveTailQueryParams,
    context: LiveTailContext,
) -> Result<impl Stream<Item = LiveTailEvent> + Send + Sync + 'static, SearchError> {
    params.validate()?;
    let plans = plan_live_tail(&context.metastore, &index_id_patterns, &params.query).await?;
    let (event_tx, event_rx) = mpsc::channel(16);
    quickwit_common::spawn_named_task(
        run_live_tail(context, plans, params.max_docs_per_sec, event_tx),
        "live_tail",
    );
    Ok(ReceiverStream::new(event_rx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_live_tail_query_params() {
        let params: LiveTailQueryParams = serde_qs::from_str("").unwrap();
        assert_eq!(params.query, "*");
        assert_eq!(params.max_docs_per_sec, 200);
        params.validate().unwrap();

        let params: LiveTailQueryParams =
            serde_qs::from_str("query=level:ERROR&max_docs_per_sec=10").unwrap();
        assert_eq!(params.query, "level:ERROR");
        assert_eq!(params.max_docs_per_sec, 10);

        let params: LiveTailQueryParams = serde_qs::from_str("max_docs_per_sec=0").unwrap();
        assert!(params.validate().is_err());
        assert!(serde_qs::from_str::<LiveTailQueryParams>("unknown=1").is_err());
    }
}
