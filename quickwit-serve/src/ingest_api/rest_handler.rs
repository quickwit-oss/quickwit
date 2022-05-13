// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::convert::Infallible;

use bytes::Bytes;
use quickwit_actors::Mailbox;
use quickwit_ingest_api::{add_doc, IngestApiService};
use quickwit_proto::ingest_api::{DocBatch, IngestRequest, TailRequest};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;
use warp::{reject, Filter, Rejection};

use crate::format::FormatError;
use crate::{require, Format};

#[derive(Debug, Error)]
#[error("Body is not utf-8.")]
struct InvalidUtf8;

impl warp::reject::Reject for InvalidUtf8 {}

#[derive(Debug, Error)]
#[error("The ingest API is not available.")]
struct IngestApiServiceUnavailable;

impl warp::reject::Reject for IngestApiServiceUnavailable {}

const CONTENT_LENGTH_LIMIT: u64 = 20_000_000; // 20M

#[derive(Debug, Error)]
pub enum BulkApiError {
    #[error("Could not parse action `{0}`.")]
    InvalidAction(String),
    #[error("Could not parse the source `{0}`.")]
    InvalidSource(String),
}

impl warp::reject::Reject for BulkApiError {}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all(deserialize = "lowercase"))]
enum BulkAction {
    Index(BulkActionMeta),
    Create(BulkActionMeta),
}

impl BulkAction {
    fn into_index(self) -> String {
        match self {
            BulkAction::Index(meta) => meta.index,
            BulkAction::Create(meta) => meta.index,
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
struct BulkActionMeta {
    #[serde(alias = "_index")]
    index: String,
    #[serde(alias = "_id")]
    id: String,
}

pub fn ingest_handler(
    ingest_api_mailbox_opt: Option<Mailbox<IngestApiService>>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    ingest_filter()
        .and(require(ingest_api_mailbox_opt))
        .and_then(ingest)
}

fn ingest_filter() -> impl Filter<Extract = (String, String), Error = Rejection> + Clone {
    warp::path!(String / "ingest")
        .and(warp::post())
        .and(warp::body::content_length_limit(CONTENT_LENGTH_LIMIT))
        .and(warp::body::bytes().and_then(|body: Bytes| async move {
            if let Ok(body_str) = std::str::from_utf8(&*body) {
                Ok(body_str.to_string())
            } else {
                Err(reject::custom(InvalidUtf8))
            }
        }))
}

fn lines(body: &str) -> impl Iterator<Item = &str> {
    body.lines().filter_map(|line| {
        let line_trimmed = line.trim();
        if line_trimmed.is_empty() {
            return None;
        }
        Some(line_trimmed)
    })
}

async fn ingest(
    index_id: String,
    payload: String,
    ingest_api_mailbox: Mailbox<IngestApiService>,
) -> Result<impl warp::Reply, Infallible> {
    let mut doc_batch = DocBatch {
        index_id,
        ..Default::default()
    };
    for doc_payload in lines(&payload) {
        add_doc(doc_payload.as_bytes(), &mut doc_batch);
    }
    let ingest_req = IngestRequest {
        doc_batches: vec![doc_batch],
    };
    let ingest_resp = ingest_api_mailbox
        .ask_for_res(ingest_req)
        .await
        .map_err(FormatError::wrap);
    Ok(Format::PrettyJson.make_rest_reply(ingest_resp))
}

pub fn tail_handler(
    ingest_api_mailbox_opt: Option<Mailbox<IngestApiService>>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    tail_filter()
        .and(require(ingest_api_mailbox_opt))
        .and_then(tail_endpoint)
}

fn tail_filter() -> impl Filter<Extract = (String,), Error = Rejection> + Clone {
    warp::path!(String / "fetch").and(warp::get())
}

async fn tail_endpoint(
    index_id: String,
    ingest_api_service: Mailbox<IngestApiService>,
) -> Result<impl warp::Reply, Infallible> {
    let tail_res = ingest_api_service
        .ask_for_res(TailRequest { index_id })
        .await
        .map_err(FormatError::wrap);
    Ok(Format::PrettyJson.make_rest_reply(tail_res))
}

fn bulk_filter() -> impl Filter<Extract = (String, String), Error = Rejection> + Clone {
    warp::path!(String / "bulk")
        .and(warp::post())
        .and(warp::body::content_length_limit(CONTENT_LENGTH_LIMIT))
        .and(warp::body::bytes().and_then(|body: Bytes| async move {
            if let Ok(body_str) = std::str::from_utf8(&*body) {
                Ok(body_str.to_string())
            } else {
                Err(reject::custom(InvalidUtf8))
            }
        }))
}

pub fn bulk_handler(
    ingest_api_mailbox_opt: Option<Mailbox<IngestApiService>>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    bulk_filter()
        .and(require(ingest_api_mailbox_opt))
        .and_then(ingest)
}

fn elastic_bulk_filter() -> impl Filter<Extract = (String,), Error = Rejection> + Clone {
    warp::path!("_bulk")
        .and(warp::post())
        .and(warp::body::content_length_limit(CONTENT_LENGTH_LIMIT))
        .and(warp::body::bytes().and_then(|body: Bytes| async move {
            if let Ok(body_str) = std::str::from_utf8(&*body) {
                Ok(body_str.to_string())
            } else {
                Err(reject::custom(InvalidUtf8))
            }
        }))
}

pub fn elastic_bulk_handler(
    ingest_api_mailbox_opt: Option<Mailbox<IngestApiService>>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    elastic_bulk_filter()
        .and(require(ingest_api_mailbox_opt))
        .and_then(elastic_ingest)
}

async fn elastic_ingest(
    payload: String,
    ingest_api_mailbox: Mailbox<IngestApiService>,
) -> Result<impl warp::Reply, Rejection> {
    let mut batches = HashMap::new();
    let mut payload_lines = lines(&payload);

    while let Some(json_str) = payload_lines.next() {
        let action = serde_json::from_str::<BulkAction>(json_str)
            .map_err(|e| BulkApiError::InvalidAction(e.to_string()))?;
        let source = payload_lines
            .next()
            .ok_or_else(|| {
                BulkApiError::InvalidSource("Expected source for the action.".to_string())
            })
            .and_then(|source| {
                serde_json::from_str::<Value>(source)
                    .map_err(|err| BulkApiError::InvalidSource(err.to_string()))
            })?;

        let index_id = action.into_index();
        let doc_batch = batches.entry(index_id.clone()).or_insert(DocBatch {
            index_id,
            ..Default::default()
        });

        add_doc(source.to_string().as_bytes(), doc_batch);
    }

    let ingest_req = IngestRequest {
        doc_batches: batches.into_iter().map(|(_, batch)| batch).collect(),
    };
    let ingest_resp = ingest_api_mailbox
        .ask_for_res(ingest_req)
        .await
        .map_err(FormatError::wrap);
    Ok(Format::PrettyJson.make_rest_reply(ingest_resp))
}

#[cfg(test)]
mod tests {
    use super::{BulkAction, BulkActionMeta};

    #[test]
    fn test_deserialize() {
        let json_str = r#"{ "create" : { "_index" : "test", "_id" : "2" } }"#;
        let bulk_object = serde_json::from_str::<BulkAction>(json_str).unwrap();
        assert_eq!(
            bulk_object,
            BulkAction::Create(BulkActionMeta {
                index: "test".to_string(),
                id: "2".to_string()
            })
        );

        let json_str = r#"{ "delete" : { "_index" : "test", "_id" : "2" } }"#;
        assert!(serde_json::from_str::<BulkAction>(json_str).is_err());
    }

    // TODO: find a way to refactor/mock IngestApiService for testing the endpoint.
}
