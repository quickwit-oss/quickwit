// Copyright (C) 2023 Quickwit, Inc.
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

#![allow(clippy::derive_partial_eq_without_eq)]
#![deny(clippy::disallowed_methods)]
#![allow(rustdoc::invalid_html_tags)]

use std::convert::Infallible;
use std::fmt;

use ::opentelemetry::global;
use ::opentelemetry::propagation::{Extractor, Injector};
use tonic::codegen::http;
use tonic::service::Interceptor;
use tonic::Status;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub mod control_plane;
pub use {bytes, tonic};
pub mod indexing;
pub mod ingest;
pub mod metastore;
pub mod search;
pub mod types;

pub use types::*;

use crate::search::ReportSplitsRequest;

pub mod jaeger {
    pub mod api_v2 {
        include!("codegen/jaeger/jaeger.api_v2.rs");
    }
    pub mod storage {
        pub mod v1 {
            include!("codegen/jaeger/jaeger.storage.v1.rs");
        }
    }
}

pub mod opentelemetry {
    #[cfg(not(doctest))]
    pub mod proto {

        pub mod collector {
            pub mod logs {
                pub mod v1 {
                    include!("codegen/opentelemetry/opentelemetry.proto.collector.logs.v1.rs");
                }
            }
            // pub mod metrics {
            //     pub mod v1 {
            //         include!("codegen/opentelemetry/opentelemetry.proto.collector.metrics.v1.rs"
            // );     }
            // }
            pub mod trace {
                pub mod v1 {
                    include!("codegen/opentelemetry/opentelemetry.proto.collector.trace.v1.rs");
                }
            }
        }
        pub mod common {
            pub mod v1 {
                include!("codegen/opentelemetry/opentelemetry.proto.common.v1.rs");
            }
        }
        pub mod logs {
            pub mod v1 {
                include!("codegen/opentelemetry/opentelemetry.proto.logs.v1.rs");
            }
        }
        // pub mod metrics {
        //     pub mod experimental {
        //         include!("codegen/opentelemetry/opentelemetry.proto.metrics.experimental.rs");
        //     }
        //     pub mod v1 {
        //         tonic::include_proto!("codegen/opentelemetry/opentelemetry.proto.metrics.v1");
        //     }
        // }
        pub mod resource {
            pub mod v1 {
                include!("codegen/opentelemetry/opentelemetry.proto.resource.v1.rs");
            }
        }
        pub mod trace {
            pub mod v1 {
                include!("codegen/opentelemetry/opentelemetry.proto.trace.v1.rs");
            }
        }
    }
}

#[macro_use]
extern crate serde;

/// This enum serves as a Rosetta stone of
/// gRPC and Http status code.
///
/// It is voluntarily a restricted subset.
#[derive(Clone, Copy)]
pub enum ServiceErrorCode {
    BadRequest,
    Internal,
    MethodNotAllowed,
    NotFound,
    RateLimited,
    Unavailable,
    UnsupportedMediaType,
    NotSupportedYet, /* Used for API that is available in elasticsearch but is not yet
                      * available in Quickwit. */
}

impl ServiceErrorCode {
    pub fn to_grpc_status_code(self) -> tonic::Code {
        match self {
            ServiceErrorCode::BadRequest => tonic::Code::InvalidArgument,
            ServiceErrorCode::Internal => tonic::Code::Internal,
            ServiceErrorCode::MethodNotAllowed => tonic::Code::InvalidArgument,
            ServiceErrorCode::NotFound => tonic::Code::NotFound,
            ServiceErrorCode::RateLimited => tonic::Code::ResourceExhausted,
            ServiceErrorCode::Unavailable => tonic::Code::Unavailable,
            ServiceErrorCode::UnsupportedMediaType => tonic::Code::InvalidArgument,
            ServiceErrorCode::NotSupportedYet => tonic::Code::Unimplemented,
        }
    }
    pub fn to_http_status_code(self) -> http::StatusCode {
        match self {
            ServiceErrorCode::BadRequest => http::StatusCode::BAD_REQUEST,
            ServiceErrorCode::Internal => http::StatusCode::INTERNAL_SERVER_ERROR,
            ServiceErrorCode::MethodNotAllowed => http::StatusCode::METHOD_NOT_ALLOWED,
            ServiceErrorCode::NotFound => http::StatusCode::NOT_FOUND,
            ServiceErrorCode::RateLimited => http::StatusCode::TOO_MANY_REQUESTS,
            ServiceErrorCode::Unavailable => http::StatusCode::SERVICE_UNAVAILABLE,
            ServiceErrorCode::UnsupportedMediaType => http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ServiceErrorCode::NotSupportedYet => http::StatusCode::NOT_IMPLEMENTED,
        }
    }
}

pub trait ServiceError: ToString {
    fn grpc_error(&self) -> tonic::Status {
        let grpc_code = self.status_code().to_grpc_status_code();
        let error_msg = self.to_string();
        tonic::Status::new(grpc_code, error_msg)
    }

    fn status_code(&self) -> ServiceErrorCode;
}

impl ServiceError for Infallible {
    fn status_code(&self) -> ServiceErrorCode {
        unreachable!()
    }
}

pub fn convert_to_grpc_result<T, E: ServiceError>(
    res: Result<T, E>,
) -> Result<tonic::Response<T>, tonic::Status> {
    res.map(tonic::Response::new)
        .map_err(|error| error.grpc_error())
}

impl TryFrom<search::SearchStreamRequest> for search::SearchRequest {
    type Error = anyhow::Error;

    fn try_from(search_stream_req: search::SearchStreamRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            index_id_patterns: vec![search_stream_req.index_id],
            query_ast: search_stream_req.query_ast,
            snippet_fields: search_stream_req.snippet_fields,
            start_timestamp: search_stream_req.start_timestamp,
            end_timestamp: search_stream_req.end_timestamp,
            ..Default::default()
        })
    }
}

impl TryFrom<metastore::DeleteQuery> for search::SearchRequest {
    type Error = anyhow::Error;

    fn try_from(delete_query: metastore::DeleteQuery) -> anyhow::Result<Self> {
        let index_uid: IndexUid = delete_query.index_uid.into();
        Ok(Self {
            index_id_patterns: vec![index_uid.index_id().to_string()],
            query_ast: delete_query.query_ast,
            start_timestamp: delete_query.start_timestamp,
            end_timestamp: delete_query.end_timestamp,
            ..Default::default()
        })
    }
}

/// `MutMetadataMap` used to extract [`tonic::metadata::MetadataMap`] from a request.
pub struct MutMetadataMap<'a>(&'a mut tonic::metadata::MetadataMap);

impl<'a> Injector for MutMetadataMap<'a> {
    /// Sets a key-value pair in the [`MetadataMap`]. No-op if the key or value is invalid.
    fn set(&mut self, key: &str, value: String) {
        if let Ok(metadata_key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(metadata_value) = tonic::metadata::MetadataValue::try_from(&value) {
                self.0.insert(metadata_key, metadata_value);
            }
        }
    }
}

impl<'a> Extractor for MutMetadataMap<'a> {
    /// Gets a value for a key from the MetadataMap.  If the value can't be converted to &str,
    /// returns None.
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}

/// [`tonic::service::interceptor::Interceptor`] which injects the span context into
/// [`tonic::metadata::MetadataMap`].
#[derive(Clone, Debug)]
pub struct SpanContextInterceptor;

impl Interceptor for SpanContextInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(
                &tracing::Span::current().context(),
                &mut MutMetadataMap(request.metadata_mut()),
            )
        });
        Ok(request)
    }
}

/// `MetadataMap` extracts OpenTelemetry
/// tracing keys from request's headers.
struct MetadataMap<'a>(&'a tonic::metadata::MetadataMap);

impl<'a> Extractor for MetadataMap<'a> {
    /// Gets a value for a key from the MetadataMap.  If the value can't be converted to &str,
    /// returns None.
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}

/// Sets parent span context derived from [`tonic::metadata::MetadataMap`].
pub fn set_parent_span_from_request_metadata(request_metadata: &tonic::metadata::MetadataMap) {
    let parent_cx =
        global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request_metadata)));
    Span::current().set_parent(parent_cx);
}

impl<E: fmt::Debug + ServiceError> ServiceError for quickwit_actors::AskError<E> {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            quickwit_actors::AskError::MessageNotDelivered => ServiceErrorCode::Internal,
            quickwit_actors::AskError::ProcessMessageError => ServiceErrorCode::Internal,
            quickwit_actors::AskError::ErrorReply(err) => err.status_code(),
        }
    }
}

impl quickwit_common::pubsub::Event for ReportSplitsRequest {}
