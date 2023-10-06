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

use std::cmp::Ordering;
use std::fmt;

use ::opentelemetry::global;
use ::opentelemetry::propagation::{Extractor, Injector};
use tonic::service::Interceptor;
use tonic::Status;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub mod control_plane;
pub use {bytes, tonic};
pub mod error;
pub mod indexing;
pub mod ingest;
pub mod metastore;
pub mod search;
pub mod types;

pub use error::{ServiceError, ServiceErrorCode};
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
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            quickwit_actors::AskError::MessageNotDelivered => ServiceErrorCode::Internal,
            quickwit_actors::AskError::ProcessMessageError => ServiceErrorCode::Internal,
            quickwit_actors::AskError::ErrorReply(err) => err.error_code(),
        }
    }
}

impl search::SortOrder {
    pub fn compare_opt<T: Ord>(&self, this: &Option<T>, other: &Option<T>) -> Ordering {
        match (this, other) {
            (Some(this), Some(other)) => self.compare(this, other),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        }
    }

    pub fn compare<T: Ord>(&self, this: &T, other: &T) -> Ordering {
        if self == &search::SortOrder::Desc {
            this.cmp(other)
        } else {
            other.cmp(this)
        }
    }
}

impl quickwit_common::pubsub::Event for ReportSplitsRequest {}
