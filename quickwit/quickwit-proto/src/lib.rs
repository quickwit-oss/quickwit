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

#![allow(clippy::derive_partial_eq_without_eq)]
#![allow(clippy::disallowed_methods)]
#![allow(clippy::doc_lazy_continuation)]
#![allow(rustdoc::invalid_html_tags)]

use std::cmp::Ordering;

use ::opentelemetry::global;
use ::opentelemetry::propagation::{Extractor, Injector};
use tonic::Status;
use tonic::service::Interceptor;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub mod cluster;
pub mod control_plane;
pub use {bytes, tonic};
pub mod developer;
pub mod error;
mod getters;
pub mod indexing;
pub mod ingest;
pub mod metastore;
pub mod search;
pub mod types;

pub use error::{GrpcServiceError, ServiceError, ServiceErrorCode};
use search::ReportSplitsRequest;

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
            // One can dream.
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

impl TryFrom<metastore::DeleteQuery> for search::SearchRequest {
    type Error = anyhow::Error;

    fn try_from(delete_query: metastore::DeleteQuery) -> anyhow::Result<Self> {
        Ok(Self {
            index_id_patterns: vec![delete_query.index_uid().index_id.to_string()],
            query_ast: delete_query.query_ast,
            start_timestamp: delete_query.start_timestamp,
            end_timestamp: delete_query.end_timestamp,
            ..Default::default()
        })
    }
}

/// `MutMetadataMap` used to extract [`tonic::metadata::MetadataMap`] from a request.
pub struct MutMetadataMap<'a>(&'a mut tonic::metadata::MetadataMap);

impl Injector for MutMetadataMap<'_> {
    /// Sets a key-value pair in the [`MetadataMap`]. No-op if the key or value is invalid.
    fn set(&mut self, key: &str, value: String) {
        if let Ok(metadata_key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes())
            && let Ok(metadata_value) = tonic::metadata::MetadataValue::try_from(&value)
        {
            self.0.insert(metadata_key, metadata_value);
        }
    }
}

impl Extractor for MutMetadataMap<'_> {
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

impl Extractor for MetadataMap<'_> {
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

impl search::SortOrder {
    #[inline(always)]
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

/// Shard update_timestamp to use when reading file metastores <v0.9
pub fn compatibility_shard_update_timestamp() -> i64 {
    // We prefer a fix value here because it makes backward compatibility tests
    // simpler. Very few users use the shard API in versions <0.9 anyway.
    1704067200 // 2024-00-00T00:00:00Z
}
