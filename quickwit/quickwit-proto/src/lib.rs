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
#![allow(deprecated)] // prost::DecodeError::new is deprecated but used in generated decode impls
#![allow(rustdoc::invalid_html_tags)]

use std::cmp::Ordering;

pub mod cluster;
pub mod control_plane;
pub use bytes;
pub use tonic;
pub mod developer;
pub mod error;
mod getters;
pub mod indexing;
pub mod ingest;
pub mod metastore;
pub mod search;
pub mod sort_fields_error;
pub mod types;

pub use error::{GrpcServiceError, ServiceError, ServiceErrorCode};
use search::ReportSplitsRequest;
pub use sort_fields_error::SortFieldsError;

pub mod sortschema {
    include!("codegen/sortschema/sortschema.rs");
}

pub mod jaeger {
    pub mod api_v2 {
        include!("codegen/jaeger/jaeger.api_v2.rs");
    }
    pub mod storage {
        pub mod v1 {
            include!("codegen/jaeger/jaeger.storage.v1.rs");
        }
        pub mod v2 {
            include!("codegen/jaeger/jaeger.storage.v2.rs");
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
            pub mod metrics {
                pub mod v1 {
                    include!("codegen/opentelemetry/opentelemetry.proto.collector.metrics.v1.rs");
                }
            }
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
        pub mod metrics {
            pub mod v1 {
                include!("codegen/opentelemetry/opentelemetry.proto.metrics.v1.rs");
            }
        }
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
