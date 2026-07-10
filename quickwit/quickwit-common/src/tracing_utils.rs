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

//! W3C trace context propagation over gRPC metadata.
//!
//! Used by the codegen-emitted gRPC client adapters (to inject) and server
//! adapters (to extract). Becomes a no-op when no global text-map propagator
//! is installed — desirable for tests.

use std::str::FromStr;

use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::{Context, global};
use tonic::Status;
use tonic::metadata::{KeyAndValueRef, MetadataKey, MetadataMap, MetadataValue};
use tonic::service::Interceptor;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

struct MetadataInjector<'a>(&'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let Ok(metadata_key) = MetadataKey::from_str(key) else {
            return;
        };
        let Ok(metadata_value) = MetadataValue::try_from(value) else {
            return;
        };
        self.0.insert(metadata_key, metadata_value);
    }
}

struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .iter()
            .filter_map(|key_and_value| match key_and_value {
                KeyAndValueRef::Ascii(key, _) => Some(key.as_str()),
                KeyAndValueRef::Binary(_, _) => None,
            })
            .collect()
    }
}

/// Injects the OpenTelemetry context of the currently-active tracing span
/// into the gRPC request metadata as W3C `traceparent` / `tracestate`.
///
/// No-op when no global text-map propagator is installed.
pub fn inject_current_context(metadata: &mut MetadataMap) {
    let context = Span::current().context();
    let mut injector = MetadataInjector(metadata);
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut injector);
    });
}

/// Extracts an OpenTelemetry context from incoming gRPC request metadata.
/// Returns the empty context when no propagator is installed or no headers
/// are present.
pub fn extract_context(metadata: &MetadataMap) -> Context {
    let extractor = MetadataExtractor(metadata);
    global::get_text_map_propagator(|propagator| propagator.extract(&extractor))
}

/// Tonic interceptor that injects the active span's W3C trace context into
/// the outgoing gRPC metadata. Drop-in replacement for the legacy
/// `quickwit_proto::SpanContextInterceptor`.
#[derive(Clone, Debug)]
pub struct SpanContextInterceptor;

impl Interceptor for SpanContextInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        inject_current_context(request.metadata_mut());
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::propagation::TextMapPropagator;
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    use super::*;

    fn known_span_context() -> SpanContext {
        SpanContext::new(
            TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        )
    }

    #[test]
    fn inject_then_extract_round_trip_preserves_span_context() {
        let span_context = known_span_context();
        let context = Context::new().with_remote_span_context(span_context.clone());

        let propagator = TraceContextPropagator::new();
        let mut metadata = MetadataMap::new();
        propagator.inject_context(&context, &mut MetadataInjector(&mut metadata));

        // `traceparent` is the W3C header — proves the injector wrote
        // through the metadata map.
        assert!(metadata.contains_key("traceparent"));

        let extracted_span_context = propagator
            .extract(&MetadataExtractor(&metadata))
            .span()
            .span_context()
            .clone();
        assert_eq!(extracted_span_context.trace_id(), span_context.trace_id());
        assert_eq!(extracted_span_context.span_id(), span_context.span_id());
        assert_eq!(
            extracted_span_context.trace_flags(),
            span_context.trace_flags()
        );
    }

    #[test]
    fn extract_returns_invalid_span_context_when_no_traceparent() {
        let propagator = TraceContextPropagator::new();
        let metadata = MetadataMap::new();
        let extracted_span_context = propagator
            .extract(&MetadataExtractor(&metadata))
            .span()
            .span_context()
            .clone();
        // No traceparent header → propagator yields an invalid span context.
        assert!(!extracted_span_context.is_valid());
    }
}
