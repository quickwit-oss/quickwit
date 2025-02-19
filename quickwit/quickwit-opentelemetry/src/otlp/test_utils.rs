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

use std::time::Duration;

use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value as OtlpAnyValueValue;
use quickwit_proto::opentelemetry::proto::common::v1::{
    AnyValue as OtlpAnyValue, ArrayValue, InstrumentationScope, KeyValue as OtlpKeyValue,
};
use quickwit_proto::opentelemetry::proto::resource::v1::Resource;
use quickwit_proto::opentelemetry::proto::trace::v1::span::{Event as OtlpEvent, Link as OtlpLink};
use quickwit_proto::opentelemetry::proto::trace::v1::{
    ResourceSpans, ScopeSpans, Span as OtlpSpan, Status as OtlpStatus,
};
use time::OffsetDateTime;

fn now_minus_x_secs(now: &OffsetDateTime, secs: u64) -> u64 {
    (*now - Duration::from_secs(secs)).unix_timestamp_nanos() as u64
}

pub fn make_resource_spans_for_test() -> Vec<ResourceSpans> {
    let now: OffsetDateTime = OffsetDateTime::now_utc();

    let attributes = vec![OtlpKeyValue {
        key: "span_key".to_string(),
        value: Some(OtlpAnyValue {
            value: Some(OtlpAnyValueValue::StringValue("span_value".to_string())),
        }),
    }];
    let events = vec![OtlpEvent {
        name: "event_name".to_string(),
        time_unix_nano: 1_000_500_003,
        attributes: vec![OtlpKeyValue {
            key: "event_key".to_string(),
            value: Some(OtlpAnyValue {
                value: Some(OtlpAnyValueValue::StringValue("event_value".to_string())),
            }),
        }],
        dropped_attributes_count: 6,
    }];
    let links = vec![OtlpLink {
        trace_id: vec![4; 16],
        span_id: vec![5; 8],
        trace_state: "link_key1=link_value1,link_key2=link_value2".to_string(),
        attributes: vec![OtlpKeyValue {
            key: "link_key".to_string(),
            value: Some(OtlpAnyValue {
                value: Some(OtlpAnyValueValue::StringValue("link_value".to_string())),
            }),
        }],
        dropped_attributes_count: 7,
    }];
    let spans = vec![
        OtlpSpan {
            trace_id: vec![1; 16],
            span_id: vec![1; 8],
            parent_span_id: Vec::new(),
            trace_state: "key1=value1,key2=value2".to_string(),
            name: "stage_splits".to_string(),
            kind: 1, // Internal
            start_time_unix_nano: now_minus_x_secs(&now, 6),
            end_time_unix_nano: now_minus_x_secs(&now, 5),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: Vec::new(),
            dropped_events_count: 0,
            links: Vec::new(),
            dropped_links_count: 0,
            status: None,
        },
        OtlpSpan {
            trace_id: vec![2; 16],
            span_id: vec![2; 8],
            parent_span_id: Vec::new(),
            trace_state: "key1=value1,key2=value2".to_string(),
            name: "publish_splits".to_string(),
            kind: 2, // Server
            start_time_unix_nano: now_minus_x_secs(&now, 4),
            end_time_unix_nano: now_minus_x_secs(&now, 3),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: Vec::new(),
            dropped_events_count: 0,
            links: Vec::new(),
            dropped_links_count: 0,
            status: None,
        },
        OtlpSpan {
            trace_id: vec![3; 16],
            span_id: vec![3; 8],
            parent_span_id: Vec::new(),
            trace_state: "key1=value1,key2=value2".to_string(),
            name: "list_splits".to_string(),
            kind: 3, // Client
            start_time_unix_nano: now_minus_x_secs(&now, 2),
            end_time_unix_nano: now_minus_x_secs(&now, 1),
            attributes,
            dropped_attributes_count: 0,
            events: Vec::new(),
            dropped_events_count: 0,
            links: Vec::new(),
            dropped_links_count: 0,
            status: Some(OtlpStatus {
                code: 1,
                message: "".to_string(),
            }),
        },
        OtlpSpan {
            trace_id: vec![4; 16],
            span_id: vec![4; 8],
            parent_span_id: Vec::new(),
            trace_state: "key1=value1,key2=value2".to_string(),
            name: "list_splits".to_string(),
            kind: 3, // Client
            start_time_unix_nano: now_minus_x_secs(&now, 2),
            end_time_unix_nano: now_minus_x_secs(&now, 1),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: Vec::new(),
            dropped_events_count: 0,
            links: Vec::new(),
            dropped_links_count: 0,
            status: Some(OtlpStatus {
                code: 2,
                message: "An error occurred.".to_string(),
            }),
        },
        OtlpSpan {
            trace_id: vec![5; 16],
            span_id: vec![5; 8],
            parent_span_id: Vec::new(),
            trace_state: "key1=value1,key2=value2".to_string(),
            name: "delete_splits".to_string(),
            kind: 3, // Client
            start_time_unix_nano: now_minus_x_secs(&now, 2),
            end_time_unix_nano: now_minus_x_secs(&now, 1),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events,
            dropped_events_count: 0,
            links,
            dropped_links_count: 0,
            status: Some(OtlpStatus {
                code: 2,
                message: "Storage error.".to_string(),
            }),
        },
    ];
    let scope_spans = vec![ScopeSpans {
        scope: Some(InstrumentationScope {
            name: "opentelemetry-otlp".to_string(),
            version: "0.11.0".to_string(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
        }),
        spans,
        schema_url: "".to_string(),
    }];
    let resource_attributes = vec![
        OtlpKeyValue {
            key: "service.name".to_string(),
            value: Some(OtlpAnyValue {
                value: Some(OtlpAnyValueValue::StringValue("quickwit".to_string())),
            }),
        },
        OtlpKeyValue {
            key: "tags".to_string(),
            value: Some(OtlpAnyValue {
                value: Some(OtlpAnyValueValue::ArrayValue(ArrayValue {
                    values: vec![OtlpAnyValue {
                        value: Some(OtlpAnyValueValue::StringValue("foo".to_string())),
                    }],
                })),
            }),
        },
    ];
    let resource_spans = ResourceSpans {
        resource: Some(Resource {
            attributes: resource_attributes,
            dropped_attributes_count: 0,
        }),
        scope_spans,
        schema_url: "".to_string(),
    };
    vec![resource_spans]
}
