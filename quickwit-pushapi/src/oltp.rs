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

use async_trait::async_trait;
use quickwit_proto::opentelemetry::proto::collector::logs::v1::logs_service_server::LogsService;
use quickwit_proto::opentelemetry::proto::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use quickwit_proto::opentelemetry::proto::collector::trace::v1::trace_service_server::TraceService;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value;
use quickwit_proto::opentelemetry::proto::common::v1::KeyValue;
use quickwit_proto::opentelemetry::proto::trace::v1::ResourceSpans;
use quickwit_proto::tonic;

#[derive(Default, Clone)]
pub struct OltpService {}

#[async_trait]
impl LogsService for OltpService {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        let _export_logs_service_req = request.into_inner();
        let resp = ExportLogsServiceResponse::default();
        Ok(tonic::Response::new(resp))
    }
}

fn debug_value(val: &Value) -> String {
    match val {
        Value::StringValue(v) => v.to_string(),
        Value::BoolValue(b) => b.to_string(),
        Value::IntValue(v) => v.to_string(),
        Value::DoubleValue(v) => v.to_string(),
        Value::ArrayValue(v) => format!("{:?}", v),
        Value::KvlistValue(kvs) => format!("{:?}", kvs),
        Value::BytesValue(bytes) => format!("{:?}", bytes),
    }
}

fn bytes_to_id(v: &[u8]) -> u64 {
    if v.len() == 0 {
        return 0u64;
    }
    let b: [u8; 8] = v[0..8].try_into().unwrap();
    u64::from_be_bytes(b)
}

fn debug_attr(attr: &KeyValue) {
    let value_opt = attr.value.as_ref().and_then(|v| v.value.as_ref());
    if let Some(value) = value_opt {
        println!("{}={:?}", attr.key, debug_value(value));
    } else {
        println!("{}", attr.key);
    }
}

fn debug_resource_spans(span: &ResourceSpans) {
    println!("\n--");
    if let Some(resource) = span.resource.as_ref() {
        for attr in &resource.attributes {
            debug_attr(attr);
        }
    }
    for scope_span in &span.scope_spans {
        for span in &scope_span.spans {
            println!("name={}", span.name);
            println!("trace_id={:?}", bytes_to_id(&span.trace_id));
            println!("span_id={:?}", bytes_to_id(&span.span_id));
            println!("trace_state={}", span.trace_state);
            println!("parent_span_id={:?}", bytes_to_id(&span.parent_span_id));
            println!("kind={:?}", span.kind);
            println!("start={}", span.start_time_unix_nano);
            println!("end={}", span.end_time_unix_nano);
            for attr in &span.attributes {
                debug_attr(attr);
            }
            for event in &span.events {
                println!(" ts={}", event.time_unix_nano);
                println!(" name={}", event.name);
                for attr in &event.attributes {
                    println!(" - key={}", attr.key);
                    println!("   val={:?}", attr.value);
                }
            }
        }
    }
}

#[async_trait]
impl TraceService for OltpService {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        let resp = ExportTraceServiceResponse::default();
        let req = request.into_inner();
        for span in &req.resource_spans {
            debug_resource_spans(&span);
        }
        // dbg!(&req);
        Ok(tonic::Response::new(resp))
    }
}
