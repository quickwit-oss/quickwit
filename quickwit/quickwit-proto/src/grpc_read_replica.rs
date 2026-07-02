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
use std::task::{Context, Poll};

pub const READ_REPLICA_HEADER_NAME: &str = "qw-use-read-replica";
pub const READ_REPLICA_HEADER_VALUE: &str = "true";

pub fn read_replica_header_interceptor() -> quickwit_common::tower::GrpcInterceptor {
    let mut headers = tonic::metadata::MetadataMap::new();
    headers.insert(
        READ_REPLICA_HEADER_NAME,
        tonic::metadata::MetadataValue::from_static(READ_REPLICA_HEADER_VALUE),
    );
    quickwit_common::tower::fixed_headers_interceptor(headers)
}

#[derive(Debug)]
pub struct ReadReplicaGrpcService<S> {
    primary: S,
    read_replica: Option<S>,
}

impl<S> ReadReplicaGrpcService<S> {
    pub fn new(primary: S, read_replica: Option<S>) -> Self {
        Self {
            primary,
            read_replica,
        }
    }
}

impl<S: Clone> Clone for ReadReplicaGrpcService<S> {
    fn clone(&self) -> Self {
        Self::new(self.primary.clone(), self.read_replica.clone())
    }
}

impl<S> tonic::server::NamedService for ReadReplicaGrpcService<S>
where S: tonic::server::NamedService
{
    const NAME: &'static str = S::NAME;
}

impl<S, B> tower::Service<http::Request<B>> for ReadReplicaGrpcService<S>
where
    S: tower::Service<
            http::Request<B>,
            Response = http::Response<tonic::body::Body>,
            Error = Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        std::task::ready!(tower::Service::poll_ready(&mut self.primary, cx))?;
        if let Some(read_replica) = self.read_replica.as_mut() {
            std::task::ready!(tower::Service::poll_ready(read_replica, cx))?;
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let use_read_replica = request
            .headers()
            .get(READ_REPLICA_HEADER_NAME)
            .and_then(|value| value.to_str().ok())
            == Some(READ_REPLICA_HEADER_VALUE);

        if use_read_replica && let Some(read_replica) = self.read_replica.as_mut() {
            tower::Service::call(read_replica, request)
        } else {
            tower::Service::call(&mut self.primary, request)
        }
    }
}
