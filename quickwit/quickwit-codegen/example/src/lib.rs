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

mod error;

#[path = "codegen/hello.rs"]
mod hello;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use quickwit_common::ServiceStream;
use quickwit_common::uri::Uri;
use tower::{Layer, Service};

pub use crate::error::HelloError;
pub use crate::hello::*;

pub type HelloResult<T> = Result<T, HelloError>;

#[derive(Debug, Clone)]
struct Counter<S> {
    counter: Arc<AtomicUsize>,
    inner: S,
}

impl<S, R> Service<R> for Counter<S>
where S: Service<R>
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: R) -> Self::Future {
        self.counter.fetch_add(1, Ordering::Relaxed);
        self.inner.call(req)
    }
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
struct CounterLayer {
    counter: Arc<AtomicUsize>,
}

impl<S> Layer<S> for CounterLayer {
    type Service = Counter<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Counter {
            counter: self.counter.clone(),
            inner,
        }
    }
}

#[allow(dead_code)]
fn spawn_ping_response_stream(
    mut request_stream: ServiceStream<PingRequest>,
) -> ServiceStream<HelloResult<PingResponse>> {
    let (ping_tx, service_stream) = ServiceStream::new_bounded(1);
    let future = async move {
        let mut name = "".to_string();
        let mut interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                request_opt = request_stream.next() => {
                    match request_opt {
                        Some(request) => name = request.name,
                        _ => break,
                    };
                }
                _ = interval.tick() => {
                    if name.is_empty() {
                        continue;
                    }
                    if name == "stop" {
                        break;
                    }
                    if ping_tx.send(Ok(PingResponse {
                        message: format!("Pong, {name}!")
                    })).await.is_err() {
                        break;
                    }
                }
            }
        }
    };
    tokio::spawn(future);
    service_stream
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
struct HelloImpl {
    delay: Duration,
}

#[async_trait]
impl Hello for HelloImpl {
    async fn hello(&self, request: HelloRequest) -> HelloResult<HelloResponse> {
        tokio::time::sleep(self.delay).await;

        if request.name.is_empty() {
            return Err(HelloError::InvalidArgument("name is empty".to_string()));
        }
        Ok(HelloResponse {
            message: format!("Hello, {}!", request.name),
        })
    }

    async fn goodbye(&self, request: GoodbyeRequest) -> HelloResult<GoodbyeResponse> {
        tokio::time::sleep(self.delay).await;

        Ok(GoodbyeResponse {
            message: format!("Goodbye, {}!", request.name),
        })
    }

    async fn ping(
        &self,
        request: ServiceStream<PingRequest>,
    ) -> HelloResult<HelloStream<PingResponse>> {
        Ok(spawn_ping_response_stream(request))
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn endpoints(&self) -> Vec<Uri> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::atomic::Ordering;

    use bytesize::ByteSize;
    use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Universe};
    use quickwit_common::tower::{BalanceChannel, Change, TimeoutLayer};
    use tokio::sync::mpsc::error::TrySendError;
    use tokio_stream::StreamExt;
    use tonic::codec::CompressionEncoding;
    use tonic::transport::{Endpoint, Server};
    use tonic::{Code, Status};

    use super::*;
    use crate::hello::MockHello;
    use crate::hello::hello_grpc_server::HelloGrpcServer;
    use crate::hello_grpc_client::HelloGrpcClient;
    use crate::{CounterLayer, GoodbyeRequest, GoodbyeResponse};

    const MAX_GRPC_MESSAGE_SIZE: ByteSize = ByteSize::mib(1);

    #[tokio::test]
    async fn test_hello_codegen() {
        let hello = HelloImpl::default();

        assert_eq!(
            hello
                .hello(HelloRequest {
                    name: "World".to_string()
                })
                .await
                .unwrap(),
            HelloResponse {
                message: "Hello, World!".to_string()
            }
        );

        let client = HelloClient::new(hello.clone()).clone();

        assert_eq!(
            client
                .hello(HelloRequest {
                    name: "World".to_string()
                })
                .await
                .unwrap(),
            HelloResponse {
                message: "Hello, World!".to_string()
            }
        );

        let (ping_stream_tx, ping_stream) = ServiceStream::new_bounded(1);
        let mut pong_stream = client.ping(ping_stream).await.unwrap();

        ping_stream_tx
            .try_send(PingRequest {
                name: "World".to_string(),
            })
            .unwrap();
        assert_eq!(
            pong_stream.next().await.unwrap().unwrap().message,
            "Pong, World!"
        );
        ping_stream_tx
            .try_send(PingRequest {
                name: "Mundo".to_string(),
            })
            .unwrap();
        assert_eq!(
            pong_stream.next().await.unwrap().unwrap().message,
            "Pong, Mundo!"
        );
        ping_stream_tx
            .try_send(PingRequest {
                name: "stop".to_string(),
            })
            .unwrap();
        assert!(pong_stream.next().await.is_none());

        let error = ping_stream_tx
            .try_send(PingRequest {
                name: "stop".to_string(),
            })
            .unwrap_err();
        assert!(matches!(error, TrySendError::Closed(_)));

        let mut mock_hello = MockHello::new();

        mock_hello.expect_hello().returning(|_| {
            Ok(HelloResponse {
                message: "Hello, Mock!".to_string(),
            })
        });

        assert_eq!(
            mock_hello
                .hello(HelloRequest {
                    name: "".to_string()
                })
                .await
                .unwrap(),
            HelloResponse {
                message: "Hello, Mock!".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_hello_codegen_grpc() {
        let grpc_server =
            HelloClient::new(HelloImpl::default()).as_grpc_service(MAX_GRPC_MESSAGE_SIZE);
        let addr: SocketAddr = "127.0.0.1:6666".parse().unwrap();

        tokio::spawn({
            async move {
                Server::builder()
                    .add_service(grpc_server)
                    .serve(addr)
                    .await
                    .unwrap();
            }
        });
        let channel = BalanceChannel::from_channel(
            "127.0.0.1:6666".parse().unwrap(),
            Endpoint::from_static("http://127.0.0.1:6666").connect_lazy(),
        );
        let grpc_client = HelloClient::from_balance_channel(channel, MAX_GRPC_MESSAGE_SIZE, None);

        assert_eq!(
            grpc_client
                .hello(HelloRequest {
                    name: "gRPC client".to_string()
                })
                .await
                .unwrap(),
            HelloResponse {
                message: "Hello, gRPC client!".to_string()
            }
        );

        assert!(matches!(
            grpc_client
                .hello(HelloRequest {
                    name: "".to_string()
                })
                .await
                .unwrap_err(),
            HelloError::InvalidArgument(_)
        ));

        let (ping_stream_tx, ping_stream) = ServiceStream::new_bounded(1);
        let mut pong_stream = grpc_client.ping(ping_stream).await.unwrap();

        ping_stream_tx
            .try_send(PingRequest {
                name: "gRPC client".to_string(),
            })
            .unwrap();
        assert_eq!(
            pong_stream.next().await.unwrap().unwrap().message,
            "Pong, gRPC client!"
        );

        ping_stream_tx
            .try_send(PingRequest {
                name: "stop".to_string(),
            })
            .unwrap();
        assert!(pong_stream.next().await.is_none());

        let error = ping_stream_tx
            .try_send(PingRequest {
                name: "stop".to_string(),
            })
            .unwrap_err();
        assert!(matches!(error, TrySendError::Closed(_)));

        grpc_client.check_connectivity().await.unwrap();
        assert_eq!(
            grpc_client.endpoints(),
            vec![Uri::from_str("grpc://127.0.0.1:6666/hello.Hello").unwrap()]
        );

        // The connectivity check fails if there is no client behind the channel.
        let (balance_channel, _): (BalanceChannel<SocketAddr>, _) = BalanceChannel::new();
        let grpc_client =
            HelloClient::from_balance_channel(balance_channel, MAX_GRPC_MESSAGE_SIZE, None);
        assert_eq!(
            grpc_client
                .check_connectivity()
                .await
                .unwrap_err()
                .to_string(),
            "no server currently available"
        );
    }

    #[tokio::test]
    async fn test_hello_codegen_grpc_with_compression() {
        #[derive(Debug, Clone)]
        struct CheckCompression<S> {
            inner: S,
        }

        impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for CheckCompression<S>
        where
            S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>
                + Clone
                + Send
                + 'static,
            S::Future: Send + 'static,
            ReqBody: Send + 'static,
        {
            type Response = S::Response;
            type Error = S::Error;
            type Future = BoxFuture<Self::Response, Self::Error>;

            fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                self.inner.poll_ready(cx)
            }

            fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
                let Some(grpc_encoding) = request.headers().get("grpc-encoding") else {
                    panic!("request should be compressed");
                };
                assert!(grpc_encoding.to_str().unwrap().contains("zstd"));

                let Some(grpc_accept_encoding) = request.headers().get("grpc-accept-encoding")
                else {
                    panic!("client should accept compressed responses");
                };
                assert!(grpc_accept_encoding.to_str().unwrap().contains("zstd"));
                let fut = self.inner.call(request);

                Box::pin(async move {
                    let response = fut.await?;

                    let grpc_status_code = Status::from_header_map(response.headers())
                        .map(|status| status.code())
                        .unwrap_or(Code::Ok);

                    if grpc_status_code == Code::Ok {
                        let Some(grpc_encoding) = response.headers().get("grpc-encoding") else {
                            panic!("response should be compressed");
                        };
                        assert!(grpc_encoding.to_str().unwrap().contains("zstd"));
                    }
                    Ok(response)
                })
            }
        }

        #[derive(Debug, Clone)]
        struct CheckCompressionLayer;

        impl<S> Layer<S> for CheckCompressionLayer {
            type Service = CheckCompression<S>;

            fn layer(&self, inner: S) -> Self::Service {
                Self::Service { inner }
            }
        }

        let grpc_server =
            HelloClient::new(HelloImpl::default()).as_grpc_service(MAX_GRPC_MESSAGE_SIZE);
        let addr: SocketAddr = "127.0.0.1:33333".parse().unwrap();

        tokio::spawn({
            async move {
                Server::builder()
                    .layer(CheckCompressionLayer)
                    .add_service(grpc_server)
                    .serve(addr)
                    .await
                    .unwrap();
            }
        });
        let channel = BalanceChannel::from_channel(
            "127.0.0.1:33333".parse().unwrap(),
            Endpoint::from_static("http://127.0.0.1:33333").connect_lazy(),
        );
        let grpc_client = HelloClient::from_balance_channel(
            channel,
            MAX_GRPC_MESSAGE_SIZE,
            Some(CompressionEncoding::Zstd),
        );

        assert_eq!(
            grpc_client
                .hello(HelloRequest {
                    name: "gRPC client".to_string()
                })
                .await
                .unwrap(),
            HelloResponse {
                message: "Hello, gRPC client!".to_string()
            }
        );

        assert!(matches!(
            grpc_client
                .hello(HelloRequest {
                    name: "".to_string()
                })
                .await
                .unwrap_err(),
            HelloError::InvalidArgument(_)
        ));

        let (ping_stream_tx, ping_stream) = ServiceStream::new_bounded(1);
        let mut pong_stream = grpc_client.ping(ping_stream).await.unwrap();

        ping_stream_tx
            .try_send(PingRequest {
                name: "gRPC client".to_string(),
            })
            .unwrap();
        assert_eq!(
            pong_stream.next().await.unwrap().unwrap().message,
            "Pong, gRPC client!"
        );

        ping_stream_tx
            .try_send(PingRequest {
                name: "stop".to_string(),
            })
            .unwrap();
        assert!(pong_stream.next().await.is_none());

        let error = ping_stream_tx
            .try_send(PingRequest {
                name: "stop".to_string(),
            })
            .unwrap_err();
        assert!(matches!(error, TrySendError::Closed(_)));
    }

    #[tokio::test]
    async fn test_hello_codegen_actor() {
        #[derive(Debug)]
        struct HelloActor;

        impl Actor for HelloActor {
            type ObservableState = ();

            fn observable_state(&self) -> Self::ObservableState {}
        }

        #[async_trait]
        impl Handler<HelloRequest> for HelloActor {
            type Reply = HelloResult<HelloResponse>;

            async fn handle(
                &mut self,
                message: HelloRequest,
                _ctx: &ActorContext<Self>,
            ) -> Result<Self::Reply, ActorExitStatus> {
                Ok(Ok(HelloResponse {
                    message: format!("Hello, {}!", message.name),
                }))
            }
        }

        #[async_trait]
        impl Handler<GoodbyeRequest> for HelloActor {
            type Reply = HelloResult<GoodbyeResponse>;

            async fn handle(
                &mut self,
                message: GoodbyeRequest,
                _ctx: &ActorContext<Self>,
            ) -> Result<Self::Reply, ActorExitStatus> {
                Ok(Ok(GoodbyeResponse {
                    message: format!("Goodbye, {}!", message.name),
                }))
            }
        }

        #[async_trait]
        impl Handler<ServiceStream<PingRequest>> for HelloActor {
            type Reply = HelloResult<HelloStream<PingResponse>>;

            async fn handle(
                &mut self,
                message: ServiceStream<PingRequest>,
                _ctx: &ActorContext<Self>,
            ) -> Result<Self::Reply, ActorExitStatus> {
                Ok(Ok(spawn_ping_response_stream(message)))
            }
        }

        let universe = Universe::new();
        let hello_actor = HelloActor;
        let (actor_mailbox, _actor_handle) = universe.spawn_builder().spawn(hello_actor);
        let actor_client = HelloClient::from_mailbox(actor_mailbox.clone());

        assert_eq!(
            actor_client
                .hello(HelloRequest {
                    name: "beautiful actor".to_string()
                })
                .await
                .unwrap(),
            HelloResponse {
                message: "Hello, beautiful actor!".to_string()
            }
        );

        actor_client.check_connectivity().await.unwrap();
        assert_eq!(
            actor_client.endpoints(),
            vec![
                Uri::from_str(&format!(
                    "actor://localhost/{}",
                    actor_mailbox.actor_instance_id()
                ))
                .unwrap()
            ]
        );

        let (ping_stream_tx, ping_stream) = ServiceStream::new_bounded(1);
        let mut pong_stream = actor_client.ping(ping_stream).await.unwrap();

        ping_stream_tx
            .try_send(PingRequest {
                name: "beautiful actor".to_string(),
            })
            .unwrap();
        assert_eq!(
            pong_stream.next().await.unwrap().unwrap().message,
            "Pong, beautiful actor!"
        );

        let hello_tower = HelloClient::tower().build_from_mailbox(actor_mailbox);

        assert_eq!(
            hello_tower
                .hello(HelloRequest {
                    name: "Tower actor".to_string()
                })
                .await
                .unwrap(),
            HelloResponse {
                message: "Hello, Tower actor!".to_string()
            }
        );

        assert_eq!(
            hello_tower
                .goodbye(GoodbyeRequest {
                    name: "Tower actor".to_string()
                })
                .await
                .unwrap(),
            GoodbyeResponse {
                message: "Goodbye, Tower actor!".to_string()
            }
        );

        let (ping_stream_tx, ping_stream) = ServiceStream::new_bounded(1);
        let mut pong_stream = actor_client.ping(ping_stream).await.unwrap();

        ping_stream_tx
            .try_send(PingRequest {
                name: "beautiful Tower actor".to_string(),
            })
            .unwrap();
        assert_eq!(
            pong_stream.next().await.unwrap().unwrap().message,
            "Pong, beautiful Tower actor!"
        );

        universe.assert_quit().await;

        actor_client.check_connectivity().await.unwrap_err();
    }

    #[tokio::test]
    async fn test_hello_codegen_tower_stack_layers() {
        let layer = CounterLayer::default();
        let hello_layer = CounterLayer::default();
        let goodbye_layer = CounterLayer::default();
        let ping_layer = CounterLayer::default();

        let hello_tower = HelloClient::tower()
            .stack_layer(layer.clone())
            .stack_hello_layer(hello_layer.clone())
            .stack_goodbye_layer(goodbye_layer.clone())
            .stack_ping_layer(ping_layer.clone())
            .build(HelloImpl::default());

        hello_tower
            .hello(HelloRequest {
                name: "Tower".to_string(),
            })
            .await
            .unwrap();

        hello_tower
            .goodbye(GoodbyeRequest {
                name: "Tower".to_string(),
            })
            .await
            .unwrap();

        let (ping_stream_tx, ping_stream) = ServiceStream::new_bounded(1);
        let mut pong_stream = hello_tower.ping(ping_stream).await.unwrap();

        ping_stream_tx
            .try_send(PingRequest {
                name: "Tower".to_string(),
            })
            .unwrap();
        assert_eq!(
            pong_stream.next().await.unwrap().unwrap().message,
            "Pong, Tower!"
        );

        assert_eq!(layer.counter.load(Ordering::Relaxed), 3);
        assert_eq!(hello_layer.counter.load(Ordering::Relaxed), 1);
        assert_eq!(goodbye_layer.counter.load(Ordering::Relaxed), 1);
        assert_eq!(ping_layer.counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_hello_codegen_tower_stack_layer_ordering() {
        trait AppendSuffix {
            fn append_suffix(&mut self, suffix: &'static str);
        }

        impl AppendSuffix for HelloRequest {
            fn append_suffix(&mut self, suffix: &'static str) {
                self.name.push_str(suffix);
            }
        }

        impl AppendSuffix for GoodbyeRequest {
            fn append_suffix(&mut self, suffix: &'static str) {
                self.name.push_str(suffix);
            }
        }

        impl AppendSuffix for PingRequest {
            fn append_suffix(&mut self, suffix: &'static str) {
                self.name.push_str(suffix);
            }
        }

        impl AppendSuffix for ServiceStream<PingRequest> {
            fn append_suffix(&mut self, _suffix: &'static str) {}
        }

        #[derive(Debug, Clone)]
        struct AppendSuffixService<S> {
            inner: S,
            suffix: &'static str,
        }

        impl<S, R> Service<R> for AppendSuffixService<S>
        where
            S: Service<R, Error = HelloError>,
            S::Response: fmt::Debug,
            S::Future: Send + 'static,
            R: AppendSuffix,
        {
            type Response = S::Response;
            type Error = HelloError;
            type Future = BoxFuture<S::Response, S::Error>;

            fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                self.inner.poll_ready(cx)
            }

            fn call(&mut self, mut req: R) -> Self::Future {
                req.append_suffix(self.suffix);
                let inner = self.inner.call(req);
                Box::pin(inner)
            }
        }

        #[derive(Debug, Clone)]
        struct AppendSuffixLayer {
            suffix: &'static str,
        }

        impl AppendSuffixLayer {
            fn new(suffix: &'static str) -> Self {
                Self { suffix }
            }
        }

        impl<S> Layer<S> for AppendSuffixLayer {
            type Service = AppendSuffixService<S>;

            fn layer(&self, inner: S) -> Self::Service {
                AppendSuffixService {
                    inner,
                    suffix: self.suffix,
                }
            }
        }
        let hello_tower = HelloClient::tower()
            .stack_layer(AppendSuffixLayer::new("->foo"))
            .stack_hello_layer(AppendSuffixLayer::new("->bar"))
            .stack_layer(AppendSuffixLayer::new("->qux"))
            .stack_hello_layer(AppendSuffixLayer::new("->tox"))
            .stack_goodbye_layer(AppendSuffixLayer::new("->moo"))
            .build(HelloImpl::default());

        let response = hello_tower
            .hello(HelloRequest {
                name: "".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(response.message, "Hello, ->foo->bar->qux->tox!");

        let response = hello_tower
            .goodbye(GoodbyeRequest {
                name: "".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(response.message, "Goodbye, ->foo->qux->moo!");
    }

    #[tokio::test]
    async fn test_from_channel() {
        let balance_channed = BalanceChannel::from_channel(
            "127.0.0.1:7777".parse().unwrap(),
            Endpoint::from_static("http://127.0.0.1:7777").connect_lazy(),
        );
        HelloClient::from_balance_channel(balance_channed, MAX_GRPC_MESSAGE_SIZE, None);
    }

    #[tokio::test]
    async fn test_balance_channel() {
        let hello = HelloImpl::default();
        let grpc_server_adapter = HelloGrpcServerAdapter::new(hello);
        let grpc_server = HelloGrpcServer::new(grpc_server_adapter);
        let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();

        tokio::spawn({
            async move {
                Server::builder()
                    .add_service(grpc_server)
                    .serve(addr)
                    .await
                    .unwrap();
            }
        });
        let (balance_channel, balance_channel_tx) = BalanceChannel::new();
        let channel = Endpoint::from_static("http://127.0.0.1:11111").connect_lazy();
        balance_channel_tx
            .send(Change::Insert("foo", channel))
            .unwrap();

        let mut grpc_client = HelloGrpcClient::new(balance_channel.clone());

        assert_eq!(
            grpc_client
                .hello(HelloRequest {
                    name: "Client".to_string()
                })
                .await
                .unwrap()
                .into_inner(),
            HelloResponse {
                message: "Hello, Client!".to_string()
            }
        );
        assert_eq!(balance_channel.num_connections(), 1);
    }

    #[tokio::test]
    async fn test_hello_codegen_mock() {
        let mut mock_hello = MockHello::new();
        mock_hello.expect_hello().returning(|_| {
            Ok(HelloResponse {
                message: "Hello, mock!".to_string(),
            })
        });
        mock_hello.expect_check_connectivity().returning(|| Ok(()));
        let hello = HelloClient::from_mock(mock_hello);

        assert_eq!(
            hello
                .hello(HelloRequest {
                    name: "World".to_string()
                })
                .await
                .unwrap(),
            HelloResponse {
                message: "Hello, mock!".to_string()
            }
        );
        assert_eq!(
            hello
                .clone()
                .hello(HelloRequest {
                    name: "World".to_string()
                })
                .await
                .unwrap(),
            HelloResponse {
                message: "Hello, mock!".to_string()
            }
        );
        hello.check_connectivity().await.unwrap();
    }

    #[tokio::test]
    async fn test_transport_errors_handling() {
        quickwit_common::setup_logging_for_tests();

        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let channel = Endpoint::from_static("http://127.0.0.1:9999")
            .timeout(Duration::from_millis(100))
            .connect_lazy();
        let max_message_size = ByteSize::mib(1);
        let grpc_client = HelloClient::from_channel(addr, channel, max_message_size, None);

        let error = grpc_client
            .hello(HelloRequest {
                name: "Client".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, HelloError::Unavailable(_)));

        let hello = HelloImpl {
            delay: Duration::from_secs(1),
        };
        let grpc_server_adapter = HelloGrpcServerAdapter::new(hello);
        let grpc_server: HelloGrpcServer<HelloGrpcServerAdapter> =
            HelloGrpcServer::new(grpc_server_adapter);
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();

        tokio::spawn({
            async move {
                Server::builder()
                    .add_service(grpc_server)
                    .serve(addr)
                    .await
                    .unwrap();
            }
        });
        let error = grpc_client
            .hello(HelloRequest {
                name: "Client".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, HelloError::Timeout(_)));
    }

    #[tokio::test]
    async fn test_balanced_channel_timeout_with_server_crash() {
        let addr_str = "127.0.0.1:11112";
        let addr: SocketAddr = addr_str.parse().unwrap();
        // We want to abruptly stop a server without even sending the connection
        // RST packet. Simply dropping the tonic Server is not enough, so we
        // spawn a thread and freeze it with thread::park().
        std::thread::spawn(move || {
            let server_fut = async {
                let hello = HelloImpl {
                    // delay the response so that the server freezes in the middle of the request
                    delay: Duration::from_millis(1000),
                };
                let grpc_server_adapter = HelloGrpcServerAdapter::new(hello);
                let grpc_server = HelloGrpcServer::new(grpc_server_adapter);
                tokio::select! {
                    // wait just enough to let the client perform its request
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                    _ = Server::builder().add_service(grpc_server).serve(addr) => {}
                };
                std::thread::park();
                println!("Thread unparked, unexpected");
            };
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(server_fut);
        });

        // create a client that will try to connect to the server
        let (balance_channel, balance_channel_tx) = BalanceChannel::new();
        let channel = Endpoint::from_str(&format!("http://{addr_str}"))
            .unwrap()
            .connect_lazy();
        balance_channel_tx
            .send(Change::Insert(addr, channel))
            .unwrap();

        let grpc_client = HelloClient::tower()
            // this test hangs forever if we comment out the TimeoutLayer, which
            // shows that a request without explicit timeout might hang forever
            .stack_layer(TimeoutLayer::new(Duration::from_secs(3)))
            .build_from_balance_channel(balance_channel, ByteSize::mib(1), None);

        let response_fut = async move {
            grpc_client
                .hello(HelloRequest {
                    name: "World".to_string(),
                })
                .await
        };
        response_fut
            .await
            .expect_err("should have timed out at the client level");
    }
}
