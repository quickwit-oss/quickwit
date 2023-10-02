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

mod error;

#[path = "codegen/hello.rs"]
mod hello;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use quickwit_common::ServiceStream;
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

#[derive(Debug, Clone)]
struct HelloImpl;

#[async_trait]
impl Hello for HelloImpl {
    async fn hello(&mut self, request: HelloRequest) -> HelloResult<HelloResponse> {
        Ok(HelloResponse {
            message: format!("Hello, {}!", request.name),
        })
    }

    async fn goodbye(&mut self, request: GoodbyeRequest) -> HelloResult<GoodbyeResponse> {
        Ok(GoodbyeResponse {
            message: format!("Goodbye, {}!", request.name),
        })
    }

    async fn ping(
        &mut self,
        request: ServiceStream<PingRequest>,
    ) -> HelloResult<HelloStream<PingResponse>> {
        Ok(spawn_ping_response_stream(request))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Universe};
    use quickwit_common::tower::{BalanceChannel, Change};
    use tokio::sync::mpsc::error::TrySendError;
    use tokio_stream::StreamExt;
    use tonic::transport::{Endpoint, Server};
    use tower::timeout::Timeout;

    use super::*;
    use crate::hello::hello_grpc_server::HelloGrpcServer;
    use crate::hello::MockHello;
    use crate::hello_grpc_client::HelloGrpcClient;
    use crate::{CounterLayer, GoodbyeRequest, GoodbyeResponse};

    #[tokio::test]
    async fn test_hello_codegen() {
        let mut hello = HelloImpl;

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

        let mut client = HelloClient::new(hello.clone()).clone();

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
        let grpc_server_adapter = HelloGrpcServerAdapter::new(HelloImpl);
        let grpc_server = HelloGrpcServer::new(grpc_server_adapter);
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
        let channel = Timeout::new(
            Endpoint::from_static("http://127.0.0.1:6666").connect_lazy(),
            Duration::from_secs(1),
        );
        let mut grpc_client = HelloClient::from_channel(channel);

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
        let mut actor_client = HelloClient::from_mailbox(actor_mailbox.clone());

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

        let mut hello_tower = HelloClient::tower().build_from_mailbox(actor_mailbox);

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
    }

    #[tokio::test]
    async fn test_hello_codegen_tower_layers() {
        let hello_layer = CounterLayer::default();
        let goodbye_layer = CounterLayer::default();
        let ping_layer = CounterLayer::default();

        let mut hello_tower = HelloClient::tower()
            .hello_layer(hello_layer.clone())
            .goodbye_layer(goodbye_layer.clone())
            .ping_layer(ping_layer.clone())
            .build(HelloImpl);

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

        assert_eq!(hello_layer.counter.load(Ordering::Relaxed), 1);
        assert_eq!(goodbye_layer.counter.load(Ordering::Relaxed), 1);
        assert_eq!(ping_layer.counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_hello_codegen_tower_shared_layer() {
        let layer = CounterLayer::default();

        let mut hello_tower = HelloClient::tower()
            .shared_layer(layer.clone())
            .build(HelloImpl);

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
    }

    #[tokio::test]
    async fn test_from_channel() {
        let timeout_channel = Timeout::new(
            Endpoint::from_static("http://127.0.0.1:7777").connect_lazy(),
            Duration::from_secs(1),
        );
        HelloClient::from_channel(timeout_channel);

        let channel = Endpoint::from_static("http://127.0.0.1:7777").connect_lazy();
        let balance_channel = BalanceChannel::from_channel("test-node", channel);
        HelloClient::from_channel(balance_channel);
    }

    #[tokio::test]
    async fn test_balance_channel() {
        let hello = HelloImpl;
        let grpc_server_adapter = HelloGrpcServerAdapter::new(hello);
        let grpc_server = HelloGrpcServer::new(grpc_server_adapter);
        let addr: SocketAddr = "127.0.0.1:8888".parse().unwrap();

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
        let channel = Endpoint::from_static("http://127.0.0.1:8888").connect_lazy();
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
        let mut hello_mock = HelloClient::mock();
        hello_mock.expect_hello().returning(|_| {
            Ok(HelloResponse {
                message: "Hello, mock!".to_string(),
            })
        });
        let mut hello: HelloClient = hello_mock.into();
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
    }
}
