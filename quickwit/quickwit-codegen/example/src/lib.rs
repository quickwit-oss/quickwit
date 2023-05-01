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

use async_trait::async_trait;
pub use error::HelloError;
pub use hello::*;
use tower::{Layer, Service};

use crate::hello::{Hello, HelloRequest, HelloResponse};

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

#[derive(Debug, Clone)]
struct HelloImpl;

#[async_trait]
impl Hello for HelloImpl {
    async fn hello(&mut self, request: HelloRequest) -> crate::HelloResult<HelloResponse> {
        Ok(HelloResponse {
            message: format!("Hello, {}!", request.name),
        })
    }

    async fn goodbye(&mut self, request: GoodbyeRequest) -> crate::HelloResult<GoodbyeResponse> {
        Ok(GoodbyeResponse {
            message: format!("Goodbye, {}!", request.name),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Universe};
    use tonic::transport::{Endpoint, Server};
    use tower::timeout::Timeout;

    use super::*;
    use crate::hello::hello_grpc_server::HelloGrpcServer;
    use crate::hello::MockHello;
    use crate::{CounterLayer, GoodbyeRequest, GoodbyeResponse, HelloError};

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

        let grpc_server_adapter = HelloGrpcServerAdapter::new(hello.clone());
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
            Endpoint::from_static("http://127.0.0.1:6666")
                .connect_timeout(Duration::from_secs(1))
                .connect_lazy(),
            Duration::from_secs(1),
        );
        let mut grpc_client = HelloClient::from_channel(channel);

        assert_eq!(
            grpc_client
                .hello(HelloRequest {
                    name: "Client".to_string()
                })
                .await
                .unwrap(),
            HelloResponse {
                message: "Hello, Client!".to_string()
            }
        );

        #[derive(Debug)]
        struct HelloActor;

        impl Actor for HelloActor {
            type ObservableState = ();

            fn observable_state(&self) -> Self::ObservableState {}
        }

        #[async_trait]
        impl Handler<HelloRequest> for HelloActor {
            type Reply = Result<HelloResponse, HelloError>;

            async fn handle(
                &mut self,
                message: HelloRequest,
                _ctx: &ActorContext<Self>,
            ) -> Result<Self::Reply, ActorExitStatus> {
                Ok(Ok(HelloResponse {
                    message: format!("Hello, {} actor!", message.name),
                }))
            }
        }

        #[async_trait]
        impl Handler<GoodbyeRequest> for HelloActor {
            type Reply = Result<GoodbyeResponse, HelloError>;

            async fn handle(
                &mut self,
                message: GoodbyeRequest,
                _ctx: &ActorContext<Self>,
            ) -> Result<Self::Reply, ActorExitStatus> {
                Ok(Ok(GoodbyeResponse {
                    message: format!("Goodbye, {} actor!", message.name),
                }))
            }
        }

        let universe = Universe::new();
        let hello_actor = HelloActor;
        let (actor_mailbox, _actor_handle) = universe.spawn_builder().spawn(hello_actor);
        let mut actor_client = HelloClient::from_mailbox(actor_mailbox.clone());

        assert_eq!(
            actor_client
                .hello(HelloRequest {
                    name: "beautiful".to_string()
                })
                .await
                .unwrap(),
            HelloResponse {
                message: "Hello, beautiful actor!".to_string()
            }
        );

        let mut hello_tower = HelloClient::tower().build_from_mailbox(actor_mailbox);

        assert_eq!(
            hello_tower
                .hello(HelloRequest {
                    name: "Tower".to_string()
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
                    name: "Tower".to_string()
                })
                .await
                .unwrap(),
            GoodbyeResponse {
                message: "Goodbye, Tower actor!".to_string()
            }
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_hello_codegen_tower_layers() {
        let hello_layer = CounterLayer::default();
        let goodbye_layer = CounterLayer::default();

        let mut hello_tower = HelloClient::tower()
            .hello_layer(hello_layer.clone())
            .goodbye_layer(goodbye_layer.clone())
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

        assert_eq!(hello_layer.counter.load(Ordering::Relaxed), 1);
        assert_eq!(goodbye_layer.counter.load(Ordering::Relaxed), 1);
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

        assert_eq!(layer.counter.load(Ordering::Relaxed), 2);
    }
}
