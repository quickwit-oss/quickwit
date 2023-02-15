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

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_codegen::Codegen;
use tonic::transport::{Endpoint, Server};
use tower::Service;

use crate::test_hello::hello_grpc_client::HelloGrpcClient;
use crate::test_hello::HelloGrpcClientAdapter;

mod test_hello;

#[tokio::test]
async fn test_hello_codegen() {
    let proto = Path::new("tests/test_hello/hello.proto");
    let out_dir = Path::new("tests/test_hello/");

    Codegen::run(
        proto,
        out_dir,
        "crate::test_hello::HelloResult",
        "crate::test_hello::error::HelloError",
    )
    .unwrap();

    use crate::test_hello::hello_grpc_server::HelloGrpcServer;
    use crate::test_hello::{
        Hello, HelloClient, HelloGrpcServerAdapter, HelloRequest, HelloResponse, MockHello,
    };

    #[derive(Debug, Clone)]
    struct HelloImpl;

    #[async_trait]
    impl Hello for HelloImpl {
        async fn hello(
            &mut self,
            request: HelloRequest,
        ) -> crate::test_hello::HelloResult<HelloResponse> {
            Ok(HelloResponse {
                message: format!("Hello, {}!", request.name),
            })
        }
    }

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

    let grpc_server_adapter = HelloGrpcServerAdapter::new(hello);
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
    let channel = Endpoint::from_static("http://127.0.0.1:6666")
        .connect_timeout(Duration::from_secs(1))
        .connect_lazy();
    let mut grpc_client =
        HelloClient::new(HelloGrpcClientAdapter::new(HelloGrpcClient::new(channel)));

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
    assert_eq!(
        grpc_client
            .call(HelloRequest {
                name: "Tower".to_string()
            })
            .await
            .unwrap(),
        HelloResponse {
            message: "Hello, Tower!".to_string()
        }
    );
}
