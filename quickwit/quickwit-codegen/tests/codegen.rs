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

use std::path::Path;

use async_trait::async_trait;
use quickwit_codegen::Codegen;

mod hello;

#[tokio::test]
async fn test_hello_codegen() {
    let proto = Path::new("tests/hello/hello.proto");
    let out_dir = Path::new("tests/hello/");

    Codegen::run(proto, out_dir, "crate::hello::HelloResult").unwrap();

    use crate::hello::{Hello, HelloRequest, HelloResponse};

    #[derive(Debug, Clone)]
    struct HelloImpl;

    #[async_trait]
    impl Hello for HelloImpl {
        async fn hello(
            &mut self,
            request: HelloRequest,
        ) -> crate::hello::HelloResult<HelloResponse> {
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
}
