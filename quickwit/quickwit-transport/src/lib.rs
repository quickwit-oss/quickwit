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

//! Node-to-node transport for Quickwit: gRPC channel construction and hot-reloadable TLS.
//!
//! This crate sits above `quickwit-config` (so it can read `TlsConfig`/`GrpcConfig` directly) and
//! is the single home for the TLS material and the gRPC [`ChannelFactory`]. Lower-level channel
//! primitives that are baked into generated gRPC clients (e.g. `BalanceChannel`) intentionally stay
//! in `quickwit-common`, which `quickwit-proto` depends on.

mod accept;
mod channel;
mod tls;

pub use accept::accept_tls_incoming;
pub use channel::ChannelFactory;
pub use tls::{make_tls_client_config, make_tls_server_config, reload_tls_cert};
