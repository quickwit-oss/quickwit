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

//! HTTP client for Quickwit object storage.
//!
//! The goal of this crate is to build a fast HTTP client to use to query an
//! object stoage. You may find other uses for it, but it might also not fit
//! your use case. Unless Hyper is somehow too slow for you, you should probably
//! not use this.
//!
//! Some limitations at the moment (which may or may not be improved in the future):
//! - no support for HTTP/2
//! - no support for connection upgaged (websocket)
//! - focus more on cpu usage and TTLB than TTFB
//! - no support for proxy yet

pub mod connection;
pub mod dns;
pub mod endpoint;
pub mod error;
pub mod tls;

pub use connection::{ConnStream, connect};
pub use dns::{DefaultDnsResolver, DnsResolver, ResolveFuture};
pub use endpoint::Endpoint;
pub use error::HttpError;
pub use tls::default_client_config;
