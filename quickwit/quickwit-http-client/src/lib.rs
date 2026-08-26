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
//! - no support for connection upgrade (websocket)
//! - focus more on cpu usage and TTLB than TTFB
//! - no support for proxy yet
//! - `write_request` serializes any method and drains/sends body frames, but does not synthesize
//!   request framing (`Content-Length` / `Transfer-Encoding: chunked`); the caller must set those
//!   headers and do the framing
//! - request trailers are dropped, not serialized (no chunked transfer-encoding is synthesized on
//!   requests, so there is no wire slot for them)
//! - the client/pool layer and the single-buffer optimization target GET downloads; other methods
//!   are not exercised yet
//! - the `Host` header is not derived yet
//! - response head parsing has no per-read timeout yet
//! - basic HTTP/1.1 framing only; Transfer-Encoding-over-Content-Length precedence, 101-not-pooled,
//!   and HTTP/1.0+TE rejection will come later

pub mod body;
pub mod client;
pub mod connection;
pub mod dns;
pub mod endpoint;
pub mod error;
pub mod exchange;
pub mod io;
pub mod pool;
pub mod request;
pub mod response;
pub mod tls;

pub use body::{BufferHint, DEFAULT_READ_TIMEOUT, ResponseBody};
pub use client::{DEFAULT_CONNECT_TIMEOUT, DEFAULT_WRITE_TIMEOUT, HttpClient, HttpClientBuilder};
pub use connection::{ConnStream, connect};
pub use dns::{DefaultDnsResolver, DnsResolver, ResolveFuture};
pub use endpoint::Endpoint;
pub use error::HttpError;
pub use pool::{ConnectionPool, DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_IDLE_PER_HOST};
pub use response::{BodyStrategy, ResponseHead, read_head};
pub use tls::default_client_config;
