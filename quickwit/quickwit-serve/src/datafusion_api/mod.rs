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

//! DataFusion wiring for `quickwit-serve`.
//!
//! The whole module is gated behind the `datafusion` feature (see `lib.rs`).
//! [`setup`] builds the session at startup and mounts the gRPC services;
//! `lib.rs` and `grpc.rs` each have a single `#[cfg(feature = "datafusion")]`
//! call site into this module.

pub(crate) mod setup;
