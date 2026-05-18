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

//! Production-side event streams for trace-conformance testing.
//!
//! The pattern mirrors [`crate::invariants::recorder`]: a global pluggable
//! observer; production calls a `record_*_event` function on every state
//! transition; tests install an observer that captures events into a
//! collector for replay through the Stateright model.

pub mod merge_pipeline;
