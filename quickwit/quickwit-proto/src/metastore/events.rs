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

// use quickwit_common::pubsub::Event;

use quickwit_common::pubsub::Event;

use super::{
    AddSourceRequest, CreateIndexRequest, DeleteIndexRequest, DeleteSourceRequest, SourceType,
    ToggleSourceRequest,
};
use crate::types::{IndexUid, SourceId};

/// Delete index event.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeleteIndexEvent {
    /// Index ID of the deleted index.
    pub index_uid: IndexUid,
}

/// Add source event.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AddSourceEvent {
    /// The ID of the index to which the source belongs.
    pub index_uid: IndexUid,
    /// The source ID.
    pub source_id: SourceId,
    /// The source type.
    pub source_type: SourceType,
}

/// Toggle source events.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToggleSourceEvent {
    /// Index ID of the toggled source.
    pub index_uid: IndexUid,
    /// Source ID of the toggled source.
    pub source_id: SourceId,
    /// Whether the source is enabled.
    pub enabled: bool,
}

/// Delete source event.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeleteSourceEvent {
    /// Index ID of the deleted source.
    pub index_uid: IndexUid,
    /// Source ID of the deleted source.
    pub source_id: SourceId,
}

impl Event for AddSourceRequest {}
impl Event for CreateIndexRequest {}
impl Event for DeleteIndexRequest {}
impl Event for DeleteSourceRequest {}
impl Event for ToggleSourceRequest {}
