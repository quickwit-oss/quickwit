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

use quickwit_common::pubsub::Event;

use super::{CloseShardsRequest, DeleteShardsRequest, SourceType};
use crate::{IndexUid, SourceId};

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

impl Event for AddSourceEvent {}
impl Event for DeleteIndexEvent {}
impl Event for DeleteSourceEvent {}
impl Event for ToggleSourceEvent {}

impl Event for CloseShardsRequest {}
impl Event for DeleteShardsRequest {}
