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

use std::time::Duration;

/// Field name reserved for storing the dynamically indexed fields.
pub const FIELD_PRESENCE_FIELD_NAME: &str = "_field_presence";

/// We cannot safely delete splits right away as a:
/// - in-flight queries could actually have selected this split,
/// - scroll queries may also have a point in time on these splits.
///
/// We deal this probably by introducing a grace period. A split is first marked as delete,
/// and hence won't be selected for search. After a few minutes, once it reasonably safe to assume
/// that all queries involving this split have terminated, we effectively delete the split.
/// This duration is controlled by `DELETION_GRACE_PERIOD`.
pub const DELETION_GRACE_PERIOD: Duration = Duration::from_secs(60 * 32); // 32 min

/// In order to amortized search with scroll, we fetch more documents than are
/// being requested.
pub const SCROLL_BATCH_LEN: usize = 1_000;
