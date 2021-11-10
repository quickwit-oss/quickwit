// Copyright (C) 2021 Quickwit, Inc.
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

table! {
    indexes (index_id) {
        index_id -> Varchar,
        index_metadata_json -> Text,
    }
}

table! {
    splits (split_id) {
        split_id -> Varchar,
        split_state -> Varchar,
        time_range_start -> Nullable<Int8>,
        time_range_end -> Nullable<Int8>,
        created_at -> Nullable<Timestamp>,
        updated_at -> Nullable<Timestamp>,
        tags -> Array<Text>,
        split_metadata_json -> Text,
        index_id -> Varchar,
    }
}

joinable!(splits -> indexes (index_id));

allow_tables_to_appear_in_same_query!(indexes, splits,);
