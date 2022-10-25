// Copyright (C) 2022 Quickwit, Inc.
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

use std::sync::Arc;

use tantivy::fastfield::{Column, FastFieldReaders};
use tantivy::schema::{Field, FieldEntry, Type};
use tantivy::{DateTime, DocId, TantivyError};

#[derive(Clone)]
pub enum GenericFastFieldReader {
    I64(Arc<dyn Column<i64>>),
    Date(Arc<dyn Column<DateTime>>),
}

impl GenericFastFieldReader {
    pub fn min_value(&self) -> i64 {
        match self {
            GenericFastFieldReader::I64(fast_reader) => fast_reader.min_value(),
            GenericFastFieldReader::Date(fast_reader) => {
                fast_reader.min_value().into_timestamp_secs()
            }
        }
    }

    pub fn max_value(&self) -> i64 {
        match self {
            GenericFastFieldReader::I64(fast_reader) => fast_reader.max_value(),
            GenericFastFieldReader::Date(fast_reader) => {
                fast_reader.max_value().into_timestamp_secs()
            }
        }
    }

    pub fn get(&self, doc_id: DocId) -> i64 {
        match self {
            GenericFastFieldReader::I64(fast_reader) => fast_reader.get_val(doc_id as u64),
            GenericFastFieldReader::Date(fast_reader) => {
                fast_reader.get_val(doc_id as u64).into_timestamp_secs()
            }
        }
    }
}

pub fn timestamp_field_reader(
    timestamp_field: Field,
    timestamp_field_entry: &FieldEntry,
    fast_field_readers: &FastFieldReaders,
) -> tantivy::Result<GenericFastFieldReader> {
    let field_schema_type = timestamp_field_entry.field_type().value_type();
    let timestamp_field_reader = match field_schema_type {
        Type::I64 => GenericFastFieldReader::I64(fast_field_readers.i64(timestamp_field)?),
        Type::Date => GenericFastFieldReader::Date(fast_field_readers.date(timestamp_field)?),
        _ => {
            return Err(TantivyError::SchemaError(format!(
                "Failed to build timestamp filter for field `{:?}`: expected I64 or Date type, \
                 got `{:?}`.",
                timestamp_field_entry.name(),
                field_schema_type
            )))
        }
    };
    Ok(timestamp_field_reader)
}
