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

//! Reads fast-field columns from an opened tantivy `SegmentReader` into an
//! Arrow `RecordBatch`.
//!
//! This is the core primitive of `pomsky-arrow`: given an already-opened
//! segment, a projected Arrow schema, and a [`DocSelection`], it reads *only*
//! the projected columns for *only* the selected docs. Reads are exact-type:
//! each projected column is read at the Arrow type matching its physical
//! tantivy `ColumnType`. A requested column with no matching physical column
//! becomes an all-null array of the requested type — a single missing column
//! never fails the whole batch.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, DictionaryArray, Float64Builder,
    Int32Builder, Int64Builder, ListBuilder, StringArray, StringBuilder,
    TimestampMicrosecondBuilder, UInt32Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Int32Type, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use tantivy::index::SegmentReader;

use crate::error::{ArrowError, Result};

/// Selects which documents of a segment to materialize.
///
/// The primary row filter is *not* this crate's job: the caller's query engine
/// decides which docs match and passes them in as [`DocSelection::Ids`].
/// [`DocSelection::Range`] and [`DocSelection::All`] cover unfiltered scans and
/// both honor the segment's alive-bitset (deleted docs are skipped).
pub enum DocSelection<'a> {
    /// Explicit doc-ids produced by the caller's filter, read in the given order.
    Ids(&'a [u32]),
    /// All alive docs in `[start, end)`.
    Range(std::ops::Range<u32>),
    /// All alive docs, optionally capped at `limit`.
    All { limit: Option<usize> },
}

/// Pre-built Arrow dictionary values for string fast fields.
///
/// Built once per segment, shared (via `Arc`) across all chunks.  The
/// dictionary is streamed in ordinal order so tantivy ordinals map directly
/// to Arrow dictionary indices.
pub struct DictCache {
    entries: HashMap<String, Arc<StringArray>>,
}

impl DictCache {
    /// Build the cache for every `Dictionary`-typed field in `projected_schema`.
    pub fn build(segment_reader: &SegmentReader, projected_schema: &SchemaRef) -> Result<Self> {
        let fast_fields = segment_reader.fast_fields();
        let mut entries = HashMap::new();

        for field in projected_schema.fields() {
            if !matches!(field.data_type(), DataType::Dictionary(_, _)) {
                continue;
            }
            let name = field.name();
            let read_name = crate::fast_field_read_name(field);
            let Ok(Some(str_col)) = fast_fields.str(read_name) else {
                // Missing field: null padding handles it.
                continue;
            };

            let num_terms = str_col.num_terms();
            let mut builder = StringBuilder::with_capacity(num_terms, num_terms * 16);
            let mut streamer = str_col
                .dictionary()
                .stream()
                .map_err(|e| ArrowError::Internal(format!("stream dict '{name}': {e}")))?;
            while streamer.advance() {
                let s = std::str::from_utf8(streamer.key())
                    .map_err(|e| ArrowError::Internal(format!("dict utf8 '{name}': {e}")))?;
                builder.append_value(s);
            }
            entries.insert(name.to_string(), Arc::new(builder.finish()));
        }

        Ok(Self { entries })
    }

    /// Get the pre-built dictionary values for a field.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Arc<StringArray>> {
        self.entries.get(name)
    }
}

/// Reads the projected fast-field columns of a single segment into an Arrow
/// `RecordBatch`.
///
/// Only `projected_schema`'s columns are read, and only for the docs selected
/// by `selection`. The synthetic columns `_doc_id` and `_segment_ord` are
/// recognized and filled from the doc list and `segment_ord` respectively.
pub fn read_segment_columns(
    segment_reader: &SegmentReader,
    projected_schema: &SchemaRef,
    selection: DocSelection<'_>,
    segment_ord: u32,
    dict_cache: Option<&DictCache>,
) -> Result<RecordBatch> {
    let fast_fields = segment_reader.fast_fields();
    let docs: Cow<'_, [u32]> = match selection {
        DocSelection::Ids(ids) => Cow::Borrowed(ids),
        DocSelection::Range(range) => Cow::Owned(collect_docs(segment_reader, range, None)),
        DocSelection::All { limit } => Cow::Owned(collect_docs(
            segment_reader,
            0..segment_reader.max_doc(),
            limit,
        )),
    };

    let num_docs = docs.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projected_schema.fields().len());

    for field in projected_schema.fields() {
        if let Some(array) = build_internal_column(field.name(), &docs, segment_ord, num_docs) {
            columns.push(array);
            continue;
        }
        let array = build_fast_field_array(field, fast_fields, &docs, num_docs, dict_cache)?;
        columns.push(array);
    }

    let batch = RecordBatch::try_new(projected_schema.clone(), columns)?;
    Ok(batch)
}

/// Materializes the alive-filtered doc list for [`DocSelection::Range`] and
/// [`DocSelection::All`].
fn collect_docs(
    segment_reader: &SegmentReader,
    range: std::ops::Range<u32>,
    limit: Option<usize>,
) -> Vec<u32> {
    let alive_bitset = segment_reader.alive_bitset();
    let iter = range.filter(|&doc_id| match alive_bitset {
        Some(bitset) => bitset.is_alive(doc_id),
        None => true,
    });
    match limit {
        Some(lim) => iter.take(lim).collect(),
        None => iter.collect(),
    }
}

fn build_internal_column(
    name: &str,
    docs: &[u32],
    segment_ord: u32,
    num_docs: usize,
) -> Option<ArrayRef> {
    match name {
        "_doc_id" => Some(Arc::new(UInt32Array::from_iter_values(
            docs.iter().copied(),
        ))),
        "_segment_ord" => Some(Arc::new(UInt32Array::from(vec![segment_ord; num_docs]))),
        _ => None,
    }
}

fn build_fast_field_array(
    field: &Arc<Field>,
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    docs: &[u32],
    num_docs: usize,
    dict_cache: Option<&DictCache>,
) -> Result<ArrayRef> {
    let name = field.name();
    let read_name = crate::fast_field_read_name(field);
    match field.data_type() {
        DataType::UInt64 => Ok(build_u64_array(fast_fields, read_name, num_docs, docs)),
        DataType::Int64 => Ok(build_i64_array(fast_fields, read_name, num_docs, docs)),
        DataType::Float64 => Ok(build_f64_array(fast_fields, read_name, num_docs, docs)),
        DataType::Boolean => Ok(build_bool_array(fast_fields, read_name, num_docs, docs)),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(build_timestamp_array(
            fast_fields,
            read_name,
            num_docs,
            docs,
        )),
        DataType::Dictionary(_, _) => build_dictionary_array(
            fast_fields,
            field,
            name,
            read_name,
            num_docs,
            docs,
            dict_cache,
        ),
        DataType::Utf8 => build_utf8_array(fast_fields, field, read_name, num_docs, docs),
        DataType::Binary => build_binary_array(fast_fields, field, read_name, num_docs, docs),
        dt @ DataType::List(inner) => {
            build_list_array(inner, dt, read_name, fast_fields, docs, num_docs)
        }
        other => Err(ArrowError::UnsupportedType(other.clone())),
    }
}

fn build_u64_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    name: &str,
    num_docs: usize,
    docs: &[u32],
) -> ArrayRef {
    match fast_fields.u64(name) {
        Ok(col) => {
            let mut builder = UInt64Builder::with_capacity(num_docs);
            for &doc_id in docs {
                match col.first(doc_id) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        Err(_) => arrow::array::new_null_array(&DataType::UInt64, num_docs),
    }
}

fn build_i64_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    name: &str,
    num_docs: usize,
    docs: &[u32],
) -> ArrayRef {
    match fast_fields.i64(name) {
        Ok(col) => {
            let mut builder = Int64Builder::with_capacity(num_docs);
            for &doc_id in docs {
                match col.first(doc_id) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        Err(_) => arrow::array::new_null_array(&DataType::Int64, num_docs),
    }
}

fn build_f64_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    name: &str,
    num_docs: usize,
    docs: &[u32],
) -> ArrayRef {
    match fast_fields.f64(name) {
        Ok(col) => {
            let mut builder = Float64Builder::with_capacity(num_docs);
            for &doc_id in docs {
                match col.first(doc_id) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        Err(_) => arrow::array::new_null_array(&DataType::Float64, num_docs),
    }
}

fn build_bool_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    name: &str,
    num_docs: usize,
    docs: &[u32],
) -> ArrayRef {
    match fast_fields.bool(name) {
        Ok(col) => {
            let mut builder = BooleanBuilder::with_capacity(num_docs);
            for &doc_id in docs {
                match col.first(doc_id) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        Err(_) => arrow::array::new_null_array(&DataType::Boolean, num_docs),
    }
}

fn build_timestamp_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    name: &str,
    num_docs: usize,
    docs: &[u32],
) -> ArrayRef {
    match fast_fields.date(name) {
        Ok(col) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(num_docs);
            for &doc_id in docs {
                match col.first(doc_id) {
                    Some(dt) => builder.append_value(dt.into_timestamp_micros()),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        Err(_) => arrow::array::new_null_array(
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            num_docs,
        ),
    }
}

fn build_dictionary_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field: &Arc<Field>,
    alias_name: &str,
    read_name: &str,
    num_docs: usize,
    docs: &[u32],
    dict_cache: Option<&DictCache>,
) -> Result<ArrayRef> {
    let str_col = match fast_fields.str(read_name) {
        Ok(Some(col)) => col,
        _ => return Ok(arrow::array::new_null_array(field.data_type(), num_docs)),
    };

    if let Some(values) = dict_cache.and_then(|cache| cache.get(alias_name)) {
        let ords_col = str_col.ords();
        let mut ord_buf: Vec<Option<u64>> = vec![None; num_docs];
        ords_col.first_vals(docs, &mut ord_buf);

        let mut keys_builder = Int32Builder::with_capacity(num_docs);
        for ord in &ord_buf {
            match ord {
                Some(o) => keys_builder.append_value(checked_i32_key(*o, alias_name)?),
                None => keys_builder.append_null(),
            }
        }

        let dict_values: ArrayRef = Arc::clone(values) as ArrayRef;
        let dict_array = DictionaryArray::<Int32Type>::try_new(keys_builder.finish(), dict_values)?;
        return Ok(Arc::new(dict_array));
    }

    build_compact_dict_array(&str_col, docs, num_docs, read_name)
}

fn build_utf8_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field: &Arc<Field>,
    name: &str,
    num_docs: usize,
    docs: &[u32],
) -> Result<ArrayRef> {
    if let Ok(Some(str_col)) = fast_fields.str(name) {
        let mut builder = StringBuilder::with_capacity(num_docs, num_docs * 16);
        let mut buf = String::new();
        for &doc_id in docs {
            let mut ords = str_col.term_ords(doc_id);
            if let Some(ord) = ords.next() {
                buf.clear();
                str_col
                    .ord_to_str(ord, &mut buf)
                    .map_err(|e| ArrowError::Internal(format!("ord_to_str '{name}': {e}")))?;
                builder.append_value(&buf);
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    } else if let Ok(col) = fast_fields.ip_addr(name) {
        let mut builder = StringBuilder::with_capacity(num_docs, num_docs * 40);
        for &doc_id in docs {
            match col.first(doc_id) {
                Some(ip) => {
                    if let Some(v4) = ip.to_ipv4_mapped() {
                        builder.append_value(v4.to_string());
                    } else {
                        builder.append_value(ip.to_string());
                    }
                }
                None => builder.append_null(),
            }
        }
        Ok(Arc::new(builder.finish()))
    } else {
        Ok(arrow::array::new_null_array(field.data_type(), num_docs))
    }
}

fn build_binary_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field: &Arc<Field>,
    name: &str,
    num_docs: usize,
    docs: &[u32],
) -> Result<ArrayRef> {
    let bytes_col = match fast_fields.bytes(name) {
        Ok(Some(col)) => col,
        _ => return Ok(arrow::array::new_null_array(field.data_type(), num_docs)),
    };
    let mut builder = BinaryBuilder::with_capacity(num_docs, num_docs * 64);
    let mut buf = Vec::new();
    for &doc_id in docs {
        let mut ord_iter = bytes_col.term_ords(doc_id);
        if let Some(ord) = ord_iter.next() {
            buf.clear();
            bytes_col
                .ord_to_bytes(ord, &mut buf)
                .map_err(|e| ArrowError::Internal(format!("ord_to_bytes '{name}': {e}")))?;
            builder.append_value(&buf);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn null_list_array(list_data_type: &DataType, num_docs: usize) -> ArrayRef {
    arrow::array::new_null_array(list_data_type, num_docs)
}

fn build_list_from_values<ValueBuilder, Values, Value>(
    mut builder: ListBuilder<ValueBuilder>,
    docs: &[u32],
    mut values_for_doc: impl FnMut(u32) -> Values,
    mut append: impl FnMut(&mut ValueBuilder, Value),
) -> ArrayRef
where
    ValueBuilder: ArrayBuilder,
    Values: IntoIterator<Item = Value>,
{
    for &doc_id in docs {
        for val in values_for_doc(doc_id) {
            append(builder.values(), val);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

/// Build a `ListArray` for a multi-valued fast field, dispatching on the inner type.
///
/// `list_data_type` is the full `DataType::List(...)` used to construct null arrays
/// when the fast field is missing from a segment (schema evolution).
fn build_list_array(
    inner_field: &Arc<Field>,
    list_data_type: &DataType,
    name: &str,
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    docs: &[u32],
    num_docs: usize,
) -> Result<ArrayRef> {
    match inner_field.data_type() {
        DataType::UInt64 => match fast_fields.u64(name) {
            Err(_) => Ok(null_list_array(list_data_type, num_docs)),
            Ok(col) => Ok(build_list_from_values(
                ListBuilder::new(UInt64Builder::new()),
                docs,
                |doc_id| col.values_for_doc(doc_id),
                UInt64Builder::append_value,
            )),
        },
        DataType::Int64 => match fast_fields.i64(name) {
            Err(_) => Ok(null_list_array(list_data_type, num_docs)),
            Ok(col) => Ok(build_list_from_values(
                ListBuilder::new(Int64Builder::new()),
                docs,
                |doc_id| col.values_for_doc(doc_id),
                Int64Builder::append_value,
            )),
        },
        DataType::Float64 => match fast_fields.f64(name) {
            Err(_) => Ok(null_list_array(list_data_type, num_docs)),
            Ok(col) => Ok(build_list_from_values(
                ListBuilder::new(Float64Builder::new()),
                docs,
                |doc_id| col.values_for_doc(doc_id),
                Float64Builder::append_value,
            )),
        },
        DataType::Boolean => match fast_fields.bool(name) {
            Err(_) => Ok(null_list_array(list_data_type, num_docs)),
            Ok(col) => Ok(build_list_from_values(
                ListBuilder::new(BooleanBuilder::new()),
                docs,
                |doc_id| col.values_for_doc(doc_id),
                BooleanBuilder::append_value,
            )),
        },
        DataType::Timestamp(TimeUnit::Microsecond, None) => match fast_fields.date(name) {
            Err(_) => Ok(null_list_array(list_data_type, num_docs)),
            Ok(col) => Ok(build_list_from_values(
                ListBuilder::new(TimestampMicrosecondBuilder::new()),
                docs,
                |doc_id| col.values_for_doc(doc_id),
                |builder, val| builder.append_value(val.into_timestamp_micros()),
            )),
        },
        DataType::Utf8 => build_utf8_list_array(fast_fields, list_data_type, name, docs, num_docs),
        DataType::Binary => {
            build_binary_list_array(fast_fields, list_data_type, name, docs, num_docs)
        }
        other => Err(ArrowError::Internal(format!(
            "unsupported inner type for list fast field '{name}': {other:?}"
        ))),
    }
}

fn build_utf8_list_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    list_data_type: &DataType,
    name: &str,
    docs: &[u32],
    num_docs: usize,
) -> Result<ArrayRef> {
    if let Ok(Some(str_col)) = fast_fields.str(name) {
        let mut builder = ListBuilder::new(StringBuilder::new());
        let mut buf = String::new();
        for &doc_id in docs {
            for ord in str_col.term_ords(doc_id) {
                buf.clear();
                str_col
                    .ord_to_str(ord, &mut buf)
                    .map_err(|e| ArrowError::Internal(format!("ord_to_str '{name}': {e}")))?;
                builder.values().append_value(&buf);
            }
            builder.append(true);
        }
        Ok(Arc::new(builder.finish()))
    } else if let Ok(col) = fast_fields.ip_addr(name) {
        Ok(build_list_from_values(
            ListBuilder::new(StringBuilder::new()),
            docs,
            |doc_id| col.values_for_doc(doc_id),
            |builder, val| {
                if let Some(v4) = val.to_ipv4_mapped() {
                    builder.append_value(v4.to_string());
                } else {
                    builder.append_value(val.to_string());
                }
            },
        ))
    } else {
        Ok(null_list_array(list_data_type, num_docs))
    }
}

fn build_binary_list_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    list_data_type: &DataType,
    name: &str,
    docs: &[u32],
    num_docs: usize,
) -> Result<ArrayRef> {
    let Ok(Some(bytes_col)) = fast_fields.bytes(name) else {
        return Ok(null_list_array(list_data_type, num_docs));
    };

    let mut builder = ListBuilder::new(BinaryBuilder::new());
    let mut buf = Vec::new();
    for &doc_id in docs {
        for ord in bytes_col.term_ords(doc_id) {
            buf.clear();
            bytes_col
                .ord_to_bytes(ord, &mut buf)
                .map_err(|e| ArrowError::Internal(format!("ord_to_bytes '{name}': {e}")))?;
            builder.values().append_value(&buf);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

/// Build a compact `DictionaryArray` from only the ordinals referenced in
/// `docs`.  This is the original per-chunk path, used when no `DictCache` is
/// available.
fn build_compact_dict_array(
    str_col: &tantivy::columnar::StrColumn,
    docs: &[u32],
    num_docs: usize,
    name: &str,
) -> Result<ArrayRef> {
    let mut raw_ords: Vec<Option<u64>> = Vec::with_capacity(num_docs);
    let mut seen_ords: Vec<u64> = Vec::new();
    for &doc_id in docs {
        match str_col.term_ords(doc_id).next() {
            Some(ord) => {
                raw_ords.push(Some(ord));
                if let Err(pos) = seen_ords.binary_search(&ord) {
                    seen_ords.insert(pos, ord);
                }
            }
            None => raw_ords.push(None),
        }
    }

    let mut dict_builder = StringBuilder::with_capacity(seen_ords.len(), seen_ords.len() * 16);
    let mut buf = String::new();
    for &ord in &seen_ords {
        buf.clear();
        str_col
            .ord_to_str(ord, &mut buf)
            .map_err(|e| ArrowError::Internal(format!("dict build '{name}': {e}")))?;
        dict_builder.append_value(&buf);
    }
    let dict_values: ArrayRef = Arc::new(dict_builder.finish());

    let mut keys_builder = Int32Builder::with_capacity(num_docs);
    for raw in &raw_ords {
        match raw {
            Some(ord) => {
                let compact_idx = seen_ords.binary_search(ord).map_err(|_| {
                    ArrowError::Internal(format!(
                        "dictionary ordinal {ord} missing from compact dictionary for '{name}'"
                    ))
                })?;
                keys_builder.append_value(checked_i32_key(compact_idx, name)?);
            }
            None => keys_builder.append_null(),
        }
    }

    let dict_array = DictionaryArray::<Int32Type>::try_new(keys_builder.finish(), dict_values)?;
    Ok(Arc::new(dict_array))
}

fn checked_i32_key(value: impl TryInto<i32>, name: &str) -> Result<i32> {
    value.try_into().map_err(|_| {
        ArrowError::Internal(format!(
            "dictionary key for fast field '{name}' exceeds Int32 capacity"
        ))
    })
}
