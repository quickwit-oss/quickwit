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
//! segment, a projected Arrow schema, and a slice of doc-ids, it reads *only*
//! the projected columns for *only* the selected docs. Reads are exact-type:
//! each projected column is read at the Arrow type matching its physical
//! tantivy `ColumnType`. A requested column with no matching physical column
//! becomes an all-null array of the requested type — a single missing column
//! never fails the whole batch.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, DictionaryArray, Float64Builder,
    Int64Builder, ListBuilder, StringArray, StringBuilder, TimestampMicrosecondBuilder,
    UInt32Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit, UInt32Type};
use arrow::record_batch::RecordBatch;
use tantivy::index::SegmentReader;

use crate::dictionary_builder::DictionaryBuilders;
use crate::error::{PomskyArrowError, Result};

/// Reads the projected fast-field columns of a single segment into an Arrow
/// `RecordBatch`.
///
/// Only `projected_schema`'s columns are read, and only for the docs in
/// `docs`, in the given order. The caller's query engine is responsible for
/// producing the doc-id set (including any alive-bitset filtering, range
/// selection, or limiting). The synthetic columns `_doc_id` and `_segment_ord`
/// are recognized and filled from the doc list and `segment_ord` respectively.
pub fn read_segment_columns(
    segment_reader: &SegmentReader,
    projected_schema: &SchemaRef,
    docs: &[u32],
    segment_ord: u32,
    dictionary_builders: &mut DictionaryBuilders,
) -> Result<RecordBatch> {
    let fast_fields = segment_reader.fast_fields();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projected_schema.fields().len());

    for field in projected_schema.fields() {
        if let Some(array) = build_internal_column(field.name(), docs, segment_ord) {
            columns.push(array);
            continue;
        }
        let array =
            build_fast_field_array(field, fast_fields, docs, segment_ord, dictionary_builders)?;
        columns.push(array);
    }

    let batch = RecordBatch::try_new(projected_schema.clone(), columns)?;
    Ok(batch)
}

fn build_internal_column(name: &str, docs: &[u32], segment_ord: u32) -> Option<ArrayRef> {
    match name {
        "_doc_id" => Some(Arc::new(UInt32Array::from_iter_values(
            docs.iter().copied(),
        ))),
        "_segment_ord" => Some(Arc::new(UInt32Array::from(vec![segment_ord; docs.len()]))),
        _ => None,
    }
}

fn build_fast_field_array(
    field: &Arc<Field>,
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    docs: &[u32],
    segment_ord: u32,
    dictionary_builders: &mut DictionaryBuilders,
) -> Result<ArrayRef> {
    let read_name = crate::fast_field_read_name(field);
    match field.data_type() {
        DataType::UInt64 => Ok(build_u64_array(fast_fields, read_name, docs)),
        DataType::Int64 => Ok(build_i64_array(fast_fields, read_name, docs)),
        DataType::Float64 => Ok(build_f64_array(fast_fields, read_name, docs)),
        DataType::Boolean => Ok(build_bool_array(fast_fields, read_name, docs)),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Ok(build_timestamp_array(fast_fields, read_name, docs))
        }
        DataType::Utf8 => build_utf8_array(fast_fields, field, read_name, docs),
        DataType::Dictionary(key_type, _) if key_type.as_ref() == &DataType::UInt32 => {
            build_dictionary_array(
                fast_fields,
                field,
                read_name,
                docs,
                segment_ord,
                dictionary_builders,
            )
        }
        DataType::Dictionary(_, _) => {
            Err(PomskyArrowError::UnsupportedType(field.data_type().clone()))
        }
        DataType::Binary => build_binary_array(fast_fields, field, read_name, docs),
        dt @ DataType::List(inner) => build_list_array(inner, dt, read_name, fast_fields, docs),
        other => Err(PomskyArrowError::UnsupportedType(other.clone())),
    }
}

fn build_u64_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    name: &str,
    docs: &[u32],
) -> ArrayRef {
    match fast_fields.u64(name) {
        Ok(col) => {
            let mut builder = UInt64Builder::with_capacity(docs.len());
            for &doc_id in docs {
                match col.first(doc_id) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        Err(_) => arrow::array::new_null_array(&DataType::UInt64, docs.len()),
    }
}

fn build_i64_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    name: &str,
    docs: &[u32],
) -> ArrayRef {
    match fast_fields.i64(name) {
        Ok(col) => {
            let mut builder = Int64Builder::with_capacity(docs.len());
            for &doc_id in docs {
                match col.first(doc_id) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        Err(_) => arrow::array::new_null_array(&DataType::Int64, docs.len()),
    }
}

fn build_f64_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    name: &str,
    docs: &[u32],
) -> ArrayRef {
    match fast_fields.f64(name) {
        Ok(col) => {
            let mut builder = Float64Builder::with_capacity(docs.len());
            for &doc_id in docs {
                match col.first(doc_id) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        Err(_) => arrow::array::new_null_array(&DataType::Float64, docs.len()),
    }
}

fn build_bool_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    name: &str,
    docs: &[u32],
) -> ArrayRef {
    match fast_fields.bool(name) {
        Ok(col) => {
            let mut builder = BooleanBuilder::with_capacity(docs.len());
            for &doc_id in docs {
                match col.first(doc_id) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        Err(_) => arrow::array::new_null_array(&DataType::Boolean, docs.len()),
    }
}

fn build_timestamp_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    name: &str,
    docs: &[u32],
) -> ArrayRef {
    match fast_fields.date(name) {
        Ok(col) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(docs.len());
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
            docs.len(),
        ),
    }
}

fn build_utf8_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field: &Arc<Field>,
    name: &str,
    docs: &[u32],
) -> Result<ArrayRef> {
    if let Ok(Some(str_col)) = fast_fields.str(name) {
        let mut builder = StringBuilder::with_capacity(docs.len(), docs.len() * 16);
        let mut buf = String::new();
        for &doc_id in docs {
            let mut ords = str_col.term_ords(doc_id);
            if let Some(ord) = ords.next() {
                buf.clear();
                str_col
                    .ord_to_str(ord, &mut buf)
                    .map_err(|e| PomskyArrowError::Internal(format!("ord_to_str '{name}': {e}")))?;
                builder.append_value(&buf);
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    } else if let Ok(col) = fast_fields.ip_addr(name) {
        let mut builder = StringBuilder::with_capacity(docs.len(), docs.len() * 40);
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
        Ok(arrow::array::new_null_array(field.data_type(), docs.len()))
    }
}

fn build_dictionary_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field: &Arc<Field>,
    read_name: &str,
    docs: &[u32],
    segment_ord: u32,
    dictionary_builders: &mut DictionaryBuilders,
) -> Result<ArrayRef> {
    let str_col = match fast_fields.str(read_name) {
        Ok(Some(col)) => col,
        _ => return Ok(arrow::array::new_null_array(field.data_type(), docs.len())),
    };

    let mut ord_buf: Vec<Option<u64>> = vec![None; docs.len()];
    str_col.ords().first_vals(docs, &mut ord_buf);

    let dictionary_builder = dictionary_builders.get(read_name, field.data_type());
    let indices_array = dictionary_builder.encode(segment_ord, &ord_buf, str_col.dictionary())?;
    let dict_array = DictionaryArray::<UInt32Type>::try_new(
        indices_array,
        dictionary_builder.build_values_array(),
    )?;

    Ok(Arc::new(dict_array))
}

fn build_binary_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field: &Arc<Field>,
    name: &str,
    docs: &[u32],
) -> Result<ArrayRef> {
    let bytes_col = match fast_fields.bytes(name) {
        Ok(Some(col)) => col,
        _ => return Ok(arrow::array::new_null_array(field.data_type(), docs.len())),
    };
    let mut builder = BinaryBuilder::with_capacity(docs.len(), docs.len() * 64);
    let mut buf = Vec::new();
    for &doc_id in docs {
        let mut ord_iter = bytes_col.term_ords(doc_id);
        if let Some(ord) = ord_iter.next() {
            buf.clear();
            bytes_col
                .ord_to_bytes(ord, &mut buf)
                .map_err(|e| PomskyArrowError::Internal(format!("ord_to_bytes '{name}': {e}")))?;
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
) -> Result<ArrayRef> {
    match inner_field.data_type() {
        DataType::UInt64 => match fast_fields.u64(name) {
            Err(_) => Ok(null_list_array(list_data_type, docs.len())),
            Ok(col) => Ok(build_list_from_values(
                ListBuilder::new(UInt64Builder::new()),
                docs,
                |doc_id| col.values_for_doc(doc_id),
                UInt64Builder::append_value,
            )),
        },
        DataType::Int64 => match fast_fields.i64(name) {
            Err(_) => Ok(null_list_array(list_data_type, docs.len())),
            Ok(col) => Ok(build_list_from_values(
                ListBuilder::new(Int64Builder::new()),
                docs,
                |doc_id| col.values_for_doc(doc_id),
                Int64Builder::append_value,
            )),
        },
        DataType::Float64 => match fast_fields.f64(name) {
            Err(_) => Ok(null_list_array(list_data_type, docs.len())),
            Ok(col) => Ok(build_list_from_values(
                ListBuilder::new(Float64Builder::new()),
                docs,
                |doc_id| col.values_for_doc(doc_id),
                Float64Builder::append_value,
            )),
        },
        DataType::Boolean => match fast_fields.bool(name) {
            Err(_) => Ok(null_list_array(list_data_type, docs.len())),
            Ok(col) => Ok(build_list_from_values(
                ListBuilder::new(BooleanBuilder::new()),
                docs,
                |doc_id| col.values_for_doc(doc_id),
                BooleanBuilder::append_value,
            )),
        },
        DataType::Timestamp(TimeUnit::Microsecond, None) => match fast_fields.date(name) {
            Err(_) => Ok(null_list_array(list_data_type, docs.len())),
            Ok(col) => Ok(build_list_from_values(
                ListBuilder::new(TimestampMicrosecondBuilder::new()),
                docs,
                |doc_id| col.values_for_doc(doc_id),
                |builder, val| builder.append_value(val.into_timestamp_micros()),
            )),
        },
        DataType::Utf8 => build_utf8_list_array(fast_fields, list_data_type, name, docs),
        DataType::Binary => build_binary_list_array(fast_fields, list_data_type, name, docs),
        other => Err(PomskyArrowError::Internal(format!(
            "unsupported inner type for list fast field '{name}': {other:?}"
        ))),
    }
}

fn build_utf8_list_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    list_data_type: &DataType,
    name: &str,
    docs: &[u32],
) -> Result<ArrayRef> {
    if let Ok(Some(str_col)) = fast_fields.str(name) {
        let mut builder = ListBuilder::new(StringBuilder::new());
        let mut buf = String::new();
        for &doc_id in docs {
            for ord in str_col.term_ords(doc_id) {
                buf.clear();
                str_col
                    .ord_to_str(ord, &mut buf)
                    .map_err(|e| PomskyArrowError::Internal(format!("ord_to_str '{name}': {e}")))?;
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
        Ok(null_list_array(list_data_type, docs.len()))
    }
}

fn build_binary_list_array(
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    list_data_type: &DataType,
    name: &str,
    docs: &[u32],
) -> Result<ArrayRef> {
    let Ok(Some(bytes_col)) = fast_fields.bytes(name) else {
        return Ok(null_list_array(list_data_type, docs.len()));
    };

    let mut builder = ListBuilder::new(BinaryBuilder::new());
    let mut buf = Vec::new();
    for &doc_id in docs {
        for ord in bytes_col.term_ords(doc_id) {
            buf.clear();
            bytes_col
                .ord_to_bytes(ord, &mut buf)
                .map_err(|e| PomskyArrowError::Internal(format!("ord_to_bytes '{name}': {e}")))?;
            builder.values().append_value(&buf);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}
