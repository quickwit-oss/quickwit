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

//! Column-major Parquet writer primitive.
//!
//! Wraps [`SerializedFileWriter`] directly to expose a per-column write
//! API that flushes one column chunk at a time. Peak memory per row
//! group is therefore bounded by the size of the largest single column
//! chunk plus small bookkeeping (bloom filters + page indexes), not by
//! the total row group.
//!
//! This module is plumbing only. Production callers (ingest, merge) keep
//! using [`super::ParquetWriter`] in PR-2; PR-3 cuts ingest over to a
//! single-RG writer built on this primitive, and PR-6 cuts the merge
//! engine over. Until then the items here are only exercised by this
//! file's tests, so `dead_code` is allowed at module scope.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::io::Write;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_writer::{ArrowColumnWriter, ArrowRowGroupWriterFactory, compute_leaves};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};

use super::writer::ParquetWriteError;

/// Column-major Parquet writer.
///
/// See module docs for invariants and intended use.
///
/// # Caller contract
/// 1. Call [`Self::start_row_group`] to obtain a [`RowGroupBuilder`].
/// 2. Call [`RowGroupBuilder::write_next_column`] once per top-level arrow field, in arrow schema
///    order, with the column data for the current row group.
/// 3. Call [`RowGroupBuilder::finish`] to close the row group.
/// 4. Repeat for additional row groups.
/// 5. Call [`Self::close`] to write the footer.
///
/// Calling out of order (too many columns, finishing before all columns
/// are written, mismatched row counts) returns a structured error rather
/// than panicking.
///
/// # Limitations (PR-2)
/// Top-level arrow fields must each map to exactly one parquet leaf
/// column — i.e., the schema is "flat" (primitive, byte-array, or
/// dictionary types). Nested types (Struct, List, Map) are rejected at
/// [`Self::start_row_group`]. The metrics schema is flat in this sense.
pub(crate) struct StreamingParquetWriter<W: Write + Send> {
    file_writer: SerializedFileWriter<W>,
    factory: ArrowRowGroupWriterFactory,
    arrow_schema: SchemaRef,
    next_rg_idx: usize,
}

/// Open row group; produced by [`StreamingParquetWriter::start_row_group`].
///
/// Borrows the parent writer for the lifetime of the row group.
/// The compiler enforces that only one row group is open at a time.
pub(crate) struct RowGroupBuilder<'a, W: Write + Send> {
    pending_writers: VecDeque<ArrowColumnWriter>,
    row_group_writer: SerializedRowGroupWriter<'a, W>,
    arrow_schema: SchemaRef,
    next_field_idx: usize,
    expected_num_rows: Option<usize>,
}

impl<W: Write + Send> StreamingParquetWriter<W> {
    /// Construct a new streaming writer.
    ///
    /// `arrow_schema` describes the columns the file will contain.
    /// `props` is consumed and used as-is — callers wanting the
    /// `ARROW:schema` IPC-encoded entry that [`parquet::arrow::ArrowWriter`]
    /// adds by default must call
    /// [`parquet::arrow::add_encoded_arrow_schema_to_metadata`] on `props`
    /// before calling this constructor. (We do not do it implicitly so
    /// PR-2 stays a thin wrapper; PR-3 ingest and PR-6 merge will add it
    /// in their own setup helpers.)
    pub(crate) fn try_new(
        sink: W,
        arrow_schema: SchemaRef,
        props: WriterProperties,
    ) -> Result<Self, ParquetWriteError> {
        let coerce_types = props.coerce_types();
        let parquet_schema = ArrowSchemaConverter::new()
            .with_coerce_types(coerce_types)
            .convert(&arrow_schema)?;

        let props_ptr = Arc::new(props);
        let file_writer = SerializedFileWriter::new(
            sink,
            parquet_schema.root_schema_ptr(),
            Arc::clone(&props_ptr),
        )?;
        let factory = ArrowRowGroupWriterFactory::new(&file_writer, arrow_schema.clone());

        Ok(Self {
            file_writer,
            factory,
            arrow_schema,
            next_rg_idx: 0,
        })
    }

    /// Open a new row group. The returned [`RowGroupBuilder`] borrows
    /// `self` for the lifetime of the row group.
    ///
    /// Returns an error if any top-level arrow field expands to more
    /// than one parquet leaf — i.e. multi-leaf nested types such as
    /// `Struct` with several primitive children. Single-leaf nested
    /// types (e.g. `List<UInt64>` for the DDSketch `counts` column)
    /// are accepted; see
    /// [`RowGroupBuilder::write_next_column_arrays`] for how those
    /// columns are routed.
    pub(crate) fn start_row_group(&mut self) -> Result<RowGroupBuilder<'_, W>, ParquetWriteError> {
        let column_writers = self.factory.create_column_writers(self.next_rg_idx)?;
        if column_writers.len() != self.arrow_schema.fields().len() {
            return Err(ParquetWriteError::SchemaValidation(format!(
                "streaming writer requires exactly one parquet leaf per arrow field; arrow schema \
                 has {} fields but produced {} parquet leaves (multi-leaf nested types — e.g. \
                 Struct with multiple children — are not supported)",
                self.arrow_schema.fields().len(),
                column_writers.len(),
            )));
        }

        let row_group_writer = self.file_writer.next_row_group()?;
        self.next_rg_idx += 1;

        Ok(RowGroupBuilder {
            pending_writers: column_writers.into(),
            row_group_writer,
            arrow_schema: self.arrow_schema.clone(),
            next_field_idx: 0,
            expected_num_rows: None,
        })
    }

    /// Close the file and return its metadata.
    pub(crate) fn close(self) -> Result<ParquetMetaData, ParquetWriteError> {
        Ok(self.file_writer.close()?)
    }

    /// The number of row groups started so far. Useful for tests and
    /// for callers that want to track output structure.
    #[cfg(test)]
    pub(crate) fn num_row_groups_started(&self) -> usize {
        self.next_rg_idx
    }
}

impl<'a, W: Write + Send> RowGroupBuilder<'a, W> {
    /// Write the next top-level arrow column for the open row group.
    ///
    /// Columns must be supplied in arrow schema order. The column's
    /// length defines the row group's row count on the first call;
    /// subsequent calls must match.
    pub(crate) fn write_next_column(&mut self, array: &ArrayRef) -> Result<(), ParquetWriteError> {
        let fields = self.arrow_schema.fields();
        if self.next_field_idx >= fields.len() {
            return Err(ParquetWriteError::SchemaValidation(format!(
                "wrote {} columns but arrow schema only has {}",
                self.next_field_idx + 1,
                fields.len(),
            )));
        }
        let field = &fields[self.next_field_idx];

        let row_count = array.len();
        match self.expected_num_rows {
            None => self.expected_num_rows = Some(row_count),
            Some(expected) if expected == row_count => {}
            Some(expected) => {
                return Err(ParquetWriteError::SchemaValidation(format!(
                    "row count mismatch in row group: column {} ('{}') has {} rows; prior columns \
                     had {}",
                    self.next_field_idx,
                    field.name(),
                    row_count,
                    expected,
                )));
            }
        }

        let leaves = compute_leaves(field.as_ref(), array)?;
        if leaves.len() != 1 {
            return Err(ParquetWriteError::SchemaValidation(format!(
                "field '{}' produces {} parquet leaves; PR-2 streaming writer requires exactly 1 \
                 (no nested types)",
                field.name(),
                leaves.len(),
            )));
        }

        // Pop the writer for this column, write the single leaf, close to
        // produce a finalized chunk, and immediately append the chunk to
        // the row group writer. After this, the chunk's encoded buffer
        // has been copied into the file writer's underlying sink and is
        // dropped — so peak in-flight chunk memory stays at one column.
        let mut writer = self.pending_writers.pop_front().expect(
            "pending_writers length matched arrow_schema fields at start_row_group; field index \
             checked against fields above",
        );
        for leaf in &leaves {
            writer.write(leaf)?;
        }
        let chunk = writer.close()?;
        chunk.append_to_row_group(&mut self.row_group_writer)?;
        self.next_field_idx += 1;
        Ok(())
    }

    /// Write the next top-level arrow column for the open row group as
    /// a sequence of arrays — typically one per input page in PR-6's
    /// merge engine.
    ///
    /// Each item in the iterator is one batch worth of values for the
    /// current column. Pages are flushed to the underlying file sink
    /// **as they are encoded** via [`SerializedColumnWriter`], so peak
    /// in-memory state stays at one in-progress page (bounded by
    /// `data_page_size_limit` / `data_page_row_count_limit`) plus a
    /// small amount of column-writer bookkeeping. Memory does NOT grow
    /// with the column chunk size.
    ///
    /// This contrasts with [`Self::write_next_column`], which uses
    /// `ArrowColumnWriter` and buffers the entire column chunk in
    /// memory before flushing on close. Use this method when the
    /// caller can stream the column in pieces (e.g., PR-6's merge
    /// driver, which produces one arrow array per input page) and
    /// where the column chunk would be large enough to matter.
    ///
    /// Output page boundaries are determined by the writer's
    /// `WriterProperties`, not by the input partition; logical values
    /// round-trip. The sum of input array lengths defines this
    /// column's row count, which must match the row count established
    /// by the first call into the row group. An empty iterator is
    /// allowed (the column contributes zero rows).
    ///
    /// # Supported types
    /// Flat physical types: `Boolean`, `Int8/16/32` and
    /// `UInt8/16/32` (mapped to parquet `Int32` physical with the
    /// appropriate logical annotation per [`ArrowSchemaConverter`]),
    /// `Int64`/`UInt64` (mapped to parquet `Int64`), `Float32`,
    /// `Float64`, `Utf8`/`LargeUtf8`/`Binary`/`LargeBinary` (mapped
    /// to parquet `ByteArray`), and `Dictionary` over any of the
    /// above (materialized via `take`).
    ///
    /// `List<T>` / `LargeList<T>` where the outer field is
    /// **non-nullable** and the inner field is **non-nullable** and
    /// one of the flat primitives above. This covers the DDSketch
    /// `keys` (`List<Int16>`) and `counts` (`List<UInt64>`) columns.
    /// The list path computes Dremel definition + repetition levels
    /// from each input array and pushes them through the same
    /// [`SerializedColumnWriter::write_batch`] call the flat path
    /// uses, so memory stays bounded by one in-flight page —
    /// pages flush directly to the file sink as
    /// `data_page_size_limit` / `data_page_row_count_limit`
    /// thresholds are reached, identical to the flat-primitive path.
    ///
    /// Other types — nullable list inner, nullable list outer,
    /// `Struct`, `Map`, `FixedSizeList`, multi-leaf nested — return
    /// [`ParquetWriteError::SchemaValidation`]. Multi-leaf nested
    /// (`Struct` with multiple primitive children, etc.) is also
    /// rejected up front by
    /// [`StreamingParquetWriter::start_row_group`] so the row group
    /// is never opened.
    ///
    /// [`SerializedColumnWriter::write_batch`]: parquet::file::writer::SerializedColumnWriter
    pub(crate) fn write_next_column_arrays<I>(
        &mut self,
        arrays: I,
    ) -> Result<(), ParquetWriteError>
    where
        I: IntoIterator<Item = ArrayRef>,
    {
        let fields = self.arrow_schema.fields();
        if self.next_field_idx >= fields.len() {
            return Err(ParquetWriteError::SchemaValidation(format!(
                "wrote {} columns but arrow schema only has {}",
                self.next_field_idx + 1,
                fields.len(),
            )));
        }
        let field = &fields[self.next_field_idx];

        // Drop the pre-allocated `ArrowColumnWriter` for this column —
        // we use `next_column()` to get a `SerializedColumnWriter`
        // instead, whose `SerializedPageWriter` flushes pages directly
        // to the file sink as they are encoded. The pre-allocated
        // `ArrowColumnWriter`'s internal `ArrowPageWriter` would
        // accumulate all pages of the column into a `SharedColumnChunk`
        // buffer until `close + append_to_row_group`, which scales
        // memory with column-chunk size, not page size.
        let _discarded = self.pending_writers.pop_front().expect(
            "pending_writers length matched arrow_schema fields at start_row_group; field index \
             checked against fields above",
        );

        let mut col_writer = self.row_group_writer.next_column()?.ok_or_else(|| {
            ParquetWriteError::SchemaValidation(
                "row group writer exhausted columns; field count mismatch with parquet schema"
                    .to_string(),
            )
        })?;

        let mut total_rows = 0usize;
        for array in arrays {
            write_array_via_serialized_column_writer(&mut col_writer, field.as_ref(), &array)?;
            total_rows += array.len();
        }

        // Check row-count consistency BEFORE close.
        // `SerializedRowGroupWriter::on_close` also detects a mismatch
        // (against the first closed column's row count), but reports
        // it as a generic `ParquetError`. Surfacing the caller-
        // friendly `SchemaValidation` first lets clients match on the
        // contract violation explicitly.
        //
        // On the error path, `col_writer` drops without `close`. That
        // leaves `row_group_writer` in a "column open, never
        // finalized" state — subsequent operations on this
        // `RowGroupBuilder` will fail. The caller is already on an
        // error path; we don't try to recover.
        match self.expected_num_rows {
            None => self.expected_num_rows = Some(total_rows),
            Some(expected) if expected == total_rows => {}
            Some(expected) => {
                return Err(ParquetWriteError::SchemaValidation(format!(
                    "row count mismatch in row group: column {} ('{}') has {} rows (sum across \
                     input arrays); prior columns had {}",
                    self.next_field_idx,
                    field.name(),
                    total_rows,
                    expected,
                )));
            }
        }

        col_writer.close()?;
        self.next_field_idx += 1;
        Ok(())
    }

    /// Sum of estimated in-memory bytes across the (still un-written)
    /// column writers in this row group. Useful for memory-bound tests
    /// and for callers that need to make backpressure decisions.
    ///
    /// Once a column has been written via [`Self::write_next_column`],
    /// its writer has been consumed — so this number reflects only
    /// future columns, which should hold near-zero memory until written.
    pub(crate) fn pending_writers_memory_size(&self) -> usize {
        self.pending_writers.iter().map(|w| w.memory_size()).sum()
    }

    /// Finalize the row group. Errors if any columns have not been
    /// written yet — the caller must complete the row group.
    pub(crate) fn finish(self) -> Result<(), ParquetWriteError> {
        if !self.pending_writers.is_empty() {
            return Err(ParquetWriteError::SchemaValidation(format!(
                "row group has {} unwritten columns",
                self.pending_writers.len(),
            )));
        }
        self.row_group_writer.close()?;
        Ok(())
    }
}

/// Dispatch one arrow `array` through `col_writer` based on the
/// arrow field's data type. Each call goes through one
/// `write_batch` invocation on the typed column writer; pages flush
/// to the underlying sink as `data_page_size_limit` /
/// `data_page_row_count_limit` thresholds are reached. Memory is
/// bounded by the in-progress page plus level-vector allocations
/// proportional to the input array length.
fn write_array_via_serialized_column_writer(
    col_writer: &mut parquet::file::writer::SerializedColumnWriter<'_>,
    field: &arrow::datatypes::Field,
    array: &ArrayRef,
) -> Result<(), ParquetWriteError> {
    use arrow::array::{
        Array as _, AsArray, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array,
        Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
        UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::DataType;
    use parquet::data_type::{BoolType, ByteArray, DoubleType, FloatType};

    match field.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("data_type() == Boolean implies BooleanArray");
            let typed = col_writer.typed::<BoolType>();
            if field.is_nullable() {
                let (def_levels, values) =
                    build_levels_and_filter_values(arr.len(), |i| arr.is_null(i), |i| arr.value(i));
                typed.write_batch(&values, Some(&def_levels), None)?;
            } else {
                let values: Vec<bool> = (0..arr.len()).map(|i| arr.value(i)).collect();
                typed.write_batch(&values, None, None)?;
            }
        }
        DataType::Int8 => write_int32_compatible(col_writer, field, array, |i| {
            array.as_any().downcast_ref::<Int8Array>().unwrap().value(i) as i32
        })?,
        DataType::Int16 => write_int32_compatible(col_writer, field, array, |i| {
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(i) as i32
        })?,
        DataType::Int32 => write_int32_compatible(col_writer, field, array, |i| {
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(i)
        })?,
        DataType::UInt8 => write_int32_compatible(col_writer, field, array, |i| {
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(i) as i32
        })?,
        DataType::UInt16 => write_int32_compatible(col_writer, field, array, |i| {
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(i) as i32
        })?,
        DataType::Int64 => write_int64_compatible(col_writer, field, array, |i| {
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(i)
        })?,
        DataType::UInt32 => write_int32_compatible(col_writer, field, array, |i| {
            // ArrowSchemaConverter maps `UInt32` to a parquet INT32
            // *physical* type with an unsigned logical annotation, so
            // the on-wire writer is `Int32Type`. The `u32 -> i32`
            // cast reinterprets bits; the unsigned logical annotation
            // tells readers to interpret those 32 bits back as `u32`,
            // so the round trip is bit-exact.
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(i) as i32
        })?,
        DataType::UInt64 => write_int64_compatible(col_writer, field, array, |i| {
            // UInt64 → Int64: caller's responsibility to ensure
            // values fit in i64 (true for timestamp_secs and other
            // metrics use cases).
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(i) as i64
        })?,
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            let typed = col_writer.typed::<FloatType>();
            if field.is_nullable() {
                let (defs, values) =
                    build_levels_and_filter_values(arr.len(), |i| arr.is_null(i), |i| arr.value(i));
                typed.write_batch(&values, Some(&defs), None)?;
            } else {
                typed.write_batch(arr.values(), None, None)?;
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let typed = col_writer.typed::<DoubleType>();
            if field.is_nullable() {
                let (defs, values) =
                    build_levels_and_filter_values(arr.len(), |i| arr.is_null(i), |i| arr.value(i));
                typed.write_batch(&values, Some(&defs), None)?;
            } else {
                typed.write_batch(arr.values(), None, None)?;
            }
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            write_byte_array(
                col_writer,
                field,
                arr.len(),
                |i| arr.is_null(i),
                |i| ByteArray::from(arr.value(i).as_bytes()),
            )?;
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            write_byte_array(
                col_writer,
                field,
                arr.len(),
                |i| arr.is_null(i),
                |i| ByteArray::from(arr.value(i).as_bytes()),
            )?;
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            write_byte_array(
                col_writer,
                field,
                arr.len(),
                |i| arr.is_null(i),
                |i| ByteArray::from(arr.value(i)),
            )?;
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            write_byte_array(
                col_writer,
                field,
                arr.len(),
                |i| arr.is_null(i),
                |i| ByteArray::from(arr.value(i)),
            )?;
        }
        DataType::Dictionary(_, value_type) => {
            // Materialize via `take(values, keys)` and recurse on the
            // resulting flat array. The materialized array preserves
            // the original null mask (taken from the dictionary's
            // keys).
            let dict = array.as_any_dictionary_opt().ok_or_else(|| {
                ParquetWriteError::SchemaValidation(format!(
                    "field '{}' has Dictionary data type but the array is not a DictionaryArray",
                    field.name(),
                ))
            })?;
            let materialized =
                arrow::compute::kernels::take::take(dict.values().as_ref(), dict.keys(), None)?;
            let materialized_field = arrow::datatypes::Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            );
            let materialized_ref: ArrayRef = materialized;
            // SAFETY-equivalent: dispatch on the materialized type. If
            // it's still nested (e.g., Dictionary of Dictionary), this
            // recursion handles it.
            return write_array_via_serialized_column_writer(
                col_writer,
                &materialized_field,
                &materialized_ref,
            );
        }
        // `List<T>` / `LargeList<T>` with non-nullable outer + inner.
        // The DDSketch `keys` (`List<Int16>`) and `counts`
        // (`List<UInt64>`) columns are this shape. We compute Dremel
        // def/rep levels from each input array and write them through
        // the same `SerializedColumnWriter::write_batch` call the flat
        // path uses, so memory stays bounded by one in-flight page.
        DataType::List(_) | DataType::LargeList(_) => {
            write_non_nullable_list_via_serialized_column_writer(col_writer, field, array)?;
        }
        // Multi-leaf nested (Struct, Map) and other unsupported types.
        // Single-leaf multi-child Structs are rejected at
        // `start_row_group` with a different error message; this arm
        // catches anything that slipped through (FixedSizeList,
        // ListView, decimals, etc.).
        other => {
            return Err(ParquetWriteError::SchemaValidation(format!(
                "field '{}' has unsupported data type {other:?} for write_next_column_arrays \
                 (supported: Boolean, Int8/16/32/64, UInt8/16/32/64, Float32/64, \
                 Utf8/LargeUtf8/Binary/LargeBinary, Dictionary over those, and List/LargeList<T> \
                 with non-nullable outer + inner where T is one of the flat primitives above)",
                field.name(),
            )));
        }
    }
    Ok(())
}

/// Page-bounded write for `List<T>` / `LargeList<T>` where the outer
/// field is non-nullable and the inner field is non-nullable. Computes
/// Dremel def/rep levels (max_def = 1, max_rep = 1) and dispatches the
/// flat inner values through the same typed `write_batch` call the flat
/// arms use. Pages flush as the writer's
/// `data_page_size_limit` / `data_page_row_count_limit` thresholds are
/// reached — same memory-bound contract as the flat path.
fn write_non_nullable_list_via_serialized_column_writer(
    col_writer: &mut parquet::file::writer::SerializedColumnWriter<'_>,
    field: &arrow::datatypes::Field,
    array: &arrow::array::ArrayRef,
) -> Result<(), ParquetWriteError> {
    use arrow::array::{Array, LargeListArray, ListArray};
    use arrow::datatypes::DataType;

    if field.is_nullable() {
        return Err(ParquetWriteError::SchemaValidation(format!(
            "field '{}' is a nullable List; only non-nullable List is supported on the streaming \
             write path",
            field.name(),
        )));
    }

    // Resolve inner field + values + per-row offsets uniformly across
    // List<T> and LargeList<T>. Offsets coerce to i64 so a single
    // function body handles both representations.
    let (inner_field, inner_values, offsets): (
        &arrow::datatypes::Field,
        &arrow::array::ArrayRef,
        Vec<i64>,
    ) = match field.data_type() {
        DataType::List(inner_field_ref) => {
            let arr = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                ParquetWriteError::SchemaValidation(format!(
                    "field '{}' has data type List but array is not a ListArray",
                    field.name(),
                ))
            })?;
            let offsets: Vec<i64> = arr.value_offsets().iter().map(|&o| o as i64).collect();
            (inner_field_ref.as_ref(), arr.values(), offsets)
        }
        DataType::LargeList(inner_field_ref) => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    ParquetWriteError::SchemaValidation(format!(
                        "field '{}' has data type LargeList but array is not a LargeListArray",
                        field.name(),
                    ))
                })?;
            let offsets: Vec<i64> = arr.value_offsets().to_vec();
            (inner_field_ref.as_ref(), arr.values(), offsets)
        }
        other => {
            return Err(ParquetWriteError::SchemaValidation(format!(
                "internal: write_non_nullable_list called with non-list type {other:?}",
            )));
        }
    };

    if inner_field.is_nullable() {
        return Err(ParquetWriteError::SchemaValidation(format!(
            "field '{}' has nullable list inner; only non-nullable inner is supported on the \
             streaming write path",
            field.name(),
        )));
    }

    // Walk per-row to build Dremel levels.
    //
    // Path: required outer group → repeated `list` → required `element`.
    // - max_rep_level = 1 (only `list` is repeated).
    // - max_def_level = 1 (the repeated `list` group can occur 0 times, which is how parquet
    //   encodes an empty list; 1 marks "element present").
    //
    // Per row:
    //  - empty list: emit one slot with def = 0, rep = 0, no value
    //  - list of N elements: emit N slots, def = 1 each, rep = 0 for the first and rep = 1 for the
    //    rest, plus N values.
    let num_rows = array.len();
    let total_present: usize = (0..num_rows)
        .map(|row| (offsets[row + 1] - offsets[row]).max(0) as usize)
        .sum();
    // Each row contributes either 1 level (empty) or list_len levels.
    let total_levels = (0..num_rows)
        .map(|row| {
            let len = (offsets[row + 1] - offsets[row]).max(0) as usize;
            if len == 0 { 1 } else { len }
        })
        .sum::<usize>();
    let mut def_levels: Vec<i16> = Vec::with_capacity(total_levels);
    let mut rep_levels: Vec<i16> = Vec::with_capacity(total_levels);
    for row in 0..num_rows {
        let start = offsets[row];
        let end = offsets[row + 1];
        let len = (end - start).max(0) as usize;
        if len == 0 {
            def_levels.push(0);
            rep_levels.push(0);
        } else {
            for i in 0..len {
                def_levels.push(1);
                rep_levels.push(if i == 0 { 0 } else { 1 });
            }
        }
    }

    // Dispatch the inner primitive through the appropriate typed
    // writer. Indexing iterates only the present (non-empty-list) rows
    // — start..end ranges, walked once for the whole array — so we
    // emit exactly `total_present` values.
    write_list_inner_values(
        col_writer,
        field,
        inner_field,
        inner_values,
        &offsets,
        total_present,
        &def_levels,
        &rep_levels,
    )
}

/// Type-dispatch for the flat inner values of a non-nullable List.
/// Calls `write_batch(values, def, rep)` on the typed writer matching
/// the inner physical type. Same shape as the flat-primitive arms in
/// [`write_array_via_serialized_column_writer`], just with def/rep
/// levels attached.
#[allow(clippy::too_many_arguments)]
fn write_list_inner_values(
    col_writer: &mut parquet::file::writer::SerializedColumnWriter<'_>,
    outer_field: &arrow::datatypes::Field,
    inner_field: &arrow::datatypes::Field,
    inner_values: &arrow::array::ArrayRef,
    offsets: &[i64],
    total_present: usize,
    def_levels: &[i16],
    rep_levels: &[i16],
) -> Result<(), ParquetWriteError> {
    use arrow::array::{
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::DataType;
    use parquet::data_type::{DoubleType, FloatType, Int32Type, Int64Type};

    // Walk the per-row [start, end) ranges once and gather the
    // present-only values into a contiguous Vec for `write_batch`.
    let collect_i32 = |extract: &dyn Fn(usize) -> i32| -> Vec<i32> {
        let mut out = Vec::with_capacity(total_present);
        for row in 0..(offsets.len() - 1) {
            let start = offsets[row].max(0) as usize;
            let end = offsets[row + 1].max(0) as usize;
            for i in start..end {
                out.push(extract(i));
            }
        }
        out
    };
    let collect_i64 = |extract: &dyn Fn(usize) -> i64| -> Vec<i64> {
        let mut out = Vec::with_capacity(total_present);
        for row in 0..(offsets.len() - 1) {
            let start = offsets[row].max(0) as usize;
            let end = offsets[row + 1].max(0) as usize;
            for i in start..end {
                out.push(extract(i));
            }
        }
        out
    };

    match inner_field.data_type() {
        DataType::Int8 => {
            let arr = inner_values.as_any().downcast_ref::<Int8Array>().unwrap();
            let values = collect_i32(&|i| arr.value(i) as i32);
            col_writer.typed::<Int32Type>().write_batch(
                &values,
                Some(def_levels),
                Some(rep_levels),
            )?;
        }
        DataType::Int16 => {
            let arr = inner_values.as_any().downcast_ref::<Int16Array>().unwrap();
            let values = collect_i32(&|i| arr.value(i) as i32);
            col_writer.typed::<Int32Type>().write_batch(
                &values,
                Some(def_levels),
                Some(rep_levels),
            )?;
        }
        DataType::Int32 => {
            let arr = inner_values.as_any().downcast_ref::<Int32Array>().unwrap();
            let values = collect_i32(&|i| arr.value(i));
            col_writer.typed::<Int32Type>().write_batch(
                &values,
                Some(def_levels),
                Some(rep_levels),
            )?;
        }
        DataType::UInt8 => {
            let arr = inner_values.as_any().downcast_ref::<UInt8Array>().unwrap();
            let values = collect_i32(&|i| arr.value(i) as i32);
            col_writer.typed::<Int32Type>().write_batch(
                &values,
                Some(def_levels),
                Some(rep_levels),
            )?;
        }
        DataType::UInt16 => {
            let arr = inner_values.as_any().downcast_ref::<UInt16Array>().unwrap();
            let values = collect_i32(&|i| arr.value(i) as i32);
            col_writer.typed::<Int32Type>().write_batch(
                &values,
                Some(def_levels),
                Some(rep_levels),
            )?;
        }
        DataType::UInt32 => {
            let arr = inner_values.as_any().downcast_ref::<UInt32Array>().unwrap();
            // Bit-reinterpret cast: the unsigned logical annotation
            // tells readers to interpret the on-wire i32 as u32.
            let values = collect_i32(&|i| arr.value(i) as i32);
            col_writer.typed::<Int32Type>().write_batch(
                &values,
                Some(def_levels),
                Some(rep_levels),
            )?;
        }
        DataType::Int64 => {
            let arr = inner_values.as_any().downcast_ref::<Int64Array>().unwrap();
            let values = collect_i64(&|i| arr.value(i));
            col_writer.typed::<Int64Type>().write_batch(
                &values,
                Some(def_levels),
                Some(rep_levels),
            )?;
        }
        DataType::UInt64 => {
            let arr = inner_values.as_any().downcast_ref::<UInt64Array>().unwrap();
            let values = collect_i64(&|i| arr.value(i) as i64);
            col_writer.typed::<Int64Type>().write_batch(
                &values,
                Some(def_levels),
                Some(rep_levels),
            )?;
        }
        DataType::Float32 => {
            let arr = inner_values
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let mut values = Vec::with_capacity(total_present);
            for row in 0..(offsets.len() - 1) {
                let start = offsets[row].max(0) as usize;
                let end = offsets[row + 1].max(0) as usize;
                for i in start..end {
                    values.push(arr.value(i));
                }
            }
            col_writer.typed::<FloatType>().write_batch(
                &values,
                Some(def_levels),
                Some(rep_levels),
            )?;
        }
        DataType::Float64 => {
            let arr = inner_values
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let mut values = Vec::with_capacity(total_present);
            for row in 0..(offsets.len() - 1) {
                let start = offsets[row].max(0) as usize;
                let end = offsets[row + 1].max(0) as usize;
                for i in start..end {
                    values.push(arr.value(i));
                }
            }
            col_writer.typed::<DoubleType>().write_batch(
                &values,
                Some(def_levels),
                Some(rep_levels),
            )?;
        }
        // Sketches don't need byte-array list inners; they use Int16 /
        // UInt64. Reject other inner types explicitly so the contract
        // is clear.
        other => {
            return Err(ParquetWriteError::SchemaValidation(format!(
                "field '{}' has List inner type {other:?}; only flat numeric primitive inners are \
                 supported (Int8/16/32/64, UInt8/16/32/64, Float32/64)",
                outer_field.name(),
            )));
        }
    }
    Ok(())
}

/// Helper for arrow types that map to parquet `Int32` (Int8, Int16,
/// Int32, UInt8, UInt16). Caller provides a closure that extracts
/// the i32 value at row `i`.
fn write_int32_compatible(
    col_writer: &mut parquet::file::writer::SerializedColumnWriter<'_>,
    field: &arrow::datatypes::Field,
    array: &ArrayRef,
    extract: impl Fn(usize) -> i32,
) -> Result<(), ParquetWriteError> {
    use parquet::data_type::Int32Type;
    let len = array.len();
    let typed = col_writer.typed::<Int32Type>();
    if field.is_nullable() {
        let (def_levels, values) =
            build_levels_and_filter_values(len, |i| array.is_null(i), extract);
        typed.write_batch(&values, Some(&def_levels), None)?;
    } else {
        let values: Vec<i32> = (0..len).map(extract).collect();
        typed.write_batch(&values, None, None)?;
    }
    Ok(())
}

/// Helper for arrow types that map to parquet `Int64`.
fn write_int64_compatible(
    col_writer: &mut parquet::file::writer::SerializedColumnWriter<'_>,
    field: &arrow::datatypes::Field,
    array: &ArrayRef,
    extract: impl Fn(usize) -> i64,
) -> Result<(), ParquetWriteError> {
    use parquet::data_type::Int64Type;
    let len = array.len();
    let typed = col_writer.typed::<Int64Type>();
    if field.is_nullable() {
        let (def_levels, values) =
            build_levels_and_filter_values(len, |i| array.is_null(i), extract);
        typed.write_batch(&values, Some(&def_levels), None)?;
    } else {
        let values: Vec<i64> = (0..len).map(extract).collect();
        typed.write_batch(&values, None, None)?;
    }
    Ok(())
}

/// Helper for byte-array-style arrow types (Utf8, LargeUtf8, Binary,
/// LargeBinary). Caller provides closures for null check and value
/// extraction (which produces a `ByteArray`).
fn write_byte_array(
    col_writer: &mut parquet::file::writer::SerializedColumnWriter<'_>,
    field: &arrow::datatypes::Field,
    len: usize,
    is_null: impl Fn(usize) -> bool,
    extract: impl Fn(usize) -> parquet::data_type::ByteArray,
) -> Result<(), ParquetWriteError> {
    use parquet::data_type::ByteArrayType;
    let typed = col_writer.typed::<ByteArrayType>();
    if field.is_nullable() {
        let (def_levels, values) = build_levels_and_filter_values(len, is_null, extract);
        typed.write_batch(&values, Some(&def_levels), None)?;
    } else {
        let values: Vec<_> = (0..len).map(extract).collect();
        typed.write_batch(&values, None, None)?;
    }
    Ok(())
}

/// Build parquet definition levels (1 = present, 0 = null at the
/// top-level of a non-nested column) and the matching filtered
/// values vector.
///
/// `is_null(i)` returns whether row `i` is null. `extract(i)` is
/// only called for non-null rows.
fn build_levels_and_filter_values<T>(
    len: usize,
    is_null: impl Fn(usize) -> bool,
    extract: impl Fn(usize) -> T,
) -> (Vec<i16>, Vec<T>) {
    let mut def_levels = Vec::with_capacity(len);
    let mut values = Vec::with_capacity(len);
    for i in 0..len {
        if is_null(i) {
            def_levels.push(0);
        } else {
            def_levels.push(1);
            values.push(extract(i));
        }
    }
    (def_levels, values)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
        UInt8Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::EnabledStatistics;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::serialized_reader::ReadOptionsBuilder;

    use super::*;
    use crate::storage::ParquetWriterConfig;

    /// Build a metrics-shaped batch with `num_rows` rows. metric_name
    /// alternates between two values; `service` is non-null on every
    /// row. Mirrors the shape ParquetWriter expects (required fields +
    /// at least one tag column).
    fn make_metrics_batch(num_rows: usize) -> RecordBatch {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timeseries_id", DataType::Int64, false),
            Field::new("service", dict_type, true),
        ]));

        let metric_keys: Vec<i32> = (0..num_rows as i32).map(|i| i % 2).collect();
        let metric_values = StringArray::from(vec!["cpu.usage", "memory.used"]);
        let metric_name: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(metric_keys),
                Arc::new(metric_values),
            )
            .unwrap(),
        );

        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
        let timestamps: Vec<u64> = (0..num_rows as u64).map(|i| 1_700_000_000 + i).collect();
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let values: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();
        let value: ArrayRef = Arc::new(Float64Array::from(values));
        let tsids: Vec<i64> = (0..num_rows as i64).map(|i| 1000 + i).collect();
        let timeseries_id: ArrayRef = Arc::new(Int64Array::from(tsids));

        let svc_keys: Vec<Option<i32>> = (0..num_rows as i32).map(|i| Some(i % 3)).collect();
        let svc_values = StringArray::from(vec!["api", "db", "cache"]);
        let service: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(svc_keys),
                Arc::new(svc_values),
            )
            .unwrap(),
        );

        RecordBatch::try_new(
            schema,
            vec![
                metric_name,
                metric_type,
                timestamp_secs,
                value,
                timeseries_id,
                service,
            ],
        )
        .unwrap()
    }

    /// A `WriterProperties` that mirrors what production
    /// `ParquetWriter::to_writer_properties_with_metadata` would build,
    /// minus the bits PR-1 changes (page-level statistics). PR-2
    /// metadata-identity tests use this to compare PR-2 vs ArrowWriter
    /// output produced under identical settings.
    fn writer_props_with_kv(arrow_schema: &ArrowSchema, kvs: Vec<KeyValue>) -> WriterProperties {
        let cfg = ParquetWriterConfig::default();
        let sort_field_names = vec!["metric_name".to_string(), "service".to_string()];
        // Populate `sorting_columns` so the metadata-identity test
        // exercises a non-empty sorting_columns vector. Indices are
        // resolved against the test schema (metric_name=0, service=5).
        let sorting_cols = vec![
            parquet::file::metadata::SortingColumn {
                column_idx: arrow_schema.index_of("metric_name").unwrap() as i32,
                descending: false,
                nulls_first: false,
            },
            parquet::file::metadata::SortingColumn {
                column_idx: arrow_schema.index_of("service").unwrap() as i32,
                descending: false,
                nulls_first: false,
            },
        ];
        cfg.to_writer_properties_with_metadata(
            arrow_schema,
            sorting_cols,
            Some(kvs),
            &sort_field_names,
        )
    }

    /// Write `batches` to bytes through `StreamingParquetWriter`,
    /// putting each batch in its own row group.
    fn write_streaming(
        arrow_schema: SchemaRef,
        props: WriterProperties,
        batches: &[RecordBatch],
    ) -> Vec<u8> {
        let mut out: Vec<u8> = Vec::new();
        {
            let mut w = StreamingParquetWriter::try_new(&mut out, arrow_schema, props).unwrap();
            for batch in batches {
                let mut rg = w.start_row_group().unwrap();
                for col_idx in 0..batch.num_columns() {
                    rg.write_next_column(batch.column(col_idx)).unwrap();
                }
                rg.finish().unwrap();
            }
            w.close().unwrap();
        }
        out
    }

    /// Write `batches` to bytes through `ArrowWriter`, with one row
    /// group per batch (forced via `flush()`).
    fn write_arrow_writer(
        arrow_schema: SchemaRef,
        props: WriterProperties,
        batches: &[RecordBatch],
    ) -> Vec<u8> {
        let mut out: Vec<u8> = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut out, arrow_schema, Some(props)).unwrap();
            for (idx, batch) in batches.iter().enumerate() {
                w.write(batch).unwrap();
                if idx + 1 < batches.len() {
                    w.flush().unwrap();
                }
            }
            w.close().unwrap();
        }
        out
    }

    /// Read a Parquet file from `bytes` into a single concatenated
    /// RecordBatch (concatenating all row groups).
    fn read_back(bytes: &[u8]) -> RecordBatch {
        let cursor = bytes::Bytes::from(bytes.to_vec());
        let reader = ParquetRecordBatchReaderBuilder::try_new(cursor)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let schema = batches[0].schema();
        arrow::compute::concat_batches(&schema, &batches).unwrap()
    }

    // -------- PW-A: round-trip --------

    /// Write one row group via the streaming writer; read it back and
    /// expect the data to match.
    #[test]
    fn test_round_trip_single_row_group() {
        let batch = make_metrics_batch(64);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        let bytes = write_streaming(arrow_schema.clone(), props, std::slice::from_ref(&batch));
        let actual = read_back(&bytes);

        assert_eq!(actual.num_rows(), batch.num_rows());
        assert_eq!(actual.num_columns(), batch.num_columns());

        // The dict types may be materialized to Utf8 on read-back since
        // we don't embed ARROW:schema. Compare on the value level by
        // converting both to strings for the dict columns.
        for col_idx in 0..batch.num_columns() {
            let original = batch.column(col_idx);
            let recovered = actual.column(col_idx);
            assert_eq!(
                original.len(),
                recovered.len(),
                "column {} length mismatch",
                col_idx,
            );
        }
        let mn_idx = actual.schema().index_of("metric_name").unwrap();
        let mn_strings = column_as_strings(actual.column(mn_idx));
        let exp_strings =
            column_as_strings(batch.column(batch.schema().index_of("metric_name").unwrap()));
        assert_eq!(mn_strings, exp_strings);
    }

    /// Two batches → two row groups; concatenated read-back equals
    /// concatenated input.
    #[test]
    fn test_round_trip_multi_row_group() {
        let batch1 = make_metrics_batch(40);
        let batch2 = make_metrics_batch(20);
        let arrow_schema = batch1.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        let bytes = write_streaming(
            arrow_schema.clone(),
            props,
            &[batch1.clone(), batch2.clone()],
        );
        let actual = read_back(&bytes);

        assert_eq!(actual.num_rows(), batch1.num_rows() + batch2.num_rows());

        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 2);
    }

    // -------- PW-B: metadata identity vs ArrowWriter --------

    /// PW-B: schema descriptor, sorting_columns, KV metadata,
    /// statistics enabled level, compression, bloom filter presence,
    /// and num row groups must agree with what ArrowWriter produces.
    #[test]
    fn test_metadata_identity_vs_arrow_writer() {
        let batch = make_metrics_batch(64);
        let arrow_schema = batch.schema();

        let kvs = vec![
            KeyValue::new(
                "qh.sort_fields".to_string(),
                "metric_name|timestamp/V2".to_string(),
            ),
            KeyValue::new("qh.window_start".to_string(), "1700000000".to_string()),
            KeyValue::new("qh.window_duration_secs".to_string(), "900".to_string()),
        ];

        let bytes_streaming = write_streaming(
            arrow_schema.clone(),
            writer_props_with_kv(&arrow_schema, kvs.clone()),
            std::slice::from_ref(&batch),
        );
        let bytes_arrow = write_arrow_writer(
            arrow_schema.clone(),
            writer_props_with_kv(&arrow_schema, kvs.clone()),
            std::slice::from_ref(&batch),
        );

        let r_streaming = SerializedFileReader::new(bytes::Bytes::from(bytes_streaming)).unwrap();
        let r_arrow = SerializedFileReader::new(bytes::Bytes::from(bytes_arrow)).unwrap();

        let m_streaming = r_streaming.metadata();
        let m_arrow = r_arrow.metadata();

        assert_eq!(m_streaming.num_row_groups(), m_arrow.num_row_groups());

        // Schema descriptor: column count, names, types.
        let s_streaming = m_streaming.file_metadata().schema_descr();
        let s_arrow = m_arrow.file_metadata().schema_descr();
        assert_eq!(s_streaming.num_columns(), s_arrow.num_columns());
        for i in 0..s_streaming.num_columns() {
            assert_eq!(
                s_streaming.column(i).name(),
                s_arrow.column(i).name(),
                "column {} name mismatch",
                i,
            );
            assert_eq!(
                format!("{:?}", s_streaming.column(i).physical_type()),
                format!("{:?}", s_arrow.column(i).physical_type()),
                "column {} ({}) physical type mismatch",
                i,
                s_streaming.column(i).name(),
            );
        }

        // Sorting columns (per row group).
        for rg_idx in 0..m_streaming.num_row_groups() {
            let sc_streaming = m_streaming.row_group(rg_idx).sorting_columns();
            let sc_arrow = m_arrow.row_group(rg_idx).sorting_columns();
            assert_eq!(
                sc_streaming, sc_arrow,
                "sorting_columns for row group {} differ",
                rg_idx,
            );
        }

        // KV metadata: every qh.* entry from the input must be present
        // in both, and the ARROW:schema entry should be absent from
        // streaming (since we don't add it implicitly) — relevant qh.*
        // entries must be byte-equal.
        let kv_streaming = m_streaming
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();
        let kv_arrow = m_arrow
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();

        for input_kv in &kvs {
            let in_streaming = kv_streaming.iter().find(|kv| kv.key == input_kv.key);
            let in_arrow = kv_arrow.iter().find(|kv| kv.key == input_kv.key);
            assert!(
                in_streaming.is_some(),
                "qh.* key {} missing from streaming output",
                input_kv.key,
            );
            assert!(
                in_arrow.is_some(),
                "qh.* key {} missing from arrow output",
                input_kv.key,
            );
            assert_eq!(
                in_streaming.unwrap().value,
                in_arrow.unwrap().value,
                "value for key {} differs between writers",
                input_kv.key,
            );
        }

        // Per-column compression and statistics-enabled level.
        for rg_idx in 0..m_streaming.num_row_groups() {
            let rg_streaming = m_streaming.row_group(rg_idx);
            let rg_arrow = m_arrow.row_group(rg_idx);
            assert_eq!(rg_streaming.num_columns(), rg_arrow.num_columns());
            assert_eq!(
                rg_streaming.num_rows(),
                rg_arrow.num_rows(),
                "row group {} num_rows mismatch",
                rg_idx,
            );
            for col_idx in 0..rg_streaming.num_columns() {
                let c_streaming = rg_streaming.column(col_idx);
                let c_arrow = rg_arrow.column(col_idx);
                assert_eq!(
                    c_streaming.compression(),
                    c_arrow.compression(),
                    "column {} compression mismatch",
                    col_idx,
                );
                assert_eq!(
                    c_streaming.bloom_filter_offset().is_some(),
                    c_arrow.bloom_filter_offset().is_some(),
                    "column {} ('{}') bloom filter presence mismatch",
                    col_idx,
                    c_streaming.column_descr().name(),
                );
            }
        }
    }

    /// PW-B (statistics_enabled): the writer must propagate the
    /// per-column statistics_enabled level from properties. Default
    /// config switched from `Chunk` to `Page` in #6377 (PR-1). `Page`
    /// implies stats at both the column-chunk *and* page levels, so
    /// chunk-level stats remain present and the page index now lands
    /// in the file too.
    #[test]
    fn test_statistics_enabled_propagates() {
        let batch = make_metrics_batch(32);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());
        let metric_name_path =
            parquet::schema::types::ColumnPath::new(vec!["metric_name".to_string()]);
        assert_eq!(
            props.statistics_enabled(&metric_name_path),
            EnabledStatistics::Page,
        );

        let bytes = write_streaming(arrow_schema, props, std::slice::from_ref(&batch));
        let opts = ReadOptionsBuilder::new().with_page_index().build();
        let reader =
            SerializedFileReader::new_with_options(bytes::Bytes::from(bytes), opts).unwrap();
        let rg = reader.metadata().row_group(0);
        let mn_col = rg
            .columns()
            .iter()
            .find(|c| c.column_descr().name() == "metric_name")
            .unwrap();
        assert!(
            mn_col.statistics().is_some(),
            "chunk-level statistics expected (Page implies Chunk)",
        );
        // PR-1 also enables the column / offset indexes for query
        // pruning; verify they show up.
        let metric_name_idx = rg
            .columns()
            .iter()
            .position(|c| c.column_descr().name() == "metric_name")
            .unwrap();
        let column_index_present = match reader.metadata().column_index() {
            Some(ci) => !ci.is_empty() && !ci[0].is_empty(),
            None => false,
        };
        assert!(
            column_index_present,
            "page-level column index expected with EnabledStatistics::Page",
        );
        let offset_index_has_pages = match reader.metadata().offset_index() {
            Some(oi) => {
                !oi.is_empty()
                    && !oi[0].is_empty()
                    && !oi[0][metric_name_idx].page_locations.is_empty()
            }
            None => false,
        };
        assert!(
            offset_index_has_pages,
            "offset index with non-empty page locations expected with EnabledStatistics::Page",
        );
    }

    // -------- PW-C: bounded memory --------

    /// PW-C: after writing each column the per-row-group writer should
    /// not be retaining old columns' chunk buffers. Concretely, the
    /// pending writers reported by `pending_writers_memory_size` is a
    /// monotone-decreasing-or-flat sequence as columns are written
    /// (since pending writers shrink, and unwritten writers carry
    /// negligible memory).
    #[test]
    fn test_bounded_memory_per_column() {
        let batch = make_metrics_batch(4096);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        let mut out: Vec<u8> = Vec::new();
        {
            let mut w =
                StreamingParquetWriter::try_new(&mut out, arrow_schema.clone(), props).unwrap();
            let mut rg = w.start_row_group().unwrap();

            let mut prior = rg.pending_writers_memory_size();
            for col_idx in 0..batch.num_columns() {
                rg.write_next_column(batch.column(col_idx)).unwrap();
                let now = rg.pending_writers_memory_size();
                // Writing a column either reduces pending memory or
                // leaves it unchanged (e.g., if writer N+1 already had
                // zero memory). It must NEVER grow above the prior
                // observation — that would mean we're accumulating.
                assert!(
                    now <= prior,
                    "pending memory grew after writing column {} ('{}'): {} -> {}",
                    col_idx,
                    batch.schema().field(col_idx).name(),
                    prior,
                    now,
                );
                prior = now;
            }
            // After all columns written, no pending writers remain.
            assert_eq!(rg.pending_writers_memory_size(), 0);
            rg.finish().unwrap();
            w.close().unwrap();
        }
    }

    // -------- Edge cases --------

    /// An empty row group must still produce a readable file with the
    /// expected schema (zero rows is the natural lower bound).
    #[test]
    fn test_empty_row_group_produces_valid_file() {
        let batch = make_metrics_batch(0);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        let bytes = write_streaming(arrow_schema.clone(), props, std::slice::from_ref(&batch));
        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        assert_eq!(reader.metadata().file_metadata().num_rows(), 0);
        assert_eq!(reader.metadata().num_row_groups(), 1);
    }

    /// Calling `write_next_column` past the last field must fail with a
    /// structured error rather than panicking.
    #[test]
    fn test_too_many_columns_returns_error() {
        let batch = make_metrics_batch(16);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        let mut out: Vec<u8> = Vec::new();
        let mut w = StreamingParquetWriter::try_new(&mut out, arrow_schema.clone(), props).unwrap();
        let mut rg = w.start_row_group().unwrap();
        for col_idx in 0..batch.num_columns() {
            rg.write_next_column(batch.column(col_idx)).unwrap();
        }
        // One past the end.
        let extra = batch.column(0);
        let err = rg.write_next_column(extra).unwrap_err();
        match err {
            ParquetWriteError::SchemaValidation(_) => {}
            other => panic!("expected SchemaValidation, got {other:?}"),
        }
    }

    /// A row count mismatch between columns must fail with a structured
    /// error before reaching the parquet layer.
    #[test]
    fn test_row_count_mismatch_returns_error() {
        let batch = make_metrics_batch(32);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        let mut out: Vec<u8> = Vec::new();
        let mut w = StreamingParquetWriter::try_new(&mut out, arrow_schema.clone(), props).unwrap();
        let mut rg = w.start_row_group().unwrap();

        // Write column 0 with 32 rows.
        rg.write_next_column(batch.column(0)).unwrap();

        // Build a column-1 array with the wrong length (16 instead of 32).
        let short: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; 16]));
        let err = rg.write_next_column(&short).unwrap_err();
        match err {
            ParquetWriteError::SchemaValidation(_) => {}
            other => panic!("expected SchemaValidation, got {other:?}"),
        }
    }

    /// `finish` before all columns are written must fail with a
    /// structured error rather than producing a corrupt file.
    #[test]
    fn test_finish_before_all_columns_returns_error() {
        let batch = make_metrics_batch(8);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        let mut out: Vec<u8> = Vec::new();
        let mut w = StreamingParquetWriter::try_new(&mut out, arrow_schema.clone(), props).unwrap();
        let rg = w.start_row_group().unwrap();
        // Don't write any columns.
        let err = rg.finish().unwrap_err();
        match err {
            ParquetWriteError::SchemaValidation(_) => {}
            other => panic!("expected SchemaValidation, got {other:?}"),
        }
    }

    /// Round-trip with nulls in a nullable dictionary column: nulls
    /// must come back as nulls, non-nulls must come back as the same
    /// values. The metrics schema has `service` as nullable, so this
    /// case is on the production hot path.
    #[test]
    fn test_round_trip_preserves_nulls() {
        let batch = make_metrics_batch_with_nulls(20);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        let bytes = write_streaming(arrow_schema.clone(), props, std::slice::from_ref(&batch));
        let actual = read_back(&bytes);

        let svc_idx_in = batch.schema().index_of("service").unwrap();
        let svc_idx_out = actual.schema().index_of("service").unwrap();
        let original_strings = column_as_strings(batch.column(svc_idx_in));
        let recovered_strings = column_as_strings(actual.column(svc_idx_out));
        assert_eq!(original_strings, recovered_strings);

        // Verify there is at least one null in the input (otherwise the
        // test would silently degrade if the helper changed).
        assert!(
            original_strings.iter().any(|v| v.is_none()),
            "test fixture must contain at least one null",
        );
    }

    /// Multi-row-group metadata identity vs ArrowWriter. The previous
    /// metadata test only exercised one row group; this exercises that
    /// per-row-group metadata (sorting_columns, num_rows, column
    /// compressions) still agrees when the file has multiple row groups.
    #[test]
    fn test_metadata_identity_multi_row_group() {
        let batch_a = make_metrics_batch(48);
        let batch_b = make_metrics_batch(16);
        let arrow_schema = batch_a.schema();
        let kvs = vec![KeyValue::new(
            "qh.sort_fields".to_string(),
            "metric_name|timestamp/V2".to_string(),
        )];

        let bytes_streaming = write_streaming(
            arrow_schema.clone(),
            writer_props_with_kv(&arrow_schema, kvs.clone()),
            &[batch_a.clone(), batch_b.clone()],
        );
        let bytes_arrow = write_arrow_writer(
            arrow_schema.clone(),
            writer_props_with_kv(&arrow_schema, kvs.clone()),
            &[batch_a, batch_b],
        );

        let r_streaming = SerializedFileReader::new(bytes::Bytes::from(bytes_streaming)).unwrap();
        let r_arrow = SerializedFileReader::new(bytes::Bytes::from(bytes_arrow)).unwrap();

        assert_eq!(r_streaming.metadata().num_row_groups(), 2);
        assert_eq!(
            r_streaming.metadata().num_row_groups(),
            r_arrow.metadata().num_row_groups(),
        );
        for rg_idx in 0..2 {
            let rg_s = r_streaming.metadata().row_group(rg_idx);
            let rg_a = r_arrow.metadata().row_group(rg_idx);
            assert_eq!(rg_s.num_rows(), rg_a.num_rows());
            assert_eq!(rg_s.sorting_columns(), rg_a.sorting_columns());
            assert_eq!(rg_s.num_columns(), rg_a.num_columns());
            for col_idx in 0..rg_s.num_columns() {
                assert_eq!(
                    rg_s.column(col_idx).compression(),
                    rg_a.column(col_idx).compression(),
                );
            }
        }
    }

    /// Bloom filters written via the streaming writer must be
    /// functionally equivalent to those written by ArrowWriter — a
    /// value present in the data is reported as possibly-present, and
    /// a value absent from the data is reported as definitely-absent
    /// (with very high probability for our 5% FPP setting).
    #[test]
    fn test_bloom_filter_round_trip() {
        let batch = make_metrics_batch(64);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        let bytes = write_streaming(arrow_schema, props, std::slice::from_ref(&batch));
        // Reader must be constructed with read_bloom_filter enabled —
        // by default ReaderProperties does NOT load bloom filters from
        // the file, so even a correctly-written filter would appear
        // absent. We mirror the production read-side configuration that
        // a future caller would use to verify presence.
        let reader_props = parquet::file::properties::ReaderProperties::builder()
            .set_read_bloom_filter(true)
            .build();
        let opts = ReadOptionsBuilder::new()
            .with_reader_properties(reader_props)
            .build();
        let reader =
            SerializedFileReader::new_with_options(bytes::Bytes::from(bytes), opts).unwrap();
        let rg = reader.get_row_group(0).unwrap();
        let mn_col_idx = (0..rg.metadata().num_columns())
            .find(|&i| rg.metadata().column(i).column_descr().name() == "metric_name")
            .expect("metric_name column present");
        let bloom = rg
            .get_column_bloom_filter(mn_col_idx)
            .expect("bloom filter must be present for metric_name");
        // Present values: metric_name is "cpu.usage" or "memory.used"
        // throughout the batch.
        assert!(
            bloom.check(&"cpu.usage"),
            "bloom should match present value"
        );
        assert!(
            bloom.check(&"memory.used"),
            "bloom should match present value"
        );
        // A value not in the batch — the test fixture only writes
        // these two metric names, so this should be filtered out with
        // very high probability.
        assert!(
            !bloom.check(&"definitely.not.in.batch"),
            "bloom must reject absent value (5% FPP makes a false hit very unlikely)",
        );
    }

    /// A schema with a *multi-leaf* nested top-level field (e.g. a
    /// Struct with several primitive children) must be rejected at
    /// `start_row_group`. Single-leaf nested fields (List<primitive>,
    /// LargeList<primitive>) ARE supported — see
    /// `test_list_uint64_round_trip_through_array_stream` for the
    /// DDSketch shape.
    #[test]
    fn test_multi_leaf_nested_type_rejected_at_start_row_group() {
        let inner_fields = arrow::datatypes::Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "nested",
            DataType::Struct(inner_fields),
            false,
        )]));
        let props = WriterProperties::builder().build();

        let mut out: Vec<u8> = Vec::new();
        let mut w = StreamingParquetWriter::try_new(&mut out, schema, props).unwrap();
        match w.start_row_group() {
            Ok(_) => panic!("expected multi-leaf nested type to be rejected"),
            Err(ParquetWriteError::SchemaValidation(msg)) => {
                assert!(
                    msg.contains("multi-leaf nested"),
                    "error message should mention multi-leaf nested: {msg}",
                );
            }
            Err(other) => panic!("expected SchemaValidation, got {other:?}"),
        }
    }

    /// Build a batch where the `service` column has nulls interleaved
    /// with non-null values. Used by [`test_round_trip_preserves_nulls`].
    fn make_metrics_batch_with_nulls(num_rows: usize) -> RecordBatch {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timeseries_id", DataType::Int64, false),
            Field::new("service", dict_type, true),
        ]));
        let metric_keys: Vec<i32> = (0..num_rows as i32).map(|i| i % 2).collect();
        let metric_values = StringArray::from(vec!["cpu.usage", "memory.used"]);
        let metric_name: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(metric_keys),
                Arc::new(metric_values),
            )
            .unwrap(),
        );
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
        let timestamps: Vec<u64> = (0..num_rows as u64).map(|i| 1_700_000_000 + i).collect();
        let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));
        let values: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();
        let value: ArrayRef = Arc::new(Float64Array::from(values));
        let tsids: Vec<i64> = (0..num_rows as i64).map(|i| 1000 + i).collect();
        let timeseries_id: ArrayRef = Arc::new(Int64Array::from(tsids));
        // Every third row gets a null service.
        let svc_keys: Vec<Option<i32>> = (0..num_rows as i32)
            .map(|i| if i % 3 == 0 { None } else { Some(i % 2) })
            .collect();
        let svc_values = StringArray::from(vec!["api", "db"]);
        let service: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(svc_keys),
                Arc::new(svc_values),
            )
            .unwrap(),
        );
        RecordBatch::try_new(
            schema,
            vec![
                metric_name,
                metric_type,
                timestamp_secs,
                value,
                timeseries_id,
                service,
            ],
        )
        .unwrap()
    }

    /// Helper: extract the string value at each row of a column (handles
    /// both Dictionary(Int32, Utf8) and plain Utf8).
    fn column_as_strings(col: &ArrayRef) -> Vec<Option<String>> {
        if let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..dict.len())
                .map(|i| {
                    if dict.is_null(i) {
                        None
                    } else {
                        Some(values.value(dict.keys().value(i) as usize).to_string())
                    }
                })
                .collect()
        } else if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
            (0..s.len())
                .map(|i| {
                    if s.is_null(i) {
                        None
                    } else {
                        Some(s.value(i).to_string())
                    }
                })
                .collect()
        } else {
            panic!(
                "column_as_strings: unsupported column type {:?}",
                col.data_type()
            );
        }
    }

    // -------- PB-* : write_next_column_arrays (page-stream input) --------

    /// Slice a record batch's columns into N approximately-equal
    /// chunks, returning a `Vec<Vec<ArrayRef>>` indexed by
    /// `[col_idx][chunk_idx]`. Used by the array-stream tests to
    /// simulate a PR-6 merge driver feeding pages one at a time.
    fn slice_columns_into_chunks(batch: &RecordBatch, num_chunks: usize) -> Vec<Vec<ArrayRef>> {
        let total_rows = batch.num_rows();
        let chunk_size = total_rows.div_ceil(num_chunks);
        let mut per_col: Vec<Vec<ArrayRef>> =
            (0..batch.num_columns()).map(|_| Vec::new()).collect();
        let mut offset = 0usize;
        while offset < total_rows {
            let len = chunk_size.min(total_rows - offset);
            for (col_idx, col_vec) in per_col.iter_mut().enumerate() {
                col_vec.push(batch.column(col_idx).slice(offset, len));
            }
            offset += len;
        }
        per_col
    }

    /// PB-A: writing a column as N chunks via `write_next_column_arrays`
    /// and reading back the file produces the same logical values as
    /// writing the same column as one piece via `write_next_column`.
    #[test]
    fn test_round_trip_column_arrays() {
        let batch = make_metrics_batch(64);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());
        let chunked = slice_columns_into_chunks(&batch, 8);

        let mut out: Vec<u8> = Vec::new();
        {
            let mut w =
                StreamingParquetWriter::try_new(&mut out, arrow_schema.clone(), props).unwrap();
            let mut rg = w.start_row_group().unwrap();
            for chunks in &chunked {
                rg.write_next_column_arrays(chunks.clone()).unwrap();
            }
            rg.finish().unwrap();
            w.close().unwrap();
        }

        let actual = read_back(&out);
        assert_eq!(actual.num_rows(), batch.num_rows());
        assert_eq!(actual.num_columns(), batch.num_columns());

        let mn_idx = actual.schema().index_of("metric_name").unwrap();
        let exp_strings =
            column_as_strings(batch.column(batch.schema().index_of("metric_name").unwrap()));
        let act_strings = column_as_strings(actual.column(mn_idx));
        assert_eq!(act_strings, exp_strings);

        let ts_idx_exp = batch.schema().index_of("timestamp_secs").unwrap();
        let ts_idx_act = actual.schema().index_of("timestamp_secs").unwrap();
        let ts_exp = batch
            .column(ts_idx_exp)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let ts_act = actual
            .column(ts_idx_act)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..ts_exp.len() {
            assert_eq!(ts_exp.value(i), ts_act.value(i));
        }
    }

    /// PB-A nulls: nulls in a nullable dictionary column round-trip
    /// when fed via the array-stream API.
    #[test]
    fn test_round_trip_column_arrays_with_nulls() {
        let batch = make_metrics_batch_with_nulls(40);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());
        let chunked = slice_columns_into_chunks(&batch, 5);

        let mut out: Vec<u8> = Vec::new();
        {
            let mut w =
                StreamingParquetWriter::try_new(&mut out, arrow_schema.clone(), props).unwrap();
            let mut rg = w.start_row_group().unwrap();
            for chunks in &chunked {
                rg.write_next_column_arrays(chunks.clone()).unwrap();
            }
            rg.finish().unwrap();
            w.close().unwrap();
        }

        let actual = read_back(&out);
        let svc_in = batch.column(batch.schema().index_of("service").unwrap());
        let svc_out = actual.column(actual.schema().index_of("service").unwrap());
        assert_eq!(column_as_strings(svc_in), column_as_strings(svc_out));
        assert!(
            column_as_strings(svc_in).iter().any(|v| v.is_none()),
            "fixture must have at least one null",
        );
    }

    /// `UInt32` columns must round trip through `write_next_column_arrays`.
    ///
    /// Regression test: an earlier dispatch routed `UInt32` through the
    /// `Int64` writer path. `ArrowSchemaConverter` maps Arrow `UInt32`
    /// to a parquet INT32 *physical* type with an unsigned logical
    /// annotation, so an `Int64` writer over an INT32 column hits a
    /// runtime physical-type mismatch — affecting any production
    /// schema with a `UInt32` field (e.g., the sketch `flags` column
    /// in `quickwit-parquet-engine/src/schema/sketch_fields.rs`).
    ///
    /// The fixture includes values `>= 2^31` to verify the
    /// `u32 -> i32` cast preserves bits and the unsigned logical
    /// annotation is honoured on read-back.
    #[test]
    fn test_uint32_round_trip_through_array_stream() {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("metric_name", dict_type.clone(), false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("timestamp_secs", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timeseries_id", DataType::Int64, false),
            Field::new("service", dict_type, true),
            Field::new("flags", DataType::UInt32, false),
        ]));

        let num_rows = 8;
        let metric_keys: Vec<i32> = (0..num_rows as i32).map(|i| i % 2).collect();
        let metric_values = StringArray::from(vec!["cpu.usage", "memory.used"]);
        let metric_name: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(metric_keys),
                Arc::new(metric_values),
            )
            .unwrap(),
        );
        let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));
        let timestamp_secs: ArrayRef =
            Arc::new(UInt64Array::from((0..num_rows as u64).collect::<Vec<_>>()));
        let value: ArrayRef = Arc::new(Float64Array::from(
            (0..num_rows).map(|i| i as f64).collect::<Vec<_>>(),
        ));
        let timeseries_id: ArrayRef =
            Arc::new(Int64Array::from((0..num_rows as i64).collect::<Vec<_>>()));
        let svc_keys: Vec<Option<i32>> = (0..num_rows as i32).map(|i| Some(i % 3)).collect();
        let svc_values = StringArray::from(vec!["api", "db", "cache"]);
        let service: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(svc_keys),
                Arc::new(svc_values),
            )
            .unwrap(),
        );
        // Includes the high-bit-set range that would clip if written
        // through an `i32` writer without the unsigned logical
        // annotation.
        let flag_values: Vec<u32> = vec![
            0,
            1,
            42,
            0x7FFF_FFFF,
            0x8000_0000,
            0xFFFF_FFFE,
            0xFFFF_FFFF,
            0xCAFE_F00D,
        ];
        let flags: ArrayRef = Arc::new(UInt32Array::from(flag_values.clone()));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                metric_name,
                metric_type,
                timestamp_secs,
                value,
                timeseries_id,
                service,
                flags,
            ],
        )
        .unwrap();

        let props = writer_props_with_kv(&schema, Vec::new());
        let mut out: Vec<u8> = Vec::new();
        {
            let mut w = StreamingParquetWriter::try_new(&mut out, schema.clone(), props).unwrap();
            let mut rg = w.start_row_group().unwrap();
            for col_idx in 0..batch.num_columns() {
                rg.write_next_column_arrays([batch.column(col_idx).clone()])
                    .unwrap();
            }
            rg.finish().unwrap();
            w.close().unwrap();
        }

        let actual = read_back(&out);
        let flags_idx = actual.schema().index_of("flags").unwrap();
        let flags_col = actual
            .column(flags_idx)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("flags column must round trip as UInt32");
        let actual_values: Vec<u32> = (0..flags_col.len()).map(|i| flags_col.value(i)).collect();
        assert_eq!(
            actual_values, flag_values,
            "UInt32 round trip diverged — likely INT32-vs-INT64 physical-type mismatch",
        );
    }

    /// `List<UInt64>` columns must round-trip through
    /// `write_next_column_arrays`. This is the shape of the DDSketch
    /// `counts` and `keys` columns
    /// (`quickwit-parquet-engine/src/schema/sketch_fields.rs:68`).
    /// Multiple input arrays are fed in to exercise the per-array
    /// loop in the nested fallback. Variable list lengths catch
    /// def/rep level bugs.
    #[test]
    fn test_list_uint64_round_trip_through_array_stream() {
        use arrow::array::ListBuilder;

        let item_field = Arc::new(Field::new("item", DataType::UInt64, false));
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "counts",
            DataType::List(Arc::clone(&item_field)),
            false,
        )]));

        // Two input arrays simulating two pages worth of merge input.
        // List lengths vary across rows.
        let make_list = |rows: &[&[u64]]| -> ArrayRef {
            let mut builder = ListBuilder::new(arrow::array::UInt64Builder::new())
                .with_field(Arc::clone(&item_field));
            for row in rows {
                for &v in *row {
                    builder.values().append_value(v);
                }
                builder.append(true);
            }
            Arc::new(builder.finish())
        };
        let arr_a = make_list(&[&[1, 2, 3], &[], &[42, 7]]);
        let arr_b = make_list(&[&[100, 200], &[u64::MAX, 0]]);
        let total_rows = arr_a.len() + arr_b.len();
        assert_eq!(total_rows, 5);

        let props = WriterProperties::builder().build();
        let mut out: Vec<u8> = Vec::new();
        {
            let mut w = StreamingParquetWriter::try_new(&mut out, schema.clone(), props).unwrap();
            let mut rg = w.start_row_group().unwrap();
            rg.write_next_column_arrays([arr_a.clone(), arr_b.clone()])
                .unwrap();
            rg.finish().unwrap();
            w.close().unwrap();
        }

        let actual = read_back(&out);
        assert_eq!(actual.num_rows(), total_rows);
        let counts_idx = actual.schema().index_of("counts").unwrap();
        let actual_list = actual
            .column(counts_idx)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .expect("counts must round-trip as ListArray");

        // Compare row by row: concatenate input arrays and compare to
        // the round-tripped output.
        let combined = arrow::compute::concat(&[arr_a.as_ref(), arr_b.as_ref()]).unwrap();
        let combined_list = combined
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        for row in 0..total_rows {
            let want = combined_list.value(row);
            let got = actual_list.value(row);
            let want_u64 = want
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .to_vec();
            let got_u64 = got
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .to_vec();
            assert_eq!(
                want_u64, got_u64,
                "row {row}: List<UInt64> round trip diverged",
            );
        }
    }

    /// PB-D edge: empty iterator produces a zero-row column. The whole
    /// row group becomes zero rows if every column is fed empty.
    #[test]
    fn test_array_stream_empty_iterator_is_zero_rows() {
        let batch = make_metrics_batch(0);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        let mut out: Vec<u8> = Vec::new();
        {
            let mut w =
                StreamingParquetWriter::try_new(&mut out, arrow_schema.clone(), props).unwrap();
            let mut rg = w.start_row_group().unwrap();
            for _ in 0..batch.num_columns() {
                rg.write_next_column_arrays(std::iter::empty()).unwrap();
            }
            rg.finish().unwrap();
            w.close().unwrap();
        }

        let reader = SerializedFileReader::new(bytes::Bytes::from(out)).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 1);
        assert_eq!(reader.metadata().row_group(0).num_rows(), 0);
    }

    /// PB-D edge: single-row arrays (one row per page) work end-to-end.
    /// Pathological page count exercise for the writer's accumulator.
    #[test]
    fn test_array_stream_single_row_per_array() {
        let batch = make_metrics_batch(16);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());
        let chunked = slice_columns_into_chunks(&batch, 16);

        let mut out: Vec<u8> = Vec::new();
        {
            let mut w =
                StreamingParquetWriter::try_new(&mut out, arrow_schema.clone(), props).unwrap();
            let mut rg = w.start_row_group().unwrap();
            for chunks in &chunked {
                rg.write_next_column_arrays(chunks.clone()).unwrap();
            }
            rg.finish().unwrap();
            w.close().unwrap();
        }

        let actual = read_back(&out);
        assert_eq!(actual.num_rows(), 16);
    }

    /// PB-A varied chunk sizes: input partition can be irregular
    /// (different array sizes per chunk) — each column's TOTAL row
    /// count is what matters, not chunk size symmetry.
    #[test]
    fn test_array_stream_varied_chunk_sizes_per_column() {
        let batch = make_metrics_batch(32);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        // Column 0: 4 chunks of 8 rows. Column 1: 1 chunk of 32 rows.
        // Column 2: 32 chunks of 1 row. All produce 32 total — should
        // succeed.
        let mut out: Vec<u8> = Vec::new();
        {
            let mut w =
                StreamingParquetWriter::try_new(&mut out, arrow_schema.clone(), props).unwrap();
            let mut rg = w.start_row_group().unwrap();
            // Column 0: 4 × 8
            let col0 = batch.column(0);
            let chunks0: Vec<ArrayRef> = (0..4).map(|i| col0.slice(i * 8, 8)).collect();
            rg.write_next_column_arrays(chunks0).unwrap();
            // Column 1: 1 × 32
            rg.write_next_column_arrays([batch.column(1).clone()])
                .unwrap();
            // Column 2: 32 × 1
            let col2 = batch.column(2);
            let chunks2: Vec<ArrayRef> = (0..32).map(|i| col2.slice(i, 1)).collect();
            rg.write_next_column_arrays(chunks2).unwrap();
            // Remaining columns: one piece each, mixing in the
            // existing single-array API for variety.
            for col_idx in 3..batch.num_columns() {
                rg.write_next_column(batch.column(col_idx)).unwrap();
            }
            rg.finish().unwrap();
            w.close().unwrap();
        }

        let actual = read_back(&out);
        assert_eq!(actual.num_rows(), 32);
    }

    /// PB-A row-count tracking: column-0 totals (via array stream) are
    /// remembered and column-N is rejected if its total disagrees.
    #[test]
    fn test_array_stream_row_count_mismatch_returns_error() {
        let batch = make_metrics_batch(32);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        let mut out: Vec<u8> = Vec::new();
        let mut w = StreamingParquetWriter::try_new(&mut out, arrow_schema.clone(), props).unwrap();
        let mut rg = w.start_row_group().unwrap();

        // Column 0: 32 rows.
        rg.write_next_column_arrays([batch.column(0).clone()])
            .unwrap();

        // Column 1: feed two arrays summing to 16 — wrong total.
        let arr_a = batch.column(1).slice(0, 8);
        let arr_b = batch.column(1).slice(8, 8);
        let err = rg.write_next_column_arrays([arr_a, arr_b]).unwrap_err();
        match err {
            ParquetWriteError::SchemaValidation(msg) => {
                assert!(msg.contains("row count mismatch"));
            }
            other => panic!("expected SchemaValidation, got {other:?}"),
        }
    }

    /// PB-A too many: writing past the last column via the array
    /// stream returns the same structured error as the single-array
    /// API.
    #[test]
    fn test_array_stream_too_many_columns_returns_error() {
        let batch = make_metrics_batch(8);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());

        let mut out: Vec<u8> = Vec::new();
        let mut w = StreamingParquetWriter::try_new(&mut out, arrow_schema.clone(), props).unwrap();
        let mut rg = w.start_row_group().unwrap();
        for col_idx in 0..batch.num_columns() {
            rg.write_next_column_arrays([batch.column(col_idx).clone()])
                .unwrap();
        }
        // One past the end.
        let err = rg
            .write_next_column_arrays([batch.column(0).clone()])
            .unwrap_err();
        match err {
            ParquetWriteError::SchemaValidation(_) => {}
            other => panic!("expected SchemaValidation, got {other:?}"),
        }
    }

    /// PB-C-1 (pending-writers cleanup): feeding a column as many
    /// small chunks must NOT cause `pending_writers_memory_size` to
    /// scale with the number of input chunks. After each
    /// `write_next_column_arrays` returns, the pre-allocated writer
    /// for that column has been popped and dropped, so the pending
    /// total is monotone non-increasing.
    ///
    /// Note: this is necessary but **not sufficient** for page-bounded
    /// memory — `pending_writers_memory_size` only sees the
    /// pre-allocated `ArrowColumnWriter`s for *future* columns, never
    /// the in-flight `SerializedColumnWriter` of the currently-writing
    /// column. The actual page-bounded contract is verified by
    /// `test_array_stream_pages_flush_incrementally`.
    #[test]
    fn test_array_stream_pending_writers_drained_per_column() {
        let batch = make_metrics_batch(2048);
        let arrow_schema = batch.schema();
        let props = writer_props_with_kv(&arrow_schema, Vec::new());
        let chunked = slice_columns_into_chunks(&batch, 64);

        let mut out: Vec<u8> = Vec::new();
        {
            let mut w =
                StreamingParquetWriter::try_new(&mut out, arrow_schema.clone(), props).unwrap();
            let mut rg = w.start_row_group().unwrap();
            let mut prior = rg.pending_writers_memory_size();
            for (col_idx, chunks) in chunked.iter().enumerate() {
                rg.write_next_column_arrays(chunks.clone()).unwrap();
                let now = rg.pending_writers_memory_size();
                assert!(
                    now <= prior,
                    "pending memory grew after writing column {} ({}): {} -> {}",
                    col_idx,
                    batch.schema().field(col_idx).name(),
                    prior,
                    now,
                );
                prior = now;
            }
            assert_eq!(rg.pending_writers_memory_size(), 0);
            rg.finish().unwrap();
            w.close().unwrap();
        }
    }

    /// PB-C-2 (pages flush through to sink during write): indirect
    /// evidence that the implementation uses `SerializedColumnWriter`
    /// (page-bounded) rather than `ArrowColumnWriter` (column-chunk-
    /// bounded). The strong claim — peak in-memory state stays at one
    /// page — follows from the choice of `next_column()` over the
    /// pre-allocated `ArrowColumnWriter`, which is a code-review
    /// observation. This test is a runtime sanity check.
    ///
    /// Setup: a single UInt64 column, 32 768 rows = 256 KiB
    /// uncompressed, paged at 4 096 rows (32 KiB per page). Each page
    /// is larger than the 8 KiB `BufWriter` that `TrackedWrite`
    /// interposes between `SerializedPageWriter` and the user-supplied
    /// sink, so each page-write call passes through directly:
    /// `SerializedColumnWriter` → 1 sink write per page (≈ 8 total).
    /// `ArrowColumnWriter`'s `io::copy` at column-close goes in 8 KiB
    /// chunks regardless, producing ≈ 32 writes for the same volume.
    /// Bounds below reject both extremes:
    ///
    /// - `>= expected_pages`: pages did flow through the sink during the column write;
    ///   `ArrowColumnWriter` would also pass this.
    /// - `<= 3 × expected_pages`: count is page-shaped, not chunk-copy-shaped.
    ///   `ArrowColumnWriter`'s path with `io::copy` would exceed this.
    ///
    /// If parquet 58 changes its internal `BufWriter` capacity or
    /// `io::copy`'s default chunk size, these bounds may need tuning.
    #[test]
    fn test_array_stream_pages_flush_incrementally() {
        use arrow::array::UInt64Array;
        use parquet::basic::Compression;

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            DataType::UInt64,
            false,
        )]));
        let values: Vec<u64> = (0..32_768u64).collect();
        let array: ArrayRef = Arc::new(UInt64Array::from(values));
        let arrays_input: Vec<ArrayRef> = (0..8).map(|i| array.slice(i * 4096, 4096)).collect();
        let expected_pages = arrays_input.len();

        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_data_page_row_count_limit(4096)
            .set_data_page_size_limit(64 * 1024)
            .set_write_batch_size(4096)
            .set_max_row_group_row_count(Some(usize::MAX))
            .build();

        let inner = Arc::new(Mutex::new(InspectableSinkInner::default()));
        let sink = SharedSink(Arc::clone(&inner));
        let mut w = StreamingParquetWriter::try_new(sink, Arc::clone(&schema), props).unwrap();
        let mut rg = w.start_row_group().unwrap();

        let writes_before_col0 = inner.lock().unwrap().num_writes;
        rg.write_next_column_arrays(arrays_input).unwrap();
        let writes_after_col0 = inner.lock().unwrap().num_writes;
        let writes_during_col0 = writes_after_col0 - writes_before_col0;

        assert!(
            writes_during_col0 >= expected_pages,
            "expected at least {expected_pages} sink writes during column-0 write, got \
             {writes_during_col0}; pages may not be flushing to sink during the column write",
        );
        assert!(
            writes_during_col0 <= expected_pages * 3,
            "got {writes_during_col0} sink writes for {expected_pages} pages; the count is larger \
             than expected, suggesting an io::copy-style chunk-by-chunk path (consistent with \
             column-chunk-buffered writes via append_column rather than per-page streaming)",
        );

        rg.finish().unwrap();
        w.close().unwrap();
    }

    /// Page-bounded contract for the `List<primitive>` path. Same
    /// shape as `test_array_stream_pages_flush_incrementally` but
    /// against `List<UInt64>` (the DDSketch `counts` shape). If the
    /// list path fell back to an `ArrowColumnWriter` (column-chunk-
    /// buffered), the sink would see one large `io::copy` burst at
    /// column close instead of per-page writes during the call.
    ///
    /// Setup: lists of length 1 so the inner-value count equals the
    /// outer-row count; that lets us reuse the same row-count page
    /// limit shape as the flat-primitive test (8 pages of 4096
    /// values each). `data_page_size_limit` is set well above the
    /// per-page byte cost (4096 values × 8 B = 32 KiB) so it doesn't
    /// interfere with the row-count-driven flushes we want to count.
    #[test]
    fn test_list_uint64_pages_flush_incrementally() {
        use arrow::array::ListBuilder;
        use parquet::basic::Compression;

        let item_field = Arc::new(Field::new("item", DataType::UInt64, false));
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "counts",
            DataType::List(Arc::clone(&item_field)),
            false,
        )]));

        // 32 768 rows, each with a 1-element list. Per-page values =
        // per-page rows = 4096; per-page size ≈ 32 KiB << 256 KiB
        // size limit, so row-count drives page flushing.
        let make_array = |start_row: usize, n_rows: usize| -> ArrayRef {
            let mut builder = ListBuilder::new(arrow::array::UInt64Builder::new())
                .with_field(Arc::clone(&item_field));
            for r in 0..n_rows {
                builder.values().append_value((start_row + r) as u64);
                builder.append(true);
            }
            Arc::new(builder.finish())
        };
        // 8 input arrays × 4096 rows each = 32 768 rows = 32 768 levels.
        let arrays_input: Vec<ArrayRef> = (0..8).map(|i| make_array(i * 4096, 4096)).collect();
        let total_levels: usize = 32_768;
        let expected_pages = total_levels.div_ceil(4096);

        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_data_page_row_count_limit(4096)
            .set_data_page_size_limit(256 * 1024)
            .set_write_batch_size(4096)
            .set_max_row_group_row_count(Some(usize::MAX))
            .build();

        let inner = Arc::new(Mutex::new(InspectableSinkInner::default()));
        let sink = SharedSink(Arc::clone(&inner));
        let mut w = StreamingParquetWriter::try_new(sink, Arc::clone(&schema), props).unwrap();
        let mut rg = w.start_row_group().unwrap();

        let writes_before = inner.lock().unwrap().num_writes;
        rg.write_next_column_arrays(arrays_input).unwrap();
        let writes_after = inner.lock().unwrap().num_writes;
        let writes_during = writes_after - writes_before;

        assert!(
            writes_during >= expected_pages,
            "expected at least {expected_pages} sink writes during list column write, got \
             {writes_during}; List<UInt64> pages may not be flushing during the column write \
             (suggests fallback to a column-chunk-buffered path)",
        );
        assert!(
            writes_during <= expected_pages * 3,
            "got {writes_during} sink writes for {expected_pages} expected pages on the list \
             column; count is too large for a per-page-streaming path (suggests a chunk-copy \
             fallback)",
        );

        rg.finish().unwrap();
        w.close().unwrap();
    }

    /// Mutex-shared sink that records every `write` call, used by the
    /// page-flush-incremental test. Wraps an `Arc<Mutex<...>>` so the
    /// test can read sink state during the writer's lifetime (the
    /// writer holds the wrapper; the test holds a clone of the Arc).
    struct SharedSink(Arc<Mutex<InspectableSinkInner>>);

    #[derive(Default)]
    struct InspectableSinkInner {
        bytes: Vec<u8>,
        num_writes: usize,
    }

    impl std::io::Write for SharedSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut g = self.0.lock().unwrap();
            g.bytes.extend_from_slice(buf);
            g.num_writes += 1;
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
