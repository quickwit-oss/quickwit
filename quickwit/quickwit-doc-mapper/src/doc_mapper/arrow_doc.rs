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

//! Builds tantivy documents straight from Arrow record batches.
//!
//! The JSON path turns each Parquet row into NDJSON text, parses it into a `serde_json::Map`, and
//! walks the mapping tree to build a `TantivyDocument`. On log data, those two conversions cost
//! more CPU than indexing the document. [`ArrowDocBuilder`] reads the Arrow columns directly and
//! produces the same document as [`DocMapper::doc_from_json_obj`] would for the JSON encoding of
//! the row (`arrow_json`, nulls omitted).
//!
//! It is deliberately conservative:
//! - [`ArrowDocBuilder::try_new`] returns `None` when the mapping or the Arrow schema uses a
//!   feature it does not reproduce exactly (nested objects, concatenate fields, source field,
//!   partition key, field presence, unsupported Arrow types, ...). The caller then uses the JSON
//!   path for the whole batch.
//! - [`ArrowDocBuilder::build_doc`] returns `None` for a row whose value would need a conversion it
//!   does not handle (e.g. an integer out of the target range). The caller then uses the JSON path
//!   for that row, which also produces the exact same error.
//!
//! Field values are added in the JSON path's order: mapped fields sorted by name, then the
//! dynamic object with keys sorted (`serde_json::Map` is a `BTreeMap`).

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, MapArray, RecordBatch};
use arrow_schema::{DataType, Schema as ArrowSchema, TimeUnit};
use quickwit_datetime::DateTimeInputFormat;
use tantivy::schema::Field;
use tantivy::schema::document::{ReferenceValue, ReferenceValueLeaf};
use tantivy::{DateTime, TantivyDocument};

use super::DocMapper;
use super::mapping_tree::{LeafType, MappingNode, MappingTree};
use crate::ModeType;

/// What the Arrow builder needs from a [`DocMapper`].
pub(crate) struct DocMapperView<'a> {
    pub root: &'a MappingNode,
    pub mode: ModeType,
    pub dynamic_field: Option<Field>,
    pub has_source_field: bool,
    pub index_field_presence: bool,
    pub has_document_size_field: bool,
    pub has_concatenate_dynamic_fields: bool,
    pub has_partition_key: bool,
    pub timestamp_field_name: Option<&'a str>,
}

/// Arrow scalar types handled by the builder, as they would appear once encoded as JSON.
#[derive(Clone, Copy, Debug)]
enum ScalarKind {
    Str,
    I64,
    U64,
    F64,
    Bool,
    /// Encoded by arrow_json as an RFC 3339 string. Only UTC or naive timestamps.
    Timestamp(TimeUnit),
}

#[derive(Clone, Copy, Debug)]
enum ColumnKind {
    Scalar(ScalarKind),
    /// `Map<Utf8, scalar>`, encoded as a JSON object.
    Map(ScalarKind),
}

#[derive(Clone, Copy, Debug)]
enum LeafKind {
    Text,
    I64,
    U64,
    F64,
    Bool,
    DateTime,
}

#[derive(Debug)]
enum Target {
    Leaf { field: Field, kind: LeafKind },
    Dynamic,
}

#[derive(Debug)]
struct ColumnPlan {
    column_idx: usize,
    name: String,
    kind: ColumnKind,
    target: Target,
}

/// Converts Arrow rows into tantivy documents for one Arrow schema. See the module docs.
#[derive(Debug)]
pub struct ArrowDocBuilder {
    // Mapped columns, sorted by name.
    leaf_columns: Vec<ColumnPlan>,
    // Unmapped columns going to the dynamic field, sorted by name.
    dynamic_columns: Vec<ColumnPlan>,
    dynamic_field: Option<Field>,
    // The dynamic field is stored, not indexed, not fast: its object is written straight in the
    // doc store encoding (`TantivyDocument::add_stored_only_value`).
    dynamic_is_stored_only: bool,
    // Every column (leaf and dynamic) in name order, as (is_dynamic, index in its Vec): the
    // order of the row object's entries.
    root_order: Vec<(bool, usize)>,
    // Lenient mode: unmapped columns dropped from documents, but present in the JSON object.
    has_ignored_columns: bool,
}

/// A row's entries, borrowed from the record batch. See [`ArrowDocBuilder::build_row`].
pub type RowArena<'a> = Vec<(&'a str, RowLeaf<'a>)>;

fn scalar_kind(data_type: &DataType) -> Option<ScalarKind> {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(ScalarKind::Str),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Some(ScalarKind::I64)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Some(ScalarKind::U64)
        }
        // Non-finite floats are rejected by the JSON path: they are checked per row.
        DataType::Float32 | DataType::Float64 => Some(ScalarKind::F64),
        DataType::Boolean => Some(ScalarKind::Bool),
        DataType::Timestamp(unit, tz) => match tz.as_deref() {
            None | Some("UTC") | Some("+00:00") | Some("Z") => Some(ScalarKind::Timestamp(*unit)),
            // Other zones would be encoded with their offset; same instant, but keep it exact.
            _ => None,
        },
        _ => None,
    }
}

fn column_kind(data_type: &DataType) -> Option<ColumnKind> {
    if let DataType::Map(entries_field, _sorted) = data_type {
        let DataType::Struct(fields) = entries_field.data_type() else {
            return None;
        };
        if fields.len() != 2 {
            return None;
        }
        if !matches!(
            fields[0].data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return None;
        }
        return scalar_kind(fields[1].data_type()).map(ColumnKind::Map);
    }
    scalar_kind(data_type).map(ColumnKind::Scalar)
}

/// The leaf kind, or `None` if a value of `scalar` could take a path not reproduced here.
fn leaf_kind(typ: &LeafType, scalar: ScalarKind) -> Option<LeafKind> {
    match (typ, scalar) {
        (LeafType::Text(_), ScalarKind::Str) => Some(LeafKind::Text),
        (LeafType::I64(_), ScalarKind::I64 | ScalarKind::U64) => Some(LeafKind::I64),
        (LeafType::U64(_), ScalarKind::I64 | ScalarKind::U64) => Some(LeafKind::U64),
        (LeafType::F64(_), ScalarKind::I64 | ScalarKind::U64 | ScalarKind::F64) => {
            Some(LeafKind::F64)
        }
        (LeafType::Bool(_), ScalarKind::Bool) => Some(LeafKind::Bool),
        (LeafType::DateTime(options), ScalarKind::Timestamp(_)) => {
            // The JSON path parses the RFC 3339 string with the first matching format. RFC 3339
            // and ISO 8601 parse it to the same instant; other formats (strptime, RFC 2822)
            // could differ or fail, so only accept those two (plus `unix_timestamp`, which
            // never matches a string).
            let formats = options.input_formats.formats();
            let all_compatible = formats.iter().all(|format| {
                matches!(
                    format,
                    DateTimeInputFormat::Rfc3339
                        | DateTimeInputFormat::Iso8601
                        | DateTimeInputFormat::Timestamp
                )
            });
            let has_string_format = formats.iter().any(|format| {
                matches!(
                    format,
                    DateTimeInputFormat::Rfc3339 | DateTimeInputFormat::Iso8601
                )
            });
            (all_compatible && has_string_format).then_some(LeafKind::DateTime)
        }
        // Coercions (string -> number, number -> text...), IPs, bytes, JSON leaves: JSON path.
        _ => None,
    }
}

impl ArrowDocBuilder {
    /// Plans the conversion of record batches with `arrow_schema`. Returns `None` if the JSON
    /// path must be used for this schema.
    pub fn try_new(doc_mapper: &DocMapper, arrow_schema: &ArrowSchema) -> Option<Self> {
        let view = doc_mapper.arrow_plan_inputs();
        if view.has_source_field
            || view.index_field_presence
            || view.has_document_size_field
            || view.has_concatenate_dynamic_fields
            || view.has_partition_key
        {
            return None;
        }
        let mut leaf_columns = Vec::new();
        let mut dynamic_columns = Vec::new();
        let mut timestamp_column_mapped = view.timestamp_field_name.is_none();
        let mut has_ignored_columns = false;
        for (column_idx, arrow_field) in arrow_schema.fields().iter().enumerate() {
            let name = arrow_field.name().clone();
            let kind = column_kind(arrow_field.data_type())?;
            let target = match view.root.branches.get(&name) {
                Some(MappingTree::Leaf(leaf)) => {
                    if leaf.has_concatenate() {
                        return None;
                    }
                    let ColumnKind::Scalar(scalar) = kind else {
                        return None;
                    };
                    let kind = leaf_kind(leaf.typ(), scalar)?;
                    if view.timestamp_field_name == Some(name.as_str()) {
                        timestamp_column_mapped = true;
                    }
                    Target::Leaf {
                        field: leaf.field(),
                        kind,
                    }
                }
                // Objects: JSON path.
                Some(MappingTree::Node(_)) => return None,
                None => match view.mode {
                    ModeType::Dynamic => {
                        view.dynamic_field?;
                        Target::Dynamic
                    }
                    ModeType::Lenient => {
                        has_ignored_columns = true;
                        continue;
                    }
                    // The JSON path reports the error.
                    ModeType::Strict => return None,
                },
            };
            let plan = ColumnPlan {
                column_idx,
                name,
                kind,
                target,
            };
            match plan.target {
                Target::Leaf { .. } => leaf_columns.push(plan),
                Target::Dynamic => dynamic_columns.push(plan),
            }
        }
        // A nested timestamp field (e.g. `a.ts`) is not reachable from top-level columns.
        let nested_timestamp_field =
            matches!(view.timestamp_field_name, Some(name) if name.contains('.'));
        if !timestamp_column_mapped && nested_timestamp_field {
            return None;
        }
        leaf_columns.sort_by(|left, right| left.name.cmp(&right.name));
        dynamic_columns.sort_by(|left, right| left.name.cmp(&right.name));
        let mut root_order: Vec<(bool, usize)> = (0..leaf_columns.len())
            .map(|idx| (false, idx))
            .chain((0..dynamic_columns.len()).map(|idx| (true, idx)))
            .collect();
        root_order.sort_by(|left, right| {
            let name = |(is_dynamic, idx): &(bool, usize)| {
                if *is_dynamic {
                    dynamic_columns[*idx].name.clone()
                } else {
                    leaf_columns[*idx].name.clone()
                }
            };
            name(left).cmp(&name(right))
        });
        let schema = doc_mapper.schema();
        let dynamic_is_stored_only = match view.dynamic_field {
            Some(field) => {
                let field_entry = schema.get_field_entry(field);
                field_entry.is_stored() && !field_entry.is_indexed() && !field_entry.is_fast()
            }
            None => false,
        };
        Some(ArrowDocBuilder {
            leaf_columns,
            dynamic_columns,
            dynamic_field: view.dynamic_field,
            dynamic_is_stored_only,
            root_order,
            has_ignored_columns,
        })
    }

    /// Whether [`Self::json_row`] covers every column of the row (false in lenient mode with
    /// unmapped columns, which only the JSON object sees).
    pub fn json_row_is_complete(&self) -> bool {
        !self.has_ignored_columns
    }

    /// Builds the document for `row` in one pass over its columns, or returns `None` if this row
    /// must go through the JSON path. `capacity_hint` sizes the document buffer.
    ///
    /// `arena` receives the row's entries (it is cleared first; reuse it across rows). With
    /// `with_row_object`, the returned range is the row object, as the JSON path would see it,
    /// for docs clustering fingerprints: see [`RowValue::new_object`]. Map entries are sorted
    /// once and shared by the document and the row object.
    pub fn build_row<'a>(
        &'a self,
        batch: &'a RecordBatch,
        row: usize,
        capacity_hint: usize,
        with_row_object: bool,
        arena: &mut RowArena<'a>,
    ) -> Option<(TantivyDocument, Option<(u32, u32)>)> {
        arena.clear();
        let mut doc = TantivyDocument::with_capacity(capacity_hint);
        for plan in &self.leaf_columns {
            let column = batch.column(plan.column_idx);
            if column.is_null(row) {
                continue;
            }
            let (Target::Leaf { field, kind }, ColumnKind::Scalar(scalar)) =
                (&plan.target, plan.kind)
            else {
                unreachable!("leaf columns are scalar leaves");
            };
            add_leaf(&mut doc, *field, *kind, scalar, column, row)?;
        }
        // Dynamic object entries first, then map entries after them.
        for plan in &self.dynamic_columns {
            if !batch.column(plan.column_idx).is_null(row) {
                // Placeholder, filled below once map entries are in the arena.
                arena.push((plan.name.as_str(), RowLeaf::Bool(false)));
            }
        }
        let num_dynamic_entries = arena.len();
        let mut dynamic_idx = 0;
        for plan in &self.dynamic_columns {
            let column = batch.column(plan.column_idx);
            if column.is_null(row) {
                continue;
            }
            let leaf = match plan.kind {
                ColumnKind::Scalar(scalar) => dynamic_leaf(scalar, column, row)?,
                ColumnKind::Map(scalar) => {
                    let (start, end) = push_map_entries(scalar, column.as_map(), row, arena)?;
                    RowLeaf::Object(start, end)
                }
            };
            arena[dynamic_idx].1 = leaf;
            dynamic_idx += 1;
        }
        if num_dynamic_entries > 0
            && let Some(dynamic_field) = self.dynamic_field
        {
            let dynamic_object = RowValue {
                leaf: RowLeaf::Object(0, num_dynamic_entries as u32),
                arena,
            };
            if self.dynamic_is_stored_only {
                doc.add_stored_only_value(dynamic_field, dynamic_object)
                    .ok()?;
            } else {
                doc.add_field_value(dynamic_field, dynamic_object);
            }
        }
        if !with_row_object {
            return Some((doc, None));
        }
        // The row object: every non-null column in name order. Dynamic entries are copied from
        // the dynamic object (map children are shared); both are in dynamic column order, so the
        // next dynamic entry is always the next one of the object.
        let root_start = arena.len();
        let mut next_dynamic_entry = 0;
        for &(is_dynamic, idx) in &self.root_order {
            if is_dynamic {
                let plan = &self.dynamic_columns[idx];
                if batch.column(plan.column_idx).is_null(row) {
                    continue;
                }
                let entry = arena[next_dynamic_entry];
                next_dynamic_entry += 1;
                arena.push(entry);
            } else {
                let plan = &self.leaf_columns[idx];
                let column = batch.column(plan.column_idx);
                if column.is_null(row) {
                    continue;
                }
                let ColumnKind::Scalar(scalar) = plan.kind else {
                    unreachable!("leaf columns are scalar leaves");
                };
                arena.push((plan.name.as_str(), dynamic_leaf(scalar, column, row)?));
            }
        }
        Some((doc, Some((root_start as u32, arena.len() as u32))))
    }

    /// Builds the document for `row`. See [`Self::build_row`].
    pub fn build_doc(
        &self,
        batch: &RecordBatch,
        row: usize,
        capacity_hint: usize,
    ) -> Option<TantivyDocument> {
        let mut arena = Vec::new();
        self.build_row(batch, row, capacity_hint, false, &mut arena)
            .map(|(doc, _)| doc)
    }

    /// The JSON object the JSON path would see for `row`, borrowed from the batch, for docs
    /// clustering fingerprints. `None` when the row must go through the JSON path. Root entries
    /// are sorted by name, like a `serde_json::Map`.
    ///
    /// Timestamps are [`RowLeaf::Timestamp`]: callers must not use it with a fingerprint policy
    /// that reads the value of a timestamp column (see [`Self::timestamp_column_names`]).
    pub fn json_row<'a>(&'a self, batch: &'a RecordBatch, row: usize) -> Option<JsonRow<'a>> {
        let mut arena = Vec::with_capacity(64);
        let (_, root_range) = self.build_row(batch, row, 0, true, &mut arena)?;
        Some(JsonRow {
            arena,
            root_range: root_range.expect("requested"),
        })
    }

    /// Names of timestamp columns, whose text the row object does not reproduce.
    pub fn timestamp_column_names(&self) -> impl Iterator<Item = &str> {
        self.leaf_columns
            .iter()
            .chain(&self.dynamic_columns)
            .filter(|plan| {
                matches!(
                    plan.kind,
                    ColumnKind::Scalar(ScalarKind::Timestamp(_))
                        | ColumnKind::Map(ScalarKind::Timestamp(_))
                )
            })
            .map(|plan| plan.name.as_str())
    }
}

/// A value borrowed from the record batch, as `serde_json` decodes its `arrow_json` encoding.
/// Objects point into a per-row arena of entries. Strings are raw: written to a document, the
/// ones tantivy would parse as RFC 3339 dates become dates (see [`RowValue`]).
#[derive(Clone, Copy, Debug)]
pub enum RowLeaf<'a> {
    /// A string.
    Str(&'a str),
    /// An integer that fits an `i64` (what `serde_json` tries first).
    I64(i64),
    /// An integer above `i64::MAX`.
    U64(u64),
    /// A finite float.
    F64(f64),
    /// A boolean.
    Bool(bool),
    /// A timestamp, in nanoseconds. `arrow_json` writes it as an RFC 3339 string: documents get
    /// the date it parses to, the text itself is not reproduced.
    Timestamp(i64),
    /// Entries `start..end` of the arena.
    Object(u32, u32),
}

/// A node of a row, borrowed from the record batch: the same value as `serde_json` would decode
/// from the row's `arrow_json` encoding (nulls omitted, map keys sorted and deduplicated), except
/// for timestamp text.
#[derive(Clone, Copy, Debug)]
pub struct RowValue<'a> {
    /// The node: a leaf or an object.
    pub leaf: RowLeaf<'a>,
    arena: &'a [(&'a str, RowLeaf<'a>)],
}

impl<'a> RowValue<'a> {
    /// The object `range` of `arena` (see [`ArrowDocBuilder::build_row`]).
    pub fn new_object(range: (u32, u32), arena: &'a [(&'a str, RowLeaf<'a>)]) -> Self {
        RowValue {
            leaf: RowLeaf::Object(range.0, range.1),
            arena,
        }
    }

    /// Entries of an object node, empty for other nodes.
    pub fn entries(&self) -> impl Iterator<Item = (&'a str, RowValue<'a>)> + 'a {
        let range = match self.leaf {
            RowLeaf::Object(start, end) => start as usize..end as usize,
            _ => 0..0,
        };
        RowObjectIter {
            entries: self.arena[range].iter(),
            arena: self.arena,
        }
    }

    /// The child for `key`, if this is an object.
    pub fn get(&self, key: &str) -> Option<RowValue<'a>> {
        self.entries()
            .filter(|(entry_key, _)| *entry_key == key)
            .last()
            .map(|(_, value)| value)
    }
}

/// The entries of one row, see [`ArrowDocBuilder::json_row`].
pub struct JsonRow<'a> {
    arena: RowArena<'a>,
    root_range: (u32, u32),
}

impl<'a> JsonRow<'a> {
    /// The row object.
    pub fn root(&'a self) -> RowValue<'a> {
        RowValue::new_object(self.root_range, &self.arena)
    }
}

/// Iterator over the entries of a [`RowValue`] object.
pub struct RowObjectIter<'a> {
    entries: std::slice::Iter<'a, (&'a str, RowLeaf<'a>)>,
    arena: &'a [(&'a str, RowLeaf<'a>)],
}

impl<'a> Iterator for RowObjectIter<'a> {
    type Item = (&'a str, RowValue<'a>);

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entries.size_hint()
    }

    fn next(&mut self) -> Option<Self::Item> {
        let (key, leaf) = self.entries.next()?;
        Some((
            key,
            RowValue {
                leaf: *leaf,
                arena: self.arena,
            },
        ))
    }
}

impl<'a> tantivy::schema::Value<'a> for RowValue<'a> {
    type ArrayIter = std::iter::Empty<RowValue<'a>>;
    type ObjectIter = RowObjectIter<'a>;

    fn as_value(&self) -> ReferenceValue<'a, Self> {
        match self.leaf {
            // tantivy's conversion of a JSON string.
            RowLeaf::Str(text) => match json_string_as_date(text) {
                Some(date_time) => ReferenceValueLeaf::Date(date_time).into(),
                None => ReferenceValueLeaf::Str(text).into(),
            },
            RowLeaf::I64(value) => ReferenceValueLeaf::I64(value).into(),
            RowLeaf::U64(value) => ReferenceValueLeaf::U64(value).into(),
            RowLeaf::F64(value) => ReferenceValueLeaf::F64(value).into(),
            RowLeaf::Bool(value) => ReferenceValueLeaf::Bool(value).into(),
            RowLeaf::Timestamp(nanos) => {
                ReferenceValueLeaf::Date(DateTime::from_timestamp_nanos(nanos)).into()
            }
            RowLeaf::Object(start, end) => ReferenceValue::Object(RowObjectIter {
                entries: self.arena[start as usize..end as usize].iter(),
                arena: self.arena,
            }),
        }
    }
}

/// tantivy's conversion of a JSON string (`&serde_json::Value`): strings starting with a digit
/// that parse as RFC 3339 become dates.
fn json_string_as_date(text: &str) -> Option<DateTime> {
    if !matches!(text.as_bytes().first(), Some(byte) if byte.is_ascii_digit()) {
        return None;
    }
    // Shortest RFC 3339 date-time: `YYYY-MM-DDTHH:MM:SSZ` (20 bytes), with `-` at 4. Skips the
    // parser for numbers-as-strings (`"0"`, `"200"`, ...), which never parse.
    let bytes = text.as_bytes();
    if bytes.len() < 20 || bytes[4] != b'-' {
        return None;
    }
    let date_time =
        time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).ok()?;
    Some(DateTime::from_utc(
        date_time.to_offset(time::UtcOffset::UTC),
    ))
}

/// A scalar as the dynamic field receives it from the JSON path (`&serde_json::Value`).
fn dynamic_leaf<'a>(scalar: ScalarKind, column: &'a ArrayRef, row: usize) -> Option<RowLeaf<'a>> {
    let leaf = match scalar {
        ScalarKind::Str => RowLeaf::Str(str_value(column, row)?),
        // serde_json numbers: integers are tried as i64 first.
        ScalarKind::I64 => RowLeaf::I64(i64_value(column, row)),
        ScalarKind::U64 => {
            let value = u64_value(column, row);
            match i64::try_from(value) {
                Ok(value) => RowLeaf::I64(value),
                Err(_) => RowLeaf::U64(value),
            }
        }
        ScalarKind::F64 => RowLeaf::F64(f64_value(column, row)?),
        ScalarKind::Bool => RowLeaf::Bool(column.as_boolean().value(row)),
        ScalarKind::Timestamp(unit) => RowLeaf::Timestamp(timestamp_nanos(column, unit, row)?),
    };
    Some(leaf)
}

/// Pushes the entries of a map value, as a JSON object: keys sorted, last duplicate wins, null
/// values omitted (`arrow_json` without explicit nulls). Returns the arena range.
fn push_map_entries<'a>(
    scalar: ScalarKind,
    map: &'a MapArray,
    row: usize,
    arena: &mut RowArena<'a>,
) -> Option<(u32, u32)> {
    let offsets = map.value_offsets();
    let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
    let (keys, values) = (map.keys(), map.values());
    let arena_start = arena.len();
    // (key, value index or None for null), then sort by key keeping the last occurrence.
    let mut has_duplicate = false;
    for idx in start..end {
        let key = str_value(keys, idx)?;
        let leaf = if values.is_null(idx) {
            None
        } else {
            Some(dynamic_leaf(scalar, values, idx)?)
        };
        // Mark nulls with an empty object range; filtered below.
        arena.push((key, leaf.unwrap_or(RowLeaf::Object(u32::MAX, u32::MAX))));
    }
    let entries = &mut arena[arena_start..];
    // Stable sort: duplicates keep their source order, the last one wins.
    entries.sort_by(|left, right| left.0.cmp(right.0));
    for pair in entries.windows(2) {
        if pair[0].0 == pair[1].0 {
            has_duplicate = true;
            break;
        }
    }
    if has_duplicate {
        let mut deduped: Vec<(&'a str, RowLeaf<'a>)> = Vec::with_capacity(entries.len());
        for entry in entries.iter() {
            match deduped.last_mut() {
                Some(last) if last.0 == entry.0 => {
                    // A null after a value with the same key drops it: JSON path.
                    if matches!(entry.1, RowLeaf::Object(u32::MAX, u32::MAX)) {
                        return None;
                    }
                    *last = *entry;
                }
                _ => deduped.push(*entry),
            }
        }
        arena.truncate(arena_start);
        arena.extend(deduped);
    }
    arena.retain_from(arena_start, |(_, leaf)| {
        !matches!(leaf, RowLeaf::Object(u32::MAX, u32::MAX))
    });
    Some((arena_start as u32, arena.len() as u32))
}

trait RetainFrom<T> {
    fn retain_from(&mut self, start: usize, keep: impl FnMut(&T) -> bool);
}

impl<T: Copy> RetainFrom<T> for Vec<T> {
    fn retain_from(&mut self, start: usize, mut keep: impl FnMut(&T) -> bool) {
        let mut write = start;
        for read in start..self.len() {
            if keep(&self[read]) {
                self[write] = self[read];
                write += 1;
            }
        }
        self.truncate(write);
    }
}

fn str_value(array: &ArrayRef, row: usize) -> Option<&str> {
    match array.data_type() {
        DataType::Utf8 => Some(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().value(row)),
        DataType::Utf8View => Some(array.as_string_view().value(row)),
        _ => None,
    }
}

fn i64_value(array: &ArrayRef, row: usize) -> i64 {
    match array.data_type() {
        DataType::Int8 => array.as_primitive::<Int8Type>().value(row) as i64,
        DataType::Int16 => array.as_primitive::<Int16Type>().value(row) as i64,
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row) as i64,
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row),
        _ => unreachable!("not a signed integer column"),
    }
}

fn u64_value(array: &ArrayRef, row: usize) -> u64 {
    match array.data_type() {
        DataType::UInt8 => array.as_primitive::<UInt8Type>().value(row) as u64,
        DataType::UInt16 => array.as_primitive::<UInt16Type>().value(row) as u64,
        DataType::UInt32 => array.as_primitive::<UInt32Type>().value(row) as u64,
        DataType::UInt64 => array.as_primitive::<UInt64Type>().value(row),
        _ => unreachable!("not an unsigned integer column"),
    }
}

/// `None` for non-finite values: the JSON path rejects them.
fn f64_value(array: &ArrayRef, row: usize) -> Option<f64> {
    let value = match array.data_type() {
        // arrow_json writes the shortest representation of the f32, which serde_json reads
        // back as the f64 closest to that decimal, not `value as f64`.
        DataType::Float32 => {
            let value = array.as_primitive::<Float32Type>().value(row);
            if !value.is_finite() {
                return None;
            }
            value.to_string().parse::<f64>().ok()?
        }
        DataType::Float64 => array.as_primitive::<Float64Type>().value(row),
        _ => unreachable!("not a float column"),
    };
    value.is_finite().then_some(value)
}

/// Timestamp as nanoseconds since the epoch, `None` if it does not fit in an `i64`.
fn timestamp_nanos(array: &ArrayRef, unit: TimeUnit, row: usize) -> Option<i64> {
    let nanos = match unit {
        TimeUnit::Second => array
            .as_primitive::<TimestampSecondType>()
            .value(row)
            .checked_mul(1_000_000_000)?,
        TimeUnit::Millisecond => array
            .as_primitive::<TimestampMillisecondType>()
            .value(row)
            .checked_mul(1_000_000)?,
        TimeUnit::Microsecond => array
            .as_primitive::<TimestampMicrosecondType>()
            .value(row)
            .checked_mul(1_000)?,
        TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
    };
    Some(nanos)
}

fn add_leaf(
    doc: &mut TantivyDocument,
    field: Field,
    kind: LeafKind,
    scalar: ScalarKind,
    column: &ArrayRef,
    row: usize,
) -> Option<()> {
    match (kind, scalar) {
        (LeafKind::Text, _) => doc.add_text(field, str_value(column, row)?),
        (LeafKind::I64, ScalarKind::I64) => doc.add_i64(field, i64_value(column, row)),
        (LeafKind::I64, ScalarKind::U64) => {
            doc.add_i64(field, i64::try_from(u64_value(column, row)).ok()?)
        }
        (LeafKind::U64, ScalarKind::U64) => doc.add_u64(field, u64_value(column, row)),
        (LeafKind::U64, ScalarKind::I64) => {
            doc.add_u64(field, u64::try_from(i64_value(column, row)).ok()?)
        }
        // serde_json's `as_f64` on an integer number.
        (LeafKind::F64, ScalarKind::I64) => doc.add_f64(field, i64_value(column, row) as f64),
        (LeafKind::F64, ScalarKind::U64) => doc.add_f64(field, u64_value(column, row) as f64),
        (LeafKind::F64, ScalarKind::F64) => doc.add_f64(field, f64_value(column, row)?),
        (LeafKind::Bool, _) => doc.add_bool(field, column.as_boolean().value(row)),
        (LeafKind::DateTime, ScalarKind::Timestamp(unit)) => doc.add_date(
            field,
            DateTime::from_timestamp_nanos(timestamp_nanos(column, unit, row)?),
        ),
        _ => return None,
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{MapBuilder, StringBuilder};
    use arrow_array::{
        ArrayRef, BooleanArray, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt8Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};
    use tantivy::TantivyDocument;
    use tantivy::schema::document::Document as _;

    use super::ArrowDocBuilder;
    use crate::DocMapper;

    fn doc_mapper(json: serde_json::Value) -> DocMapper {
        serde_json::from_value(json).unwrap()
    }

    /// The searchbench OTel mapping: dynamic mode, stored-only dynamic field.
    fn otel_doc_mapper() -> DocMapper {
        doc_mapper(serde_json::json!({
            "mode": "dynamic",
            "dynamic_mapping": {"indexed": false, "stored": true, "expand_dots": true},
            "field_mappings": [
                {"name": "Timestamp", "type": "datetime", "input_formats": ["unix_timestamp", "rfc3339"],
                 "fast_precision": "milliseconds", "fast": true, "indexed": true},
                {"name": "Body", "type": "text", "tokenizer": "default", "record": "position"},
                {"name": "ServiceName", "type": "text", "tokenizer": "raw", "fast": true},
                {"name": "SeverityNumber", "type": "i64", "fast": true},
                {"name": "Ratio", "type": "f64"},
                {"name": "Count", "type": "u64"},
                {"name": "Flag", "type": "bool"}
            ],
            "timestamp_field": "Timestamp"
        }))
    }

    fn attributes(rows: &[&[(&str, Option<&str>)]], nulls: &[usize]) -> ArrayRef {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for (row_idx, row) in rows.iter().enumerate() {
            for (key, value) in row.iter() {
                builder.keys().append_value(key);
                match value {
                    Some(value) => builder.values().append_value(value),
                    None => builder.values().append_null(),
                }
            }
            builder.append(!nulls.contains(&row_idx)).unwrap();
        }
        Arc::new(builder.finish())
    }

    fn otel_batch() -> RecordBatch {
        let num_rows = 5;
        let ts_nanos = vec![
            Some(1_758_585_602_150_167_919i64),
            Some(1_758_585_602_150_000_000),
            Some(0),
            Some(1_758_585_602_999_999_999),
            Some(-1_000_000_001),
        ];
        let columns: Vec<(&str, ArrayRef)> = vec![
            (
                "Timestamp",
                Arc::new(TimestampNanosecondArray::from(ts_nanos).with_timezone("UTC")),
            ),
            (
                "TimestampTime",
                Arc::new(
                    TimestampMillisecondArray::from(vec![
                        Some(1_758_585_602_150i64),
                        None,
                        Some(1),
                        Some(2),
                        Some(3),
                    ])
                    .with_timezone("UTC"),
                ),
            ),
            (
                "Body",
                Arc::new(StringArray::from(vec![
                    Some("failed to place order: connection refused"),
                    Some("GetCartAsync called with userId=9670e906"),
                    None,
                    Some(""),
                    Some("2025-09-21T07:47:47Z"),
                ])),
            ),
            (
                "ServiceName",
                Arc::new(StringArray::from(vec![
                    Some("cart"),
                    Some("frontend"),
                    Some("cart"),
                    None,
                    Some("checkout"),
                ])),
            ),
            (
                "SeverityNumber",
                Arc::new(UInt8Array::from(vec![
                    Some(9),
                    Some(17),
                    None,
                    Some(0),
                    Some(255),
                ])),
            ),
            (
                "Ratio",
                Arc::new(Float32Array::from(vec![
                    Some(0.1f32),
                    Some(1.5),
                    None,
                    Some(-3.25),
                    Some(1e-7),
                ])),
            ),
            (
                "Count",
                Arc::new(Int64Array::from(vec![
                    Some(3),
                    Some(0),
                    None,
                    Some(7),
                    Some(1 << 40),
                ])),
            ),
            (
                "Flag",
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    None,
                    Some(true),
                    None,
                ])),
            ),
            (
                "TraceFlags",
                Arc::new(UInt64Array::from(vec![
                    Some(0),
                    Some(1),
                    Some(u64::MAX),
                    None,
                    Some(1 << 63),
                ])),
            ),
            (
                "Score",
                Arc::new(Float64Array::from(vec![
                    Some(0.5),
                    Some(-0.0),
                    None,
                    Some(1e300),
                    Some(2.0),
                ])),
            ),
            (
                "ScopeName",
                Arc::new(StringArray::from(vec![
                    Some(""),
                    Some("otel"),
                    None,
                    Some("x"),
                    Some("y"),
                ])),
            ),
            (
                "ResourceAttributes",
                attributes(
                    &[
                        &[
                            ("service.name", Some("cart")),
                            ("k8s.pod.start_time", Some("2025-09-21T07:47:47Z")),
                            ("app", Some("app")),
                        ],
                        &[],
                        &[("dup", Some("first")), ("dup", Some("second"))],
                        &[("nullable", None), ("dup", Some("x")), ("dup", None)],
                        &[
                            ("z", Some("1")),
                            ("a", Some("2")),
                            ("n", None),
                            ("n", Some("3")),
                        ],
                    ],
                    &[1],
                ),
            ),
        ];
        let schema = ArrowSchema::new(
            columns
                .iter()
                .map(|(name, array)| ArrowField::new(*name, array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        );
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            columns.into_iter().map(|(_, array)| array).collect(),
        )
        .unwrap();
        assert_eq!(batch.num_rows(), num_rows);
        batch
    }

    /// The JSON path: `arrow_json` with nulls omitted, then `doc_from_json_bytes`.
    fn json_docs(doc_mapper: &DocMapper, batch: &RecordBatch) -> Vec<Option<TantivyDocument>> {
        let mut writer = arrow_json::WriterBuilder::new()
            .with_explicit_nulls(false)
            .build::<_, arrow_json::writer::LineDelimited>(Vec::new());
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        let ndjson = writer.into_inner();
        ndjson
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| {
                doc_mapper
                    .doc_from_json_bytes(line)
                    .ok()
                    .map(|(_, doc)| doc)
            })
            .collect()
    }

    /// The doc store encoding of the document: what search returns.
    fn stored_bytes(doc: &TantivyDocument, doc_mapper: &DocMapper) -> Vec<u8> {
        let mut bytes = Vec::new();
        doc.serialize_stored_fields(&doc_mapper.schema(), &mut bytes)
            .unwrap()
            .unwrap();
        bytes
    }

    /// The values the inverted index and the fast fields see.
    fn indexed_values(doc: &TantivyDocument, doc_mapper: &DocMapper) -> Vec<(String, String)> {
        let schema = doc_mapper.schema();
        doc.iter_fields_and_values()
            .filter(|(field, _)| {
                let entry = schema.get_field_entry(*field);
                entry.is_indexed() || entry.is_fast()
            })
            .map(|(field, value)| {
                let value: tantivy::schema::OwnedValue = value.into();
                (
                    schema.get_field_name(field).to_string(),
                    format!("{value:?}"),
                )
            })
            .collect()
    }

    /// Arrow documents equal JSON documents (same stored bytes, same indexed values), for every
    /// row the builder converts. Returns the rows it converts.
    fn check_arrow_docs_match_json_docs(doc_mapper: &DocMapper, batch: &RecordBatch) -> Vec<usize> {
        let builder = ArrowDocBuilder::try_new(doc_mapper, &batch.schema()).unwrap();
        let expected = json_docs(doc_mapper, batch);
        let mut native_rows = Vec::new();
        let mut arena = Vec::new();
        for (row, expected_doc) in expected.iter().enumerate() {
            // The builder may hand a row to the JSON path, never produce a different doc.
            let Some((actual_doc, _)) = builder.build_row(batch, row, 256, false, &mut arena)
            else {
                continue;
            };
            let expected_doc = expected_doc
                .as_ref()
                .expect("the JSON path accepts the row");
            assert_eq!(
                stored_bytes(&actual_doc, doc_mapper),
                stored_bytes(expected_doc, doc_mapper),
                "stored bytes, row {row}"
            );
            assert_eq!(
                indexed_values(&actual_doc, doc_mapper),
                indexed_values(expected_doc, doc_mapper),
                "indexed values, row {row}"
            );
            native_rows.push(row);
        }
        native_rows
    }

    #[test]
    fn test_arrow_docs_match_json_docs() {
        let doc_mapper = otel_doc_mapper();
        let batch = otel_batch();
        // Row 3 has a null after a non-null duplicate key and is handed to the JSON path.
        assert_eq!(
            check_arrow_docs_match_json_docs(&doc_mapper, &batch),
            vec![0, 1, 2, 4]
        );
        // The stored-only dynamic object is not visible to the indexers.
        let builder = ArrowDocBuilder::try_new(&doc_mapper, &batch.schema()).unwrap();
        let doc = builder.build_doc(&batch, 0, 256).unwrap();
        let schema = doc_mapper.schema();
        assert!(
            doc.iter_fields_and_values()
                .all(|(field, _)| schema.get_field_name(field) != "_dynamic")
        );
    }

    #[test]
    fn test_arrow_docs_with_string_views_match_json_docs() {
        // The Parquet source can decode strings as `Utf8View` (and map keys/values too).
        let doc_mapper = otel_doc_mapper();
        let batch = otel_batch();
        let view_columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|column| to_string_views(column))
            .collect();
        let view_fields: Vec<ArrowField> = batch
            .schema()
            .fields()
            .iter()
            .zip(&view_columns)
            .map(|(field, column)| ArrowField::new(field.name(), column.data_type().clone(), true))
            .collect();
        let view_batch =
            RecordBatch::try_new(Arc::new(ArrowSchema::new(view_fields)), view_columns).unwrap();
        assert!(
            view_batch
                .schema()
                .fields()
                .iter()
                .any(|field| field.data_type() == &DataType::Utf8View)
        );
        let builder = ArrowDocBuilder::try_new(&doc_mapper, &view_batch.schema()).unwrap();
        let utf8_builder = ArrowDocBuilder::try_new(&doc_mapper, &batch.schema()).unwrap();
        let mut arena = Vec::new();
        let mut utf8_arena = Vec::new();
        for row in 0..batch.num_rows() {
            let view_doc = builder.build_row(&view_batch, row, 0, false, &mut arena);
            let utf8_doc = utf8_builder.build_row(&batch, row, 0, false, &mut utf8_arena);
            assert_eq!(view_doc.is_some(), utf8_doc.is_some(), "row {row}");
            if let (Some((view_doc, _)), Some((utf8_doc, _))) = (view_doc, utf8_doc) {
                assert_eq!(
                    stored_bytes(&view_doc, &doc_mapper),
                    stored_bytes(&utf8_doc, &doc_mapper),
                    "row {row}"
                );
                assert_eq!(
                    indexed_values(&view_doc, &doc_mapper),
                    indexed_values(&utf8_doc, &doc_mapper),
                    "row {row}"
                );
            }
        }
    }

    /// Casts `Utf8` to `Utf8View`, including map keys and values.
    fn to_string_views(column: &ArrayRef) -> ArrayRef {
        use arrow_array::{Array, MapArray, StringViewArray, StructArray};
        match column.data_type() {
            DataType::Utf8 => Arc::new(StringViewArray::from_iter(
                column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter(),
            )),
            DataType::Map(_, sorted) => {
                let map = column.as_any().downcast_ref::<MapArray>().unwrap();
                let entries = map.entries();
                let keys = to_string_views(entries.column(0));
                let values = to_string_views(entries.column(1));
                let fields = vec![
                    ArrowField::new("key", keys.data_type().clone(), false),
                    ArrowField::new("value", values.data_type().clone(), true),
                ];
                let entries = StructArray::new(fields.clone().into(), vec![keys, values], None);
                let entries_field = Arc::new(ArrowField::new(
                    "entries",
                    DataType::Struct(fields.into()),
                    false,
                ));
                Arc::new(
                    MapArray::try_new(
                        entries_field,
                        map.offsets().clone(),
                        entries,
                        map.nulls().cloned(),
                        *sorted,
                    )
                    .unwrap(),
                )
            }
            _ => column.clone(),
        }
    }

    #[test]
    fn test_arrow_docs_match_json_docs_with_indexed_dynamic_field() {
        // Default dynamic mapping: indexed and fast, so the object goes through the document.
        let doc_mapper = doc_mapper(serde_json::json!({
            "mode": "dynamic",
            "field_mappings": [
                {"name": "Timestamp", "type": "datetime", "input_formats": ["unix_timestamp", "rfc3339"],
                 "fast_precision": "milliseconds", "fast": true, "indexed": true},
                {"name": "Body", "type": "text", "tokenizer": "default", "record": "position"},
                {"name": "ServiceName", "type": "text", "tokenizer": "raw", "fast": true}
            ],
            "timestamp_field": "Timestamp"
        }));
        let batch = otel_batch();
        assert_eq!(
            check_arrow_docs_match_json_docs(&doc_mapper, &batch),
            vec![0, 1, 2, 4]
        );
    }

    /// Converts a borrowed row to `serde_json`, mapping timestamp text to "".
    fn row_to_json(value: super::RowValue<'_>) -> serde_json::Value {
        use super::RowLeaf;
        match value.leaf {
            RowLeaf::Str(text) => text.into(),
            RowLeaf::I64(number) => number.into(),
            RowLeaf::U64(number) => number.into(),
            RowLeaf::F64(number) => serde_json::Number::from_f64(number).unwrap().into(),
            RowLeaf::Bool(value) => value.into(),
            RowLeaf::Timestamp(_) => "".into(),
            RowLeaf::Object(..) => serde_json::Value::Object(
                value
                    .entries()
                    .map(|(key, child)| (key.to_string(), row_to_json(child)))
                    .collect(),
            ),
        }
    }

    #[test]
    fn test_json_row_matches_arrow_json() {
        let doc_mapper = otel_doc_mapper();
        let batch = otel_batch();
        let builder = ArrowDocBuilder::try_new(&doc_mapper, &batch.schema()).unwrap();
        assert!(builder.json_row_is_complete());
        let mut writer = arrow_json::WriterBuilder::new()
            .with_explicit_nulls(false)
            .build::<_, arrow_json::writer::LineDelimited>(Vec::new());
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let ndjson = writer.into_inner();
        let timestamp_columns: Vec<&str> = builder.timestamp_column_names().collect();
        assert_eq!(timestamp_columns, ["Timestamp", "TimestampTime"]);
        let mut num_checked = 0;
        for (row, line) in ndjson
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .enumerate()
        {
            let Some(json_row) = builder.json_row(&batch, row) else {
                continue;
            };
            let mut expected: serde_json::Map<String, serde_json::Value> =
                serde_json::from_slice(line).unwrap();
            for column in &timestamp_columns {
                if let Some(value) = expected.get_mut(*column) {
                    *value = serde_json::Value::String(String::new());
                }
            }
            // Same entries in the same order (both sorted by key).
            let actual = row_to_json(json_row.root());
            assert_eq!(
                serde_json::to_string(&actual).unwrap(),
                serde_json::to_string(&serde_json::Value::Object(expected)).unwrap(),
                "row {row}"
            );
            num_checked += 1;
        }
        assert_eq!(num_checked, 4);
    }

    #[test]
    fn test_json_string_as_date_matches_tantivy() {
        use tantivy::schema::OwnedValue;
        for text in [
            "0",
            "200",
            "2025",
            "2025-09-21",
            "2025-09-21T07:47:47Z",
            "2025-09-21T07:47:47.123+02:00",
            "2025-09-21t07:47:47z",
            "2025-09-21 07:47:47Z",
            "1999-12-31T23:59:60Z",
            "9999-99-99T99:99:99Z",
            "12345678901234567890",
            "2025-09-21T07:47:47",
            "0000-01-01T00:00:00Z",
            "abc",
        ] {
            let tantivy_date = OwnedValue::from(serde_json::Value::String(text.to_string()));
            let expected = match tantivy_date {
                OwnedValue::Date(date_time) => Some(date_time),
                _ => None,
            };
            assert_eq!(super::json_string_as_date(text), expected, "{text}");
        }
    }

    #[test]
    fn test_arrow_out_of_range_integer_falls_back_to_json_path() {
        let doc_mapper = doc_mapper(serde_json::json!({
            "mode": "lenient",
            "field_mappings": [{"name": "n", "type": "i64"}]
        }));
        let batch = RecordBatch::try_from_iter(vec![(
            "n",
            Arc::new(UInt64Array::from(vec![1, u64::MAX])) as ArrayRef,
        )])
        .unwrap();
        let builder = ArrowDocBuilder::try_new(&doc_mapper, &batch.schema()).unwrap();
        assert!(builder.build_doc(&batch, 0, 0).is_some());
        assert!(builder.build_doc(&batch, 1, 0).is_none());
        // And the JSON path rejects it.
        assert!(
            doc_mapper
                .doc_from_json_str(r#"{"n": 18446744073709551615}"#)
                .is_err()
        );
    }

    #[test]
    fn test_arrow_declines_unsupported_mappings_and_types() {
        let batch_with =
            |data_type: DataType| ArrowSchema::new(vec![ArrowField::new("a", data_type, true)]);
        // Strict mode with an unknown column: the JSON path reports the error.
        let strict = doc_mapper(serde_json::json!({"mode": "strict", "field_mappings": []}));
        assert!(ArrowDocBuilder::try_new(&strict, &batch_with(DataType::Utf8)).is_none());
        // Coercion string -> number is left to the JSON path.
        let numeric = doc_mapper(serde_json::json!({
            "field_mappings": [{"name": "a", "type": "i64"}]
        }));
        assert!(ArrowDocBuilder::try_new(&numeric, &batch_with(DataType::Utf8)).is_none());
        // Nested types.
        let dynamic = doc_mapper(serde_json::json!({"mode": "dynamic", "field_mappings": []}));
        let list = DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true)));
        assert!(ArrowDocBuilder::try_new(&dynamic, &batch_with(list)).is_none());
        assert!(ArrowDocBuilder::try_new(&dynamic, &batch_with(DataType::Binary)).is_none());
        // Non-UTC zones.
        let zoned = DataType::Timestamp(TimeUnit::Second, Some("+02:00".into()));
        assert!(ArrowDocBuilder::try_new(&dynamic, &batch_with(zoned)).is_none());
        // Partition keys, concatenate fields, field presence.
        let partitioned = doc_mapper(serde_json::json!({
            "mode": "dynamic", "field_mappings": [], "partition_key": "a"
        }));
        assert!(ArrowDocBuilder::try_new(&partitioned, &batch_with(DataType::Utf8)).is_none());
        let presence = doc_mapper(serde_json::json!({
            "mode": "dynamic", "field_mappings": [], "index_field_presence": true
        }));
        assert!(ArrowDocBuilder::try_new(&presence, &batch_with(DataType::Utf8)).is_none());
        // The default dynamic mapping is supported.
        assert!(ArrowDocBuilder::try_new(&dynamic, &batch_with(DataType::Utf8)).is_some());
    }
}
