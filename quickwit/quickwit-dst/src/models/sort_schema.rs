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

//! Stateright model for Sort Schema invariants (ADR-002).
//!
//! Mirrors `docs/internals/specs/tla/SortSchema.tla`.
//!
//! # Invariants
//! - SS-1: All rows within a split are sorted according to the split's schema
//! - SS-2: Null values sort correctly per direction (nulls last asc, first desc)
//! - SS-3: Missing sort columns are treated as NULL
//! - SS-4: A split's sort schema never changes after write
//! - SS-5: Three copies of sort schema are identical per split

use std::collections::BTreeMap;

use stateright::*;

/// Column identifier (small domain for model checking).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Column {
    C1,
}

impl Column {
    pub const ALL: &[Column] = &[Column::C1];
}

/// Sort direction.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Direction {
    Asc,
    Desc,
}

/// A single sort column specification.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SortColumn {
    pub column: Column,
    pub direction: Direction,
}

/// A cell value, modeling TLA+ `ValuesWithNull`.
/// NULL is represented as `None`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Val(i8),
}

impl Value {
    /// Non-null values domain (matches TLA+ `Values == {1, 2, 3}`).
    pub const NON_NULL: &[Value] = &[Value::Val(1), Value::Val(2), Value::Val(3)];

    pub const ALL: &[Value] = &[Value::Null, Value::Val(1), Value::Val(2), Value::Val(3)];

    fn is_null(self) -> bool {
        matches!(self, Value::Null)
    }
}

/// A row: maps present columns to values.
/// Columns absent from the map are treated as NULL (SS-3).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Row {
    pub cells: BTreeMap<Column, Value>,
}

impl Row {
    fn get_value(&self, col: Column) -> Value {
        self.cells.get(&col).copied().unwrap_or(Value::Null)
    }
}

/// A split in object storage.
/// Mirrors the TLA+ split record with all three schema copies.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Split {
    pub id: u32,
    pub rows: Vec<Row>,
    pub sort_schema: Vec<SortColumn>,
    pub metadata_sort_schema: Vec<SortColumn>,
    pub kv_sort_schema: Vec<SortColumn>,
    pub sorting_columns_schema: Vec<SortColumn>,
    pub columns_present: Vec<Column>,
}

/// Sort schema model state. Mirrors TLA+ `VARIABLES`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SortSchemaState {
    pub metastore_schema: Vec<SortColumn>,
    pub splits: Vec<Split>,
    pub next_split_id: u32,
    pub schema_change_count: u32,
    pub split_schema_history: BTreeMap<u32, Vec<SortColumn>>,
}

/// Actions. Mirrors TLA+ `Next`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum SortSchemaAction {
    /// Ingest a batch with specific columns present and specific row data.
    IngestBatch {
        columns_present: Vec<Column>,
        rows: Vec<Row>,
    },
    /// Change the metastore schema to a new value.
    ChangeSchema { new_schema: Vec<SortColumn> },
    /// Compact two splits (identified by index in the splits vec).
    CompactSplits {
        s1_idx: usize,
        s2_idx: usize,
        merged_rows: Vec<Row>,
    },
}

/// Model configuration. Mirrors TLA+ `CONSTANTS`.
#[derive(Clone, Debug)]
pub struct SortSchemaModel {
    pub rows_per_split_max: usize,
    pub splits_max: usize,
    pub schema_changes_max: u32,
}

impl SortSchemaModel {
    /// Small model matching `SortSchema_small.cfg`.
    pub fn small() -> Self {
        SortSchemaModel {
            rows_per_split_max: 2,
            splits_max: 2,
            schema_changes_max: 1,
        }
    }
}

/// Compare two values with null ordering (SS-2).
/// Returns `Ordering`: Less, Equal, Greater.
/// Ascending: nulls sort AFTER non-null.
/// Descending: nulls sort BEFORE non-null.
///
/// Delegates to the shared [`crate::invariants::sort::compare_with_null_ordering`]
/// for the null ordering logic, converting model-specific `Value` to `Option`.
fn compare_values(v1: Value, v2: Value, direction: Direction) -> std::cmp::Ordering {
    let a = match v1 {
        Value::Null => None,
        Value::Val(v) => Some(v),
    };
    let b = match v2 {
        Value::Null => None,
        Value::Val(v) => Some(v),
    };
    let ascending = matches!(direction, Direction::Asc);
    crate::invariants::sort::compare_with_null_ordering(a.as_ref(), b.as_ref(), ascending)
}

/// Check if row1 <= row2 according to the schema (lexicographic).
/// Mirrors TLA+ `RowLEQ`.
fn row_leq(row1: &Row, row2: &Row, schema: &[SortColumn]) -> bool {
    for sc in schema {
        let v1 = row1.get_value(sc.column);
        let v2 = row2.get_value(sc.column);
        let cmp = compare_values(v1, v2, sc.direction);
        match cmp {
            std::cmp::Ordering::Less => return true,
            std::cmp::Ordering::Greater => return false,
            std::cmp::Ordering::Equal => continue,
        }
    }
    true // all columns equal
}

/// Check if rows are sorted. Mirrors TLA+ `IsSorted`.
fn is_sorted(rows: &[Row], schema: &[SortColumn]) -> bool {
    rows.windows(2).all(|w| row_leq(&w[0], &w[1], schema))
}

/// Generate all possible sort schemas (length 0, 1, or 2).
/// Mirrors TLA+ `AllSortSchemas`.
fn all_sort_schemas() -> Vec<Vec<SortColumn>> {
    let directions = [Direction::Asc, Direction::Desc];
    let mut schemas = vec![vec![]]; // empty schema

    // Length 1
    for &col in Column::ALL {
        for &dir in &directions {
            schemas.push(vec![SortColumn {
                column: col,
                direction: dir,
            }]);
        }
    }

    // Length 2
    for &c1 in Column::ALL {
        for &d1 in &directions {
            for &c2 in Column::ALL {
                for &d2 in &directions {
                    schemas.push(vec![
                        SortColumn {
                            column: c1,
                            direction: d1,
                        },
                        SortColumn {
                            column: c2,
                            direction: d2,
                        },
                    ]);
                }
            }
        }
    }

    schemas
}

/// Generate all possible rows for a given column set with n rows.
fn all_row_sequences(columns_present: &[Column], n: usize) -> Vec<Vec<Row>> {
    if n == 0 {
        return vec![vec![]];
    }

    // Each row maps each present column to a value (including null).
    let single_rows = all_single_rows(columns_present);

    // Generate all sequences of length n.
    let mut result = vec![vec![]];
    for _ in 0..n {
        let mut next = Vec::new();
        for prefix in &result {
            for row in &single_rows {
                let mut extended = prefix.clone();
                extended.push(row.clone());
                next.push(extended);
            }
        }
        result = next;
    }
    result
}

fn all_single_rows(columns_present: &[Column]) -> Vec<Row> {
    if columns_present.is_empty() {
        return vec![Row {
            cells: BTreeMap::new(),
        }];
    }

    // Generate all combinations of values for each column.
    let mut rows = vec![BTreeMap::new()];
    for &col in columns_present {
        let mut next = Vec::new();
        for partial in &rows {
            for &val in Value::ALL {
                let mut full = partial.clone();
                full.insert(col, val);
                next.push(full);
            }
        }
        rows = next;
    }
    rows.into_iter().map(|cells| Row { cells }).collect()
}

/// Generate all subsets of columns.
fn all_column_subsets() -> Vec<Vec<Column>> {
    let mut subsets = Vec::new();
    let cols = Column::ALL;
    // 2^n subsets
    for mask in 0..(1u32 << cols.len()) {
        let mut subset = Vec::new();
        for (i, &col) in cols.iter().enumerate() {
            if mask & (1 << i) != 0 {
                subset.push(col);
            }
        }
        subsets.push(subset);
    }
    subsets
}

/// Check if merged_rows is a valid permutation of the union of s1 and s2 rows,
/// accounting for column extension (missing columns become NULL).
fn is_valid_merge(merged_rows: &[Row], s1: &Split, s2: &Split, merged_columns: &[Column]) -> bool {
    let total_rows = s1.rows.len() + s2.rows.len();
    if merged_rows.len() != total_rows {
        return false;
    }

    // Each merged row must come from either s1 or s2 (extended with NULLs).
    // Build extended versions of input rows.
    let extend_row = |row: &Row, merged_cols: &[Column]| -> Row {
        let mut cells = BTreeMap::new();
        for &col in merged_cols {
            cells.insert(col, row.get_value(col));
        }
        Row { cells }
    };

    let s1_extended: Vec<Row> = s1
        .rows
        .iter()
        .map(|r| extend_row(r, merged_columns))
        .collect();
    let s2_extended: Vec<Row> = s2
        .rows
        .iter()
        .map(|r| extend_row(r, merged_columns))
        .collect();

    // Check that merged_rows is a permutation of s1_extended ++ s2_extended.
    let mut all_input: Vec<Row> = s1_extended;
    all_input.extend(s2_extended);
    all_input.sort();

    let mut sorted_merged = merged_rows.to_vec();
    sorted_merged.sort();

    sorted_merged == all_input
}

impl Model for SortSchemaModel {
    type State = SortSchemaState;
    type Action = SortSchemaAction;

    fn init_states(&self) -> Vec<Self::State> {
        // TLA+ Init: metastore_schema \in AllSortSchemas, splits = {}, etc.
        all_sort_schemas()
            .into_iter()
            .map(|schema| SortSchemaState {
                metastore_schema: schema,
                splits: Vec::new(),
                next_split_id: 1,
                schema_change_count: 0,
                split_schema_history: BTreeMap::new(),
            })
            .collect()
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // IngestBatch: if splits < SplitsMax
        if state.splits.len() < self.splits_max {
            let current_schema = &state.metastore_schema;

            for columns_present in all_column_subsets() {
                for n in 1..=self.rows_per_split_max {
                    for rows in all_row_sequences(&columns_present, n) {
                        // Only add if rows are sorted by current schema.
                        if is_sorted(&rows, current_schema) {
                            actions.push(SortSchemaAction::IngestBatch {
                                columns_present: columns_present.clone(),
                                rows,
                            });
                        }
                    }
                }
            }
        }

        // ChangeSchema: if schema_change_count < SchemaChangesMax
        if state.schema_change_count < self.schema_changes_max {
            for new_schema in all_sort_schemas() {
                if new_schema != state.metastore_schema {
                    actions.push(SortSchemaAction::ChangeSchema { new_schema });
                }
            }
        }

        // CompactSplits: merge two splits with same sort_schema
        for (i, s1) in state.splits.iter().enumerate() {
            for (j, s2) in state.splits.iter().enumerate() {
                if i >= j {
                    continue;
                }
                if s1.sort_schema != s2.sort_schema {
                    continue;
                }

                let total_rows = s1.rows.len() + s2.rows.len();
                if total_rows > self.rows_per_split_max {
                    continue;
                }

                let merged_schema = &s1.sort_schema;
                let mut merged_columns: Vec<Column> = s1.columns_present.clone();
                for &col in &s2.columns_present {
                    if !merged_columns.contains(&col) {
                        merged_columns.push(col);
                    }
                }
                merged_columns.sort();

                // Generate all valid merged row sequences.
                for merged_rows in all_row_sequences(&merged_columns, total_rows) {
                    if is_sorted(&merged_rows, merged_schema)
                        && is_valid_merge(&merged_rows, s1, s2, &merged_columns)
                    {
                        actions.push(SortSchemaAction::CompactSplits {
                            s1_idx: i,
                            s2_idx: j,
                            merged_rows,
                        });
                    }
                }
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();

        match action {
            SortSchemaAction::IngestBatch {
                columns_present,
                rows,
            } => {
                let new_id = next.next_split_id;
                let current_schema = next.metastore_schema.clone();
                let split = Split {
                    id: new_id,
                    rows,
                    sort_schema: current_schema.clone(),
                    metadata_sort_schema: current_schema.clone(),
                    kv_sort_schema: current_schema.clone(),
                    sorting_columns_schema: current_schema.clone(),
                    columns_present,
                };
                next.splits.push(split);
                next.splits.sort();
                next.next_split_id += 1;
                next.split_schema_history.insert(new_id, current_schema);
            }
            SortSchemaAction::ChangeSchema { new_schema } => {
                next.metastore_schema = new_schema;
                next.schema_change_count += 1;
            }
            SortSchemaAction::CompactSplits {
                s1_idx,
                s2_idx,
                merged_rows,
            } => {
                let s1 = &state.splits[s1_idx];
                let s2 = &state.splits[s2_idx];
                let merged_schema = s1.sort_schema.clone();
                let mut merged_columns: Vec<Column> = s1.columns_present.clone();
                for &col in &s2.columns_present {
                    if !merged_columns.contains(&col) {
                        merged_columns.push(col);
                    }
                }
                merged_columns.sort();

                let new_id = next.next_split_id;
                let new_split = Split {
                    id: new_id,
                    rows: merged_rows,
                    sort_schema: merged_schema.clone(),
                    metadata_sort_schema: merged_schema.clone(),
                    kv_sort_schema: merged_schema.clone(),
                    sorting_columns_schema: merged_schema.clone(),
                    columns_present: merged_columns,
                };

                // Remove old splits (higher index first to preserve indices).
                let (lo, hi) = if s1_idx < s2_idx {
                    (s1_idx, s2_idx)
                } else {
                    (s2_idx, s1_idx)
                };
                next.splits.remove(hi);
                next.splits.remove(lo);
                next.splits.push(new_split);
                next.splits.sort();
                next.next_split_id += 1;
                next.split_schema_history.insert(new_id, merged_schema);
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // SS-1: All rows within a split are sorted according to its schema.
            // Mirrors SortSchema.tla line 217-219
            Property::always(
                "SS-1: rows sorted",
                |_model: &SortSchemaModel, state: &SortSchemaState| {
                    state
                        .splits
                        .iter()
                        .all(|s| is_sorted(&s.rows, &s.sort_schema))
                },
            ),
            // SS-2: Null values ordered correctly per direction.
            // Ascending: null must NOT appear before non-null.
            // Descending: non-null must NOT appear before null.
            // Mirrors SortSchema.tla lines 228-247
            Property::always(
                "SS-2: null ordering",
                |_model: &SortSchemaModel, state: &SortSchemaState| {
                    for s in &state.splits {
                        for w in s.rows.windows(2) {
                            let (row_curr, row_next) = (&w[0], &w[1]);
                            for (k, sc) in s.sort_schema.iter().enumerate() {
                                let v_curr = row_curr.get_value(sc.column);
                                let v_next = row_next.get_value(sc.column);

                                // Check only when earlier columns are equal.
                                let earlier_equal = s.sort_schema[..k].iter().all(|prev_sc| {
                                    row_curr.get_value(prev_sc.column)
                                        == row_next.get_value(prev_sc.column)
                                });

                                if earlier_equal {
                                    // Ascending: null must not appear before non-null.
                                    if sc.direction == Direction::Asc
                                        && v_curr.is_null()
                                        && !v_next.is_null()
                                    {
                                        return false;
                                    }
                                    // Descending: non-null must not appear before null.
                                    if sc.direction == Direction::Desc
                                        && !v_curr.is_null()
                                        && v_next.is_null()
                                    {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                    true
                },
            ),
            // SS-3: Missing sort columns treated as NULL.
            // Mirrors SortSchema.tla lines 253-259
            Property::always(
                "SS-3: missing columns null",
                |_model: &SortSchemaModel, state: &SortSchemaState| {
                    for s in &state.splits {
                        for sc in &s.sort_schema {
                            if !s.columns_present.contains(&sc.column) {
                                for row in &s.rows {
                                    if row.get_value(sc.column) != Value::Null {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                    true
                },
            ),
            // SS-4: Schema immutable after write.
            // Mirrors SortSchema.tla lines 263-266
            Property::always(
                "SS-4: schema immutable",
                |_model: &SortSchemaModel, state: &SortSchemaState| {
                    for s in &state.splits {
                        if let Some(historical) = state.split_schema_history.get(&s.id)
                            && *historical != s.sort_schema
                        {
                            return false;
                        }
                    }
                    true
                },
            ),
            // SS-5: Three copies of sort schema are identical.
            // Mirrors SortSchema.tla lines 270-274
            Property::always(
                "SS-5: three-copy consistency",
                |_model: &SortSchemaModel, state: &SortSchemaState| {
                    state.splits.iter().all(|s| {
                        s.sort_schema == s.metadata_sort_schema
                            && s.sort_schema == s.kv_sort_schema
                            && s.sort_schema == s.sorting_columns_schema
                    })
                },
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_sort_schema_small() {
        let model = SortSchemaModel::small();
        model.checker().spawn_bfs().join().assert_properties();
    }

    #[test]
    fn compare_values_null_ordering() {
        // Ascending: null > non-null
        assert_eq!(
            compare_values(Value::Null, Value::Val(1), Direction::Asc),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            compare_values(Value::Val(1), Value::Null, Direction::Asc),
            std::cmp::Ordering::Less
        );

        // Descending: null < non-null
        assert_eq!(
            compare_values(Value::Null, Value::Val(1), Direction::Desc),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values(Value::Val(1), Value::Null, Direction::Desc),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn is_sorted_basic() {
        let schema = vec![SortColumn {
            column: Column::C1,
            direction: Direction::Asc,
        }];
        let rows = vec![
            Row {
                cells: [(Column::C1, Value::Val(1))].into(),
            },
            Row {
                cells: [(Column::C1, Value::Val(2))].into(),
            },
        ];
        assert!(is_sorted(&rows, &schema));

        let rows_unsorted = vec![
            Row {
                cells: [(Column::C1, Value::Val(2))].into(),
            },
            Row {
                cells: [(Column::C1, Value::Val(1))].into(),
            },
        ];
        assert!(!is_sorted(&rows_unsorted, &schema));
    }
}
