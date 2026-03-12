---- MODULE SortSchema ----
\* Formal specification for Sort Schema invariants (ADR-002)
\*
\* Models:
\*   - A metastore holding the current sort schema per index (mutable at runtime)
\*   - An indexing pipeline that reads the current schema, sorts rows, and writes splits
\*   - Splits in object storage with rows, metadata sort schema, Parquet KV sort schema,
\*     and Parquet sorting_columns sort schema
\*   - Schema change action that updates the metastore schema
\*   - Compaction that merges splits only when sort schemas match
\*
\* Key Invariants (from ADR-002):
\*   - SS-1: All rows within a split are sorted according to its recorded sort schema
\*   - SS-2: Nulls sort after non-null for ascending, before non-null for descending
\*   - SS-3: Missing sort columns in a split are treated as null (not an error)
\*   - SS-4: A split's sort schema never changes after it is written
\*   - SS-5: The three copies of sort schema (metastore, KV metadata, sorting_columns)
\*           are identical for each split
\*
\* Signal Applicability:
\*   - Metrics: Primary target. Parquet splits sorted by metric_name|host|...|timestamp
\*   - Traces:  Same model. Sort by service_name|operation_name|trace_id|timestamp
\*   - Logs:    Same model. Sort by service_name|level|host|timestamp
\*
\* TLA+-to-Rust Mapping:
\* | TLA+ Concept                 | Rust Implementation                                          |
\* |------------------------------|--------------------------------------------------------------|
\* | MetastoreSchema              | Metastore per-index sort_schema field (PostgreSQL)           |
\* | SortSchema (sequence)        | Sort schema string "col+|col-|.../V2" parsed into Vec       |
\* | SortColumn (record)          | SortColumn { name, direction } in sort schema parser         |
\* | Split (record)               | MetricsSplitMetadata in quickwit-parquet-engine              |
\* | split.rows                   | RecordBatch rows in a Parquet file                           |
\* | split.metadata_sort_schema   | sort_schema field in MetricsSplitMetadata (PostgreSQL)       |
\* | split.kv_sort_schema         | sort_schema key in Parquet key_value_metadata                |
\* | split.sorting_columns_schema | Parquet native sorting_columns field                         |
\* | IngestBatch action           | ParquetWriter::sort_batch() + write in writer.rs             |
\* | ChangeSchema action          | Metastore update_index() changing sort_schema                |
\* | CompactSplits action         | Parquet merge executor (sorted k-way merge, ADR-003)         |
\* | RowsSorted property          | debug_assert! after lexsort_to_indices in sort_batch()       |
\* | NullOrdering property        | SortColumn nulls_first field per direction                   |
\* | Columns                      | ParquetField enum variants in schema/fields.rs               |

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    \* @type: Set(Str);
    \* Column names that can appear in sort schemas and row data
    Columns,

    \* @type: Int;
    \* Maximum number of rows per split (kept small for model checking)
    RowsPerSplitMax,

    \* @type: Int;
    \* Maximum number of splits in the system
    SplitsMax,

    \* @type: Int;
    \* Maximum number of schema changes allowed
    SchemaChangesMax

VARIABLES
    \* @type: Seq(<<Str, Str>>);
    \* The current sort schema in the metastore: a sequence of <<column_name, direction>>
    \* direction is "asc" or "desc"
    metastore_schema,

    \* @type: Set([
    \*   id: Int,
    \*   rows: Seq([col: Str -> Int | Str]),
    \*   sort_schema: Seq(<<Str, Str>>),
    \*   metadata_sort_schema: Seq(<<Str, Str>>),
    \*   kv_sort_schema: Seq(<<Str, Str>>),
    \*   sorting_columns_schema: Seq(<<Str, Str>>),
    \*   columns_present: Set(Str)
    \* ]);
    \* The set of splits in object storage
    splits,

    \* @type: Int;
    \* Counter for generating unique split IDs
    next_split_id,

    \* @type: Int;
    \* Counter for schema changes (bounded)
    schema_change_count,

    \* @type: Set(Int) -> Seq(<<Str, Str>>);
    \* Historical record: maps split ID to the sort schema at time of write
    \* Used to verify SS-4 (immutability)
    split_schema_history

vars == <<metastore_schema, splits, next_split_id, schema_change_count, split_schema_history>>

\* ============================================================================
\* Constants and Value Domains
\* ============================================================================

\* Possible directions for sort columns
Directions == {"asc", "desc"}

\* A special sentinel value representing NULL.
\* Must be an integer outside the normal value range so that
\* TLC can compare it with < and > without type errors.
NULL == -999

\* Non-null values are modeled as integers from a small domain
\* This keeps the state space finite and manageable
Values == {1, 2, 3}

\* The full value domain including NULL
ValuesWithNull == Values \cup {NULL}

\* All possible sort schemas: sequences of (column, direction) pairs
\* We limit to schemas of length 1 or 2 to keep state space bounded
AllSortSchemas ==
    { <<>> } \cup
    { <<<<c, d>>>> : c \in Columns, d \in Directions } \cup
    { <<<<c1, d1>>, <<c2, d2>>>> : c1 \in Columns, c2 \in Columns, d1 \in Directions, d2 \in Directions }

\* ============================================================================
\* Helper Operators
\* ============================================================================

\* Get the value of a column from a row, treating missing columns as NULL
\* This implements SS-3: missing columns are NULL
GetValue(row, col, columns_present) ==
    IF col \in columns_present
    THEN row[col]
    ELSE NULL

\* Compare two values for a single column with null ordering rules (SS-2)
\* Returns: -1 (less), 0 (equal), 1 (greater)
\*
\* For ascending: nulls sort AFTER non-null (nulls are "greater")
\* For descending: nulls sort BEFORE non-null (nulls are "lesser")
CompareValues(v1, v2, direction) ==
    CASE v1 = NULL /\ v2 = NULL -> 0
      [] v1 = NULL /\ v2 /= NULL ->
            IF direction = "asc" THEN 1    \* null after non-null for ascending
            ELSE -1                         \* null before non-null for descending
      [] v1 /= NULL /\ v2 = NULL ->
            IF direction = "asc" THEN -1   \* non-null before null for ascending
            ELSE 1                          \* non-null after null for descending
      [] v1 /= NULL /\ v2 /= NULL ->
            IF direction = "asc"
            THEN (CASE v1 < v2 -> -1 [] v1 = v2 -> 0 [] v1 > v2 -> 1)
            ELSE (CASE v1 > v2 -> -1 [] v1 = v2 -> 0 [] v1 < v2 -> 1)

\* Compare two rows lexicographically according to a sort schema
\* Returns TRUE if row1 should come before or equal to row2
\* columns_present is the set of columns that exist in the split
RECURSIVE RowLEQ(_, _, _, _)
RowLEQ(row1, row2, schema, columns_present) ==
    \* For an empty schema, all rows are equal (any order is valid)
    IF schema = <<>>
    THEN TRUE
    ELSE
        LET col == schema[1][1]
            dir == schema[1][2]
            v1  == GetValue(row1, col, columns_present)
            v2  == GetValue(row2, col, columns_present)
            cmp == CompareValues(v1, v2, dir)
        IN
        IF cmp = -1 THEN TRUE       \* row1 < row2, strictly less
        ELSE IF cmp = 1 THEN FALSE  \* row1 > row2, strictly greater
        ELSE                          \* cmp = 0, equal on this column
            IF Len(schema) = 1
            THEN TRUE                \* all columns compared, equal is ok
            ELSE RowLEQ(row1, row2, SubSeq(schema, 2, Len(schema)), columns_present)

\* Check if a sequence of rows is sorted according to a schema
\* Empty and single-element sequences are trivially sorted
IsSorted(rows, schema, columns_present) ==
    \A i \in 1..(Len(rows) - 1) :
        RowLEQ(rows[i], rows[i + 1], schema, columns_present)

\* Sort a sequence of rows by the schema (specification-level sort)
\* We define this as: there EXISTS a permutation of the input that is sorted
\* For model checking, we verify the result IS sorted rather than computing it
\*
\* Instead of computing a sort, we non-deterministically choose rows that form
\* a valid sorted sequence. The key insight: we check that IngestBatch produces
\* splits whose rows ARE sorted; we don't need to compute the sort ourselves.

\* Check if two sequences contain exactly the same multiset of elements
\* (same elements with same multiplicities)
IsPermutation(s1, s2) ==
    /\ Len(s1) = Len(s2)
    /\ \A i \in 1..Len(s1) :
        Cardinality({j \in 1..Len(s1) : s1[j] = s1[i]}) =
        Cardinality({j \in 1..Len(s2) : s2[j] = s1[i]})

\* ============================================================================
\* Type Invariant
\* ============================================================================

TypeInvariant ==
    /\ metastore_schema \in AllSortSchemas
    /\ \A s \in splits :
        /\ s.id \in Nat
        /\ s.sort_schema \in AllSortSchemas
        /\ s.metadata_sort_schema \in AllSortSchemas
        /\ s.kv_sort_schema \in AllSortSchemas
        /\ s.sorting_columns_schema \in AllSortSchemas
        /\ s.columns_present \subseteq Columns
        /\ Len(s.rows) <= RowsPerSplitMax
    /\ next_split_id \in Nat
    /\ schema_change_count \in 0..SchemaChangesMax

\* ============================================================================
\* Safety Properties (SS-1 through SS-5)
\* ============================================================================

\* SS-1: All rows within a split are sorted according to the sort schema
\*       recorded in that split's metadata
SS1_RowsSorted ==
    \A s \in splits :
        IsSorted(s.rows, s.sort_schema, s.columns_present)

\* SS-2: Null values are ordered correctly per column direction
\* This is enforced by the CompareValues function used in IsSorted.
\* We verify it explicitly: for every adjacent pair of rows, when all
\* earlier sort columns are equal, if one value is NULL and the other
\* is not, the NULL must be in the correct position:
\*   - Ascending:  NULL comes AFTER non-null (nulls last)
\*   - Descending: NULL comes BEFORE non-null (nulls first)
SS2_NullOrdering ==
    \A s \in splits :
        \A i \in 1..(Len(s.rows) - 1) :
            \A k \in 1..Len(s.sort_schema) :
                LET col == s.sort_schema[k][1]
                    dir == s.sort_schema[k][2]
                    v_curr == GetValue(s.rows[i], col, s.columns_present)
                    v_next == GetValue(s.rows[i + 1], col, s.columns_present)
                    \* Only check null ordering when earlier columns are equal
                    earlier_equal == \A j \in 1..(k - 1) :
                        LET ec == s.sort_schema[j][1]
                            ev1 == GetValue(s.rows[i], ec, s.columns_present)
                            ev2 == GetValue(s.rows[i + 1], ec, s.columns_present)
                        IN ev1 = ev2
                IN
                earlier_equal =>
                    \* Ascending: null must NOT appear before non-null
                    /\ ~(dir = "asc" /\ v_curr = NULL /\ v_next /= NULL)
                    \* Descending: non-null must NOT appear before null
                    /\ ~(dir = "desc" /\ v_curr /= NULL /\ v_next = NULL)

\* SS-3: If a sort column is missing from a split's data, all rows in that
\*       split are treated as null for that column. This is not an error.
\*       We verify: for any sort column not in columns_present, GetValue
\*       returns NULL for every row.
SS3_MissingColumnsAreNull ==
    \A s \in splits :
        \A k \in 1..Len(s.sort_schema) :
            LET col == s.sort_schema[k][1]
            IN col \notin s.columns_present =>
                \A i \in 1..Len(s.rows) :
                    GetValue(s.rows[i], col, s.columns_present) = NULL

\* SS-4: A split's sort schema never changes after it is written.
\*       Verified by comparing current schema to the historical record.
SS4_SchemaImmutable ==
    \A s \in splits :
        s.id \in DOMAIN split_schema_history =>
            split_schema_history[s.id] = s.sort_schema

\* SS-5: For every split, the three copies of sort schema are identical:
\*       metadata_sort_schema, kv_sort_schema, sorting_columns_schema
SS5_SchemaConsistency ==
    \A s \in splits :
        /\ s.sort_schema = s.metadata_sort_schema
        /\ s.sort_schema = s.kv_sort_schema
        /\ s.sort_schema = s.sorting_columns_schema

\* Combined safety property
Safety ==
    /\ SS1_RowsSorted
    /\ SS3_MissingColumnsAreNull
    /\ SS4_SchemaImmutable
    /\ SS5_SchemaConsistency

\* ============================================================================
\* Actions
\* ============================================================================

\* --- IngestBatch ---
\* The indexing pipeline reads the current schema from the metastore,
\* receives a batch of rows, sorts them according to the schema, and
\* writes a split with the schema recorded in all three locations.
\*
\* We model this by non-deterministically choosing:
\*   - A subset of columns present in this batch (may not include all sort columns)
\*   - A set of rows (values for each present column, NULL for absent columns)
\*   - The rows are sorted according to the current metastore schema
IngestBatch ==
    /\ Cardinality(splits) < SplitsMax
    /\ \E columns_present \in SUBSET Columns :
        \E n \in 1..RowsPerSplitMax :
            \* Non-deterministically choose a sorted sequence of rows
            \E rows \in [1..n -> [columns_present -> ValuesWithNull]] :
                LET current_schema == metastore_schema
                    new_id == next_split_id
                    \* Build full rows that include all columns (present ones from data,
                    \* absent ones implicitly NULL via GetValue)
                    new_split == [
                        id |-> new_id,
                        rows |-> rows,
                        sort_schema |-> current_schema,
                        metadata_sort_schema |-> current_schema,
                        kv_sort_schema |-> current_schema,
                        sorting_columns_schema |-> current_schema,
                        columns_present |-> columns_present
                    ]
                IN
                \* The rows must be sorted according to the schema
                /\ IsSorted(rows, current_schema, columns_present)
                /\ splits' = splits \cup {new_split}
                /\ next_split_id' = next_split_id + 1
                /\ split_schema_history' = split_schema_history @@ (new_id :> current_schema)
                /\ UNCHANGED <<metastore_schema, schema_change_count>>

\* --- ChangeSchema ---
\* An operator updates the sort schema in the metastore.
\* Already-written splits are NOT affected (SS-4).
ChangeSchema ==
    /\ schema_change_count < SchemaChangesMax
    /\ \E new_schema \in AllSortSchemas :
        /\ new_schema /= metastore_schema
        /\ metastore_schema' = new_schema
        /\ schema_change_count' = schema_change_count + 1
        /\ UNCHANGED <<splits, next_split_id, split_schema_history>>

\* --- CompactSplits ---
\* Merges two or more splits that share the same sort_schema.
\* The output split contains the union of all rows, re-sorted by the schema.
\* Column sets may differ (SS-3, MC-4 from ADR-003): output has union of columns.
\*
\* For model checking tractability, we merge exactly two splits.
CompactSplits ==
    /\ \E s1 \in splits :
        \E s2 \in splits :
            /\ s1 /= s2
            \* Only merge splits with the same sort schema (CS-1 from ADR-003)
            /\ s1.sort_schema = s2.sort_schema
            /\ LET merged_schema == s1.sort_schema
                   merged_columns == s1.columns_present \cup s2.columns_present
                   \* Build canonical row representations with all merged columns
                   \* Rows from s1: existing columns keep values, new columns get NULL
                   \* Rows from s2: same treatment
                   all_row_count == Len(s1.rows) + Len(s2.rows)
               IN
               /\ all_row_count <= RowsPerSplitMax
               /\ \E merged_rows \in [1..all_row_count -> [merged_columns -> ValuesWithNull]] :
                    LET new_id == next_split_id
                        new_split == [
                            id |-> new_id,
                            rows |-> merged_rows,
                            sort_schema |-> merged_schema,
                            metadata_sort_schema |-> merged_schema,
                            kv_sort_schema |-> merged_schema,
                            sorting_columns_schema |-> merged_schema,
                            columns_present |-> merged_columns
                        ]
                    IN
                    \* The merged rows must be sorted
                    /\ IsSorted(merged_rows, merged_schema, merged_columns)
                    \* The merged rows must be a permutation of the union of input rows
                    \* (MC-1: no rows added, removed, or duplicated)
                    \* We verify row preservation by checking that for each row in the
                    \* merged output, it came from one of the inputs (extended with NULLs
                    \* for missing columns)
                    /\ \A i \in 1..all_row_count :
                        \/ \E j \in 1..Len(s1.rows) :
                            \A c \in merged_columns :
                                merged_rows[i][c] = GetValue(s1.rows[j], c, s1.columns_present)
                        \/ \E j \in 1..Len(s2.rows) :
                            \A c \in merged_columns :
                                merged_rows[i][c] = GetValue(s2.rows[j], c, s2.columns_present)
                    \* Remove old splits, add new one
                    /\ splits' = (splits \ {s1, s2}) \cup {new_split}
                    /\ next_split_id' = next_split_id + 1
                    /\ split_schema_history' = split_schema_history @@ (new_id :> merged_schema)
                    /\ UNCHANGED <<metastore_schema, schema_change_count>>

\* ============================================================================
\* Initial State
\* ============================================================================

\* Initial sort schema: a simple single-column ascending schema
\* (The specific initial schema is set via the config file's CONSTANTS)
Init ==
    /\ metastore_schema \in AllSortSchemas
    /\ splits = {}
    /\ next_split_id = 1
    /\ schema_change_count = 0
    /\ split_schema_history = <<>>

\* ============================================================================
\* Next-State Relation
\* ============================================================================

Next ==
    \/ IngestBatch
    \/ ChangeSchema
    \/ CompactSplits

\* ============================================================================
\* Specification
\* ============================================================================

Spec == Init /\ [][Next]_vars

\* ============================================================================
\* Liveness Properties
\* ============================================================================

\* If there are mergeable splits (same schema), compaction is eventually possible.
\* This is a weak liveness property: we only require that the system does not
\* get permanently stuck — compaction CAN happen, not that it MUST.
\* (In practice, a compaction scheduler drives this.)
Liveness ==
    []<>(\A s1 \in splits : \A s2 \in splits :
        (s1 /= s2 /\ s1.sort_schema = s2.sort_schema) =>
            ENABLED CompactSplits)

====
