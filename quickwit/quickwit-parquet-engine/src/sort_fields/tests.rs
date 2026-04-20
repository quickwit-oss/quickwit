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

//! Test suite ported from Go `schemautils_test.go` plus strict V2-only enforcement tests.

use quickwit_proto::sortschema::{SortColumnDirection, SortSchema};

use super::column_type::ColumnTypeId;
use super::display::{schema_to_string, schema_to_string_short};
use super::equivalence::{equivalent_schemas, equivalent_schemas_for_compaction};
use super::parser::parse_sort_fields;

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn must_parse(s: &str) -> SortSchema {
    parse_sort_fields(s).unwrap_or_else(|e| panic!("failed to parse '{}': {}", s, e))
}

// ---------------------------------------------------------------------------
// Strict V2-only enforcement tests
// ---------------------------------------------------------------------------

#[test]
fn test_v2_only_rejects_unversioned() {
    // No version suffix defaults to version 0 -> rejected.
    let err = parse_sort_fields("timestamp").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported") || msg.contains("version"),
        "expected unsupported version error, got: {}",
        msg
    );
}

#[test]
fn test_v2_only_rejects_v0() {
    let err = parse_sort_fields("timestamp/V0").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported") || msg.contains("version"),
        "expected unsupported version error, got: {}",
        msg
    );
}

#[test]
fn test_v2_only_rejects_v1() {
    let err = parse_sort_fields("timestamp/V1").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported") || msg.contains("version"),
        "expected unsupported version error, got: {}",
        msg
    );
}

#[test]
fn test_v2_only_accepts_v2() {
    let schema = must_parse("timestamp/V2");
    assert_eq!(schema.sort_version, 2);
    assert_eq!(schema.column.len(), 1);
    assert_eq!(schema.column[0].name, "timestamp");
}

#[test]
fn test_v2_only_accepts_v3() {
    let schema = must_parse("timestamp/V3");
    assert_eq!(schema.sort_version, 3);
}

// ---------------------------------------------------------------------------
// Port of Go TestStringToSchema -- error paths
// ---------------------------------------------------------------------------

#[test]
fn test_string_to_schema_mangled() {
    // Mangled schema with multiple `=`
    assert!(parse_sort_fields("a=b=c/V2").is_err(), "must disallow >1 =");
}

#[test]
fn test_string_to_schema_invalid_column_format() {
    assert!(
        parse_sort_fields("name:dense-int64:+:what-is-this?/V2").is_err(),
        "must disallow invalid column formats"
    );
    assert!(
        parse_sort_fields("name:dense-int64:+:what-is-this?:really?/V2").is_err(),
        "must disallow invalid column formats"
    );
}

#[test]
fn test_string_to_schema_3part_errors() {
    // Invalid type name.
    assert!(
        parse_sort_fields("name__i:invalid-type:+|timestamp/V2").is_err(),
        "must disallow an invalid type"
    );
    // Type mismatch: suffix says int64 but explicit says string.
    assert!(
        parse_sort_fields("name__i:dense-string:+|timestamp/V2").is_err(),
        "must disallow mismatch between type suffix and explicit type"
    );
    // Invalid sort direction.
    assert!(
        parse_sort_fields("name__i:dense-int64:invalid-sort-direction|timestamp/V2").is_err(),
        "must disallow an invalid sort direction"
    );
}

#[test]
fn test_string_to_schema_2part_errors() {
    // `name__x` no longer errors: unknown suffixes are treated as bare column names
    // with default type (String), so `name__x` is a valid column name.
    assert!(
        parse_sort_fields("name__x:+|timestamp/V2").is_ok(),
        "bare column names with unknown suffix-like patterns are now valid"
    );
    // Invalid sort direction.
    assert!(
        parse_sort_fields("name__i:invalid-sort-direction|timestamp/V2").is_err(),
        "must disallow an invalid sort direction"
    );
}

#[test]
fn test_string_to_schema_missing_timestamp() {
    // Semantically invalid: missing timestamp column.
    assert!(
        parse_sort_fields("name__i:dense-int64:+/V2").is_err(),
        "must disallow schema with missing timestamp column"
    );
}

#[test]
fn test_string_to_schema_bad_version() {
    // `/X` is not a valid version specification.
    assert!(
        parse_sort_fields("timestamp/X").is_err(),
        "/X isn't a valid version specification"
    );
    // `/VX` -- V followed by non-numeric.
    assert!(
        parse_sort_fields("timestamp/VX").is_err(),
        "/VX isn't a valid version specification"
    );
}

// ---------------------------------------------------------------------------
// LSM cutoff marker error paths (from Go TestStringToSchema)
// ---------------------------------------------------------------------------

#[test]
fn test_lsm_cutoff_multiple_markers() {
    assert!(
        parse_sort_fields("service__s|&env__s|&source__s|timestamp/V2").is_err(),
        "must disallow multiple LSM cutoff markers"
    );
}

#[test]
fn test_lsm_cutoff_double_ampersand() {
    assert!(
        parse_sort_fields("service__s|&&env__s|timestamp/V2").is_err(),
        "must disallow multiple consecutive LSM cutoff markers"
    );
}

#[test]
fn test_lsm_cutoff_empty_after_marker() {
    assert!(
        parse_sort_fields("service__s|&|timestamp/V2").is_err(),
        "must disallow empty column name after LSM cutoff marker"
    );
}

#[test]
fn test_lsm_cutoff_in_middle() {
    assert!(
        parse_sort_fields("service__s|env&__s|timestamp/V2").is_err(),
        "must disallow LSM cutoff marker in middle of column name"
    );
}

#[test]
fn test_lsm_cutoff_single_column() {
    assert!(
        parse_sort_fields("&timestamp/V2").is_err(),
        "must disallow LSM cutoff marker on single column schema"
    );
}

#[test]
fn test_lsm_cutoff_first_column() {
    assert!(
        parse_sort_fields("&service__s|env__s|timestamp/V2").is_err(),
        "must disallow LSM cutoff marker on first column"
    );
}

// ---------------------------------------------------------------------------
// Port of Go TestStringToSchema -- valid schemas
// ---------------------------------------------------------------------------

#[test]
fn test_string_to_schema_timestamp_only() {
    let s = must_parse("timestamp/V2");
    assert_eq!(s.column.len(), 1);
    assert_eq!(s.column[0].name, "timestamp");
    assert_eq!(s.column[0].column_type, ColumnTypeId::Int64 as u64);
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32
    );
}

#[test]
fn test_string_to_schema_named_timestamp() {
    let s = must_parse("defaultTimestampSchema=timestamp/V2");
    assert_eq!(s.name, "defaultTimestampSchema");
    assert_eq!(s.column.len(), 1);
    assert_eq!(s.column[0].name, "timestamp");
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32
    );
}

#[test]
fn test_string_to_schema_named_timestamp_explicit_direction() {
    let s = must_parse("defaultTimestampSchema=timestamp:-/V2");
    assert_eq!(s.name, "defaultTimestampSchema");
    assert_eq!(s.column[0].name, "timestamp");
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32
    );
}

#[test]
fn test_string_to_schema_named_timestamp_explicit_type() {
    let s = must_parse("defaultTimestampSchema=timestamp:dense-int64:-/V2");
    assert_eq!(s.name, "defaultTimestampSchema");
    assert_eq!(s.column[0].name, "timestamp");
    assert_eq!(s.column[0].column_type, ColumnTypeId::Int64 as u64);
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32
    );
}

#[test]
fn test_string_to_schema_multi_column() {
    let s = must_parse("testSchema=columnA__s|columnB__i:-|timestamp/V2");
    assert_eq!(s.name, "testSchema");
    assert_eq!(s.sort_version, 2);
    assert_eq!(s.column.len(), 3);

    // columnA__s: string, ascending (default)
    assert_eq!(s.column[0].name, "columnA");
    assert_eq!(s.column[0].column_type, ColumnTypeId::String as u64);
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionAscending as i32
    );

    // columnB__i: int64, descending (explicit)
    assert_eq!(s.column[1].name, "columnB");
    assert_eq!(s.column[1].column_type, ColumnTypeId::Int64 as u64);
    assert_eq!(
        s.column[1].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32
    );

    // timestamp: int64, descending (default)
    assert_eq!(s.column[2].name, "timestamp");
    assert_eq!(s.column[2].column_type, ColumnTypeId::Int64 as u64);
    assert_eq!(
        s.column[2].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32
    );
}

#[test]
fn test_string_to_schema_multi_column_explicit_type() {
    let s = must_parse("testSchema=columnA__s:dense-string:+|columnB__i:-|timestamp/V2");
    assert_eq!(s.column[0].column_type, ColumnTypeId::String as u64);
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionAscending as i32
    );
}

// ---------------------------------------------------------------------------
// SchemaToString and SchemaToStringShort
// ---------------------------------------------------------------------------

#[test]
fn test_schema_to_string_full() {
    let s = must_parse("testSchema=columnA__s|columnB__i:-|timestamp/V2");
    let full = schema_to_string(&s);
    // columnA has type String == default for a generic name, so no suffix in output.
    // columnB has type Int64 != default String, so __i suffix is preserved.
    assert_eq!(
        full,
        "testSchema=columnA:dense-string:+|columnB__i:dense-int64:-|timestamp:dense-int64:-/V2"
    );
}

#[test]
fn test_schema_to_string_short() {
    let s = must_parse("testSchema=columnA__s|columnB__i:-|timestamp/V2");
    let short = schema_to_string_short(&s);
    // columnA has type String == default, so no suffix. columnB keeps __i (Int64 != default).
    assert_eq!(short, "testSchema=columnA|columnB__i:-|timestamp/V2");
}

// ---------------------------------------------------------------------------
// Round-trip tests
// ---------------------------------------------------------------------------

#[test]
fn test_round_trip_short_form() {
    // Inputs that round-trip to themselves: bare names (no suffix) and non-default
    // typed columns already serialize without a suffix change.
    let exact_round_trip_cases = [
        "timestamp/V2",
        // `service` with no suffix: default String, serializes as `service` (no suffix).
        "service|timestamp/V2",
        "service|env|timestamp/V2",
        // columnB__i: Int64 != default String, keeps __i suffix.
        "testSchema=columnA|columnB__i:-|timestamp/V2",
    ];
    for input in exact_round_trip_cases {
        let parsed = must_parse(input);
        let short = schema_to_string_short(&parsed);
        assert_eq!(short, input, "short round-trip failed for '{}'", input);

        // Also verify full round-trip: parse(to_string(schema)) == schema.
        let full = schema_to_string(&parsed);
        let reparsed = must_parse(&full);
        assert_eq!(
            parsed.column.len(),
            reparsed.column.len(),
            "column count mismatch after full round-trip for '{}'",
            input
        );
        for (a, b) in parsed.column.iter().zip(reparsed.column.iter()) {
            assert_eq!(a.name, b.name);
            assert_eq!(a.column_type, b.column_type);
            assert_eq!(a.sort_direction, b.sort_direction);
        }
        assert_eq!(parsed.sort_version, reparsed.sort_version);
        assert_eq!(parsed.lsm_comparison_cutoff, reparsed.lsm_comparison_cutoff);
    }

    // Inputs with explicit __s suffix on default-String columns: the short form drops
    // the suffix (it is not needed), but the semantic content is preserved.
    let semantic_round_trip_cases = [
        ("service__s|timestamp/V2", "service|timestamp/V2"),
        ("service__s|env__s|timestamp/V2", "service|env|timestamp/V2"),
        (
            "testSchema=columnA__s|columnB__i:-|timestamp/V2",
            "testSchema=columnA|columnB__i:-|timestamp/V2",
        ),
    ];
    for (input, expected_short) in semantic_round_trip_cases {
        let parsed = must_parse(input);
        let short = schema_to_string_short(&parsed);
        assert_eq!(short, expected_short, "short form mismatch for '{}'", input);

        // Verify semantic round-trip: parse the short output and compare protos.
        let reparsed = must_parse(&short);
        assert_eq!(
            parsed.column.len(),
            reparsed.column.len(),
            "column count mismatch after semantic round-trip for '{}'",
            input
        );
        for (a, b) in parsed.column.iter().zip(reparsed.column.iter()) {
            assert_eq!(a.name, b.name);
            assert_eq!(a.column_type, b.column_type);
            assert_eq!(a.sort_direction, b.sort_direction);
        }
        assert_eq!(parsed.sort_version, reparsed.sort_version);
        assert_eq!(parsed.lsm_comparison_cutoff, reparsed.lsm_comparison_cutoff);
    }
}

// ---------------------------------------------------------------------------
// Port of Go TestEquivalentSchemas
// ---------------------------------------------------------------------------

#[test]
fn test_equivalent_schemas_identical() {
    let a = must_parse("timestamp/V2");
    let b = must_parse("timestamp/V2");
    assert!(equivalent_schemas(&a, &b));
    assert!(equivalent_schemas_for_compaction(&a, &b));
}

#[test]
fn test_equivalent_schemas_different_column_counts() {
    let a = must_parse("service__s|timestamp/V2");
    let b = must_parse("timestamp/V2");
    assert!(!equivalent_schemas(&a, &b));
    assert!(!equivalent_schemas_for_compaction(&a, &b));
}

#[test]
fn test_equivalent_schemas_same_columns_different_names() {
    let a = must_parse("service__s|timestamp/V2");
    let b = must_parse("serviceSchema=service__s|timestamp/V2");
    assert!(equivalent_schemas(&a, &b));
    assert!(equivalent_schemas_for_compaction(&a, &b));
}

#[test]
fn test_equivalent_schemas_different_column_order() {
    let a = must_parse("env__s|service__s|timestamp/V2");
    let b = must_parse("serviceSchema=service__s|timestamp/V2");
    assert!(!equivalent_schemas(&a, &b));
    assert!(!equivalent_schemas_for_compaction(&a, &b));
}

#[test]
fn test_equivalent_schemas_different_versions() {
    let a = must_parse("service__s|timestamp/V2");
    let b = must_parse("serviceSchema=service__s|timestamp/V3");
    assert!(!equivalent_schemas(&a, &b));
    assert!(!equivalent_schemas_for_compaction(&a, &b));
}

#[test]
fn test_equivalent_schemas_different_lsm_cutoffs() {
    // Different LSM cutoffs: affects EquivalentSchemas but NOT EquivalentSchemasForCompaction.
    let a = must_parse("service__s|&env__s|timestamp/V2");
    let b = must_parse("service__s|env__s|timestamp/V2");
    assert!(
        !equivalent_schemas(&a, &b),
        "different LSM cutoffs should not be equivalent"
    );
    assert!(
        equivalent_schemas_for_compaction(&a, &b),
        "different LSM cutoffs should be equivalent for compaction"
    );
}

#[test]
fn test_equivalent_schemas_different_cutoff_positions() {
    let a = must_parse("service__s|&env__s|timestamp/V2");
    let b = must_parse("service__s|env__s|&timestamp/V2");
    assert!(!equivalent_schemas(&a, &b));
    assert!(equivalent_schemas_for_compaction(&a, &b));
}

#[test]
fn test_equivalent_schemas_identical_lsm_cutoffs() {
    let a = must_parse("service__s|&env__s|timestamp/V2");
    let b = must_parse("service__s|&env__s|timestamp/V2");
    assert!(equivalent_schemas(&a, &b));
    assert!(equivalent_schemas_for_compaction(&a, &b));
}

// ---------------------------------------------------------------------------
// Port of Go TestStringToSchemaWithLSMCutoff
// ---------------------------------------------------------------------------

#[test]
fn test_lsm_cutoff_after_first_column() {
    let s = must_parse("service__s|&env__s|timestamp/V2");
    assert_eq!(s.lsm_comparison_cutoff, 1);
    assert_eq!(s.column[1].name, "env"); // "&" stripped
}

#[test]
fn test_lsm_cutoff_after_second_column() {
    let s = must_parse("service__s|env__s|&source__s|timestamp/V2");
    assert_eq!(s.lsm_comparison_cutoff, 2);
    assert_eq!(s.column[2].name, "source");
}

#[test]
fn test_lsm_cutoff_before_timestamp() {
    let s = must_parse("service__s|env__s|source__s|&timestamp/V2");
    assert_eq!(s.lsm_comparison_cutoff, 3);
    assert_eq!(s.column[3].name, "timestamp");
}

#[test]
fn test_lsm_cutoff_named_schema() {
    let s = must_parse("testSchema=service__s|&env__s|timestamp/V2");
    assert_eq!(s.name, "testSchema");
    assert_eq!(s.lsm_comparison_cutoff, 1);
}

#[test]
fn test_lsm_cutoff_with_version() {
    let s = must_parse("service__s|&env__s|timestamp/V2");
    assert_eq!(s.lsm_comparison_cutoff, 1);
    assert_eq!(s.sort_version, 2);
}

#[test]
fn test_lsm_cutoff_with_explicit_type_direction() {
    let s = must_parse("service__s:dense-string:+|&env__s:dense-string:+|timestamp/V2");
    assert_eq!(s.lsm_comparison_cutoff, 1);
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionAscending as i32
    );
}

#[test]
fn test_no_lsm_cutoff() {
    let s = must_parse("service__s|env__s|timestamp/V2");
    assert_eq!(s.lsm_comparison_cutoff, 0);
}

// ---------------------------------------------------------------------------
// Port of Go TestSchemaToStringWithLSMCutoff
// ---------------------------------------------------------------------------

#[test]
fn test_schema_to_string_preserves_cutoff_marker_short() {
    let s = must_parse("service__s|&env__s|timestamp/V2");
    // String columns with default type serialize without the __s suffix.
    assert_eq!(schema_to_string_short(&s), "service|&env|timestamp/V2");
}

#[test]
fn test_schema_to_string_preserves_cutoff_marker_full() {
    let s = must_parse("service__s|&env__s|timestamp/V2");
    // String columns with default type serialize without the __s suffix in full form too.
    let expected = "service:dense-string:+|&env:dense-string:+|timestamp:dense-int64:-/V2";
    assert_eq!(schema_to_string(&s), expected);
}

#[test]
fn test_schema_to_string_named_with_cutoff() {
    let s = must_parse("testSchema=service__s|&env__s|timestamp/V2");
    // String columns with default type serialize without the __s suffix.
    assert_eq!(
        schema_to_string_short(&s),
        "testSchema=service|&env|timestamp/V2"
    );
}

#[test]
fn test_schema_to_string_no_cutoff() {
    let s = must_parse("service__s|env__s|timestamp/V2");
    // String columns with default type serialize without the __s suffix.
    assert_eq!(schema_to_string_short(&s), "service|env|timestamp/V2");
}

// ---------------------------------------------------------------------------
// Port of Go TestLSMCutoffRoundTrip
// ---------------------------------------------------------------------------

#[test]
fn test_lsm_cutoff_round_trip() {
    // Pairs of (input, expected_short_output).
    // String columns with default type serialize without the __s suffix, so inputs
    // using __s produce shorter output that is semantically equivalent.
    let test_cases = [
        (
            "service__s|&env__s|timestamp/V2",
            "service|&env|timestamp/V2",
        ),
        (
            "service__s|env__s|&source__s|timestamp/V2",
            "service|env|&source|timestamp/V2",
        ),
        (
            "service__s|env__s|source__s|&timestamp/V2",
            "service|env|source|&timestamp/V2",
        ),
        (
            "testSchema=service__s|&env__s|timestamp/V2",
            "testSchema=service|&env|timestamp/V2",
        ),
        ("service__s|env__s|timestamp/V2", "service|env|timestamp/V2"),
    ];
    for (input, expected_short) in test_cases {
        let parsed = must_parse(input);
        let result = schema_to_string_short(&parsed);
        assert_eq!(result, expected_short, "round-trip failed for '{}'", input);

        // Verify the output parses back to the same proto (semantic round-trip).
        let reparsed = must_parse(&result);
        assert_eq!(
            parsed.column.len(),
            reparsed.column.len(),
            "column count mismatch after round-trip for '{}'",
            input
        );
        for (a, b) in parsed.column.iter().zip(reparsed.column.iter()) {
            assert_eq!(a.name, b.name);
            assert_eq!(a.column_type, b.column_type);
            assert_eq!(a.sort_direction, b.sort_direction);
        }
        assert_eq!(
            parsed.lsm_comparison_cutoff, reparsed.lsm_comparison_cutoff,
            "LSM cutoff mismatch for '{}'",
            input
        );
    }
}

// ---------------------------------------------------------------------------
// Direction prefix/suffix tests
// ---------------------------------------------------------------------------

#[test]
fn test_direction_prefix_ascending() {
    let s = must_parse("+service__s|timestamp/V2");
    assert_eq!(s.column[0].name, "service");
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionAscending as i32
    );
}

#[test]
fn test_direction_prefix_descending() {
    let s = must_parse("-service__s|timestamp/V2");
    assert_eq!(s.column[0].name, "service");
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32
    );
}

#[test]
fn test_direction_suffix_ascending() {
    let s = must_parse("service__s+|timestamp/V2");
    assert_eq!(s.column[0].name, "service");
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionAscending as i32
    );
}

#[test]
fn test_direction_suffix_descending() {
    let s = must_parse("service__s-|timestamp/V2");
    assert_eq!(s.column[0].name, "service");
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32
    );
}

#[test]
fn test_direction_prefix_descending_on_timestamp() {
    // Explicit descending prefix on timestamp matches the default and is accepted.
    let s = must_parse("service__s|-timestamp/V2");
    assert_eq!(s.column[1].name, "timestamp");
    assert_eq!(
        s.column[1].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32
    );
}

#[test]
fn test_direction_prefix_ascending_on_timestamp_rejected() {
    // Ascending timestamp is rejected by validation (timestamp must be descending).
    assert!(
        parse_sort_fields("service__s|+timestamp/V2").is_err(),
        "ascending timestamp must be rejected by validation"
    );
}

#[test]
fn test_direction_suffix_on_timestamp() {
    let s = must_parse("service__s|timestamp-/V2");
    assert_eq!(s.column[1].name, "timestamp");
    assert_eq!(
        s.column[1].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32
    );
}

#[test]
fn test_direction_prefix_and_suffix_is_error() {
    // +name+ : direction on both sides
    assert!(
        parse_sort_fields("+service__s+|timestamp/V2").is_err(),
        "must reject direction on both prefix and suffix"
    );
    // -name- : same direction both sides, still error
    assert!(
        parse_sort_fields("-service__s-|timestamp/V2").is_err(),
        "must reject direction on both prefix and suffix (same direction)"
    );
    // +name- : conflicting directions
    assert!(
        parse_sort_fields("+service__s-|timestamp/V2").is_err(),
        "must reject conflicting direction prefix and suffix"
    );
}

#[test]
fn test_direction_prefix_with_colon_direction_is_error() {
    // +name:+ : prefix direction + colon direction
    assert!(
        parse_sort_fields("+service__s:+|timestamp/V2").is_err(),
        "must reject direction in both prefix and colon form"
    );
}

#[test]
fn test_direction_suffix_with_colon_direction_is_error() {
    // name-:- : suffix direction + colon direction
    assert!(
        parse_sort_fields("service__s-:-|timestamp/V2").is_err(),
        "must reject direction in both suffix and colon form"
    );
}

#[test]
fn test_direction_prefix_multi_column() {
    let s = must_parse("-metric_name__s|+host__s|timestamp/V2");
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32
    );
    assert_eq!(
        s.column[1].sort_direction,
        SortColumnDirection::SortDirectionAscending as i32
    );
    assert_eq!(
        s.column[2].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32 // default for timestamp
    );
}

#[test]
fn test_direction_prefix_with_lsm_cutoff() {
    // &+env__s : cutoff marker then direction prefix
    let s = must_parse("service__s|&+env__s|timestamp/V2");
    assert_eq!(s.lsm_comparison_cutoff, 1);
    assert_eq!(s.column[1].name, "env");
    assert_eq!(
        s.column[1].sort_direction,
        SortColumnDirection::SortDirectionAscending as i32
    );
}

#[test]
fn test_direction_suffix_with_lsm_cutoff() {
    let s = must_parse("service__s|&env__s-|timestamp/V2");
    assert_eq!(s.lsm_comparison_cutoff, 1);
    assert_eq!(s.column[1].name, "env");
    assert_eq!(
        s.column[1].sort_direction,
        SortColumnDirection::SortDirectionDescending as i32
    );
}

// ---------------------------------------------------------------------------
// Bare name parsing (no type suffix — uses defaults)
// ---------------------------------------------------------------------------

#[test]
fn test_bare_names_default_to_string() {
    let s = must_parse("service|env|host|timestamp/V2");
    assert_eq!(s.column[0].name, "service");
    assert_eq!(s.column[0].column_type, ColumnTypeId::String as u64);
    assert_eq!(s.column[1].name, "env");
    assert_eq!(s.column[1].column_type, ColumnTypeId::String as u64);
    assert_eq!(s.column[2].name, "host");
    assert_eq!(s.column[2].column_type, ColumnTypeId::String as u64);
}

#[test]
fn test_bare_timestamp_defaults_to_int64() {
    let s = must_parse("service|timestamp/V2");
    assert_eq!(s.column[1].name, "timestamp");
    assert_eq!(s.column[1].column_type, ColumnTypeId::Int64 as u64);
}

#[test]
fn test_bare_timeseries_id_defaults_to_int64() {
    let s = must_parse("service|timeseries_id|timestamp/V2");
    assert_eq!(s.column[1].name, "timeseries_id");
    assert_eq!(s.column[1].column_type, ColumnTypeId::Int64 as u64);
}

#[test]
fn test_bare_tiebreaker_defaults_to_int64() {
    let s = must_parse("service|timestamp|tiebreaker/V2");
    assert_eq!(s.column[2].name, "tiebreaker");
    assert_eq!(s.column[2].column_type, ColumnTypeId::Int64 as u64);
}

#[test]
fn test_bare_metric_value_defaults_to_float64() {
    let s = must_parse("metric_value|timestamp/V2");
    assert_eq!(s.column[0].name, "metric_value");
    assert_eq!(s.column[0].column_type, ColumnTypeId::Float64 as u64);
}

#[test]
fn test_bare_and_suffixed_produce_same_proto() {
    let bare = must_parse("metric_name|host|timeseries_id|timestamp/V2");
    let suffixed = must_parse("metric_name__s|host__s|timeseries_id__i|timestamp/V2");
    assert_eq!(bare.column.len(), suffixed.column.len());
    for (a, b) in bare.column.iter().zip(suffixed.column.iter()) {
        assert_eq!(a.name, b.name, "names should match");
        assert_eq!(
            a.column_type, b.column_type,
            "types should match for {}",
            a.name
        );
        assert_eq!(
            a.sort_direction, b.sort_direction,
            "directions should match for {}",
            a.name
        );
    }
}

#[test]
fn test_suffix_overrides_default() {
    // metric_value defaults to Float64, but __i suffix forces Int64
    let s = must_parse("metric_value__i|timestamp/V2");
    assert_eq!(s.column[0].name, "metric_value");
    assert_eq!(s.column[0].column_type, ColumnTypeId::Int64 as u64);
}

#[test]
fn test_display_omits_default_suffix() {
    let s = must_parse("metric_name|host|timestamp/V2");
    let short = schema_to_string_short(&s);
    // All columns use default types, so no suffixes in output.
    assert_eq!(short, "metric_name|host|timestamp/V2");
}

#[test]
fn test_display_includes_non_default_suffix() {
    // Force host to Int64 (non-default for bare "host" which defaults to String)
    let s = must_parse("metric_name|host__i|timestamp/V2");
    let short = schema_to_string_short(&s);
    assert_eq!(short, "metric_name|host__i|timestamp/V2");
}

// ---------------------------------------------------------------------------
// timeseries_id handling
// ---------------------------------------------------------------------------

#[test]
fn test_timeseries_id_as_int64() {
    let s = must_parse("metric_name__s|host__s|timeseries_id__i|timestamp/V2");
    assert_eq!(s.column.len(), 4);

    // timeseries_id__i should be TypeIDInt64, ascending.
    let ts_id_col = &s.column[2];
    assert_eq!(ts_id_col.name, "timeseries_id");
    assert_eq!(ts_id_col.column_type, ColumnTypeId::Int64 as u64);
    assert_eq!(
        ts_id_col.sort_direction,
        SortColumnDirection::SortDirectionAscending as i32
    );
}

// ---------------------------------------------------------------------------
// Float and sketch column types
// ---------------------------------------------------------------------------

#[test]
fn test_float_column() {
    let s = must_parse("value__nf|timestamp/V2");
    assert_eq!(s.column[0].column_type, ColumnTypeId::Float64 as u64);
    assert_eq!(
        s.column[0].sort_direction,
        SortColumnDirection::SortDirectionAscending as i32
    );
}

#[test]
fn test_sketch_column() {
    let s = must_parse("latency__sk|timestamp/V2");
    assert_eq!(s.column[0].column_type, ColumnTypeId::Sketch as u64);
}

// ---------------------------------------------------------------------------
// SchemasToString / SchemasToStringShort multi-schema (convenience)
// ---------------------------------------------------------------------------

#[test]
fn test_schemas_to_string() {
    let schema1 = must_parse("test=key1__s|timestamp/V2");
    let schema2 = must_parse("key2__i|timestamp/V2");

    let schemas = [schema1, schema2];
    let strings: Vec<String> = schemas.iter().map(schema_to_string).collect();
    let actual = strings.join(",");

    // key1 has type String == default, so no suffix. key2 has type Int64 != default, keeps __i.
    assert_eq!(
        actual,
        "test=key1:dense-string:+|timestamp:dense-int64:-/V2,key2__i:dense-int64:+|timestamp:\
         dense-int64:-/V2"
    );
}

#[test]
fn test_schemas_to_string_short() {
    let schema1 = must_parse("test=key1__s|timestamp/V2");
    let schema2 = must_parse("key2__i|timestamp/V2");

    let schemas = [schema1, schema2];
    let strings: Vec<String> = schemas.iter().map(schema_to_string_short).collect();
    let actual = strings.join(",");

    // key1 has type String == default, so no suffix. key2 has type Int64 != default, keeps __i.
    assert_eq!(actual, "test=key1|timestamp/V2,key2__i|timestamp/V2");
}

// ---------------------------------------------------------------------------
// ColumnTypeId TryFrom<u64> (proto deserialization path)
// ---------------------------------------------------------------------------

#[test]
fn test_column_type_try_from_u64_valid() {
    assert_eq!(ColumnTypeId::try_from(2u64).unwrap(), ColumnTypeId::Int64);
    assert_eq!(
        ColumnTypeId::try_from(10u64).unwrap(),
        ColumnTypeId::Float64
    );
    assert_eq!(ColumnTypeId::try_from(14u64).unwrap(), ColumnTypeId::String);
    assert_eq!(ColumnTypeId::try_from(17u64).unwrap(), ColumnTypeId::Sketch);
    assert_eq!(
        ColumnTypeId::try_from(20u64).unwrap(),
        ColumnTypeId::CpcSketch
    );
    assert_eq!(
        ColumnTypeId::try_from(22u64).unwrap(),
        ColumnTypeId::ItemSketch
    );
}

#[test]
fn test_column_type_try_from_u64_invalid() {
    assert!(ColumnTypeId::try_from(0u64).is_err());
    assert!(ColumnTypeId::try_from(1u64).is_err());
    assert!(ColumnTypeId::try_from(99u64).is_err());
}
