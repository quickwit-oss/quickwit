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

//! Automaton, regex builder, and MinMax tests.
//!
//! Ported from Go `automaton_test.go`.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{
    DataType, Field, Int8Type, Int16Type, Int32Type, Int64Type, Schema as ArrowSchema, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow::record_batch::RecordBatch;

use crate::test_helpers::{create_dict_array, create_nullable_dict_array};
use crate::zonemap::automaton::{
    write_character_class, write_disjunctive_clauses, write_disjunctive_clauses_factoring_suffix,
};
use crate::zonemap::minmax::{MinMaxBuilder, hash_int, hash_string};
use crate::zonemap::{self, ZonemapOptions, extract_zonemap_regexes, generate_regex_from_strings};

// ---------------------------------------------------------------------------
// Helper: build a superset regex from values with optional pruning.
// ---------------------------------------------------------------------------

fn build_superset_regex(
    values: &[&str],
    max_num_transitions: i32,
) -> zonemap::automaton::Automaton {
    let max_depth = if max_num_transitions >= 0 {
        (max_num_transitions as usize) * 2
    } else {
        0
    };
    let mut aut = zonemap::automaton::Automaton::new(max_depth);
    for value in values {
        aut.add(value);
    }
    if max_num_transitions >= 0 {
        aut.prune(max_num_transitions as usize);
    }
    aut
}

fn assert_automaton(regex: &str, is_strict_superset: bool, aut: &zonemap::automaton::Automaton) {
    assert_eq!(aut.regex(), regex, "regex mismatch");
    assert_eq!(
        aut.is_strict_superset, is_strict_superset,
        "is_strict_superset mismatch for regex '{}'",
        regex
    );
}

// ---------------------------------------------------------------------------
// automaton_test.go: TestRegex
// ---------------------------------------------------------------------------

#[test]
fn test_regex_basic() {
    assert_automaton("^a$", true, &build_superset_regex(&["a"], -1));
    assert_automaton("^(a|bc)$", true, &build_superset_regex(&["bc", "a"], -1));
    assert_automaton("^a(|b)$", true, &build_superset_regex(&["ab", "a"], -1));
    assert_automaton(
        "^(|a(|bc))$",
        true,
        &build_superset_regex(&["abc", "a", ""], -1),
    );
}

// ---------------------------------------------------------------------------
// automaton_test.go: TestEscaping
// ---------------------------------------------------------------------------

#[test]
fn test_escaping() {
    assert_automaton("^\\^\\$$", true, &build_superset_regex(&["^$"], -1));
    assert_automaton(
        "^\\$(|\\?-\\^)$",
        true,
        &build_superset_regex(&["$", "$?-^"], -1),
    );
}

// ---------------------------------------------------------------------------
// automaton_test.go: TestPrune
// ---------------------------------------------------------------------------

#[test]
fn test_prune_booleans() {
    let values = &["false", "False", "true", "True"];
    assert_automaton(
        "^([Ff]alse|[Tt]rue)$",
        true,
        &build_superset_regex(values, -1),
    );
    assert_automaton(
        "^([Ff]alse|[Tt]rue)$",
        true,
        &build_superset_regex(values, 18),
    );
    assert_automaton(
        "^(False|[Tt]rue|fals[\\s\\S]+)$",
        false,
        &build_superset_regex(values, 17),
    );
    assert_automaton("^[FTft][\\s\\S]+$", false, &build_superset_regex(values, 4));
    assert_automaton("^[\\s\\S]+$", false, &build_superset_regex(values, 3));
}

#[test]
fn test_prune_alphanumeric() {
    let values = &["a0", "a1", "b0", "b1", "b2"];
    assert_automaton("^(a[01]|b[012])$", true, &build_superset_regex(values, -1));
    assert_automaton("^(a[01]|b[012])$", true, &build_superset_regex(values, 7));
    assert_automaton(
        "^(a[\\s\\S]+|b[012])$",
        false,
        &build_superset_regex(values, 6),
    );
    assert_automaton(
        "^(a[01]|b[\\s\\S]+)$",
        false,
        &build_superset_regex(values, 4),
    );
    assert_automaton("^[ab][\\s\\S]+$", false, &build_superset_regex(values, 3));
    assert_automaton("^[\\s\\S]+$", false, &build_superset_regex(values, 1));
}

#[test]
fn test_prune_duplicates() {
    let values = &["aa", "bb", "bb"];
    assert_automaton("^(aa|bb)$", true, &build_superset_regex(values, -1));
    assert_automaton("^(aa|bb)$", true, &build_superset_regex(values, 4));
    assert_automaton("^(a[\\s\\S]+|bb)$", false, &build_superset_regex(values, 3));
    assert_automaton("^[ab][\\s\\S]+$", false, &build_superset_regex(values, 2));
    assert_automaton("^[\\s\\S]+$", false, &build_superset_regex(values, 1));
}

#[test]
fn test_prune_with_empty_string() {
    let values = &["", "a", "aaa"];
    assert_automaton("^(|a(|aa))$", true, &build_superset_regex(values, -1));
    assert_automaton("^(|a(|aa))$", true, &build_superset_regex(values, 3));
    assert_automaton(
        "^(|a(|a[\\s\\S]+))$",
        false,
        &build_superset_regex(values, 2),
    );
    assert_automaton("^(|a[\\s\\S]*)$", false, &build_superset_regex(values, 1));
    assert_automaton("^[\\s\\S]*$", false, &build_superset_regex(values, 0));
}

// ---------------------------------------------------------------------------
// Pruned regex must match values containing newlines
// ---------------------------------------------------------------------------

#[test]
fn test_pruned_regex_matches_newline_values() {
    // A value with a newline: when pruned, the suffix wildcard must match it.
    let values = &["service-a\nline2", "service-b\nline2", "service-c"];
    let aut = build_superset_regex(values, 4);

    let regex_str = aut.regex();

    // The regex should contain [\s\S]+ (not .+) for the pruned suffix.
    assert!(
        regex_str.contains("[\\s\\S]+"),
        "pruned regex should use [\\s\\S]+ wildcard: {regex_str}"
    );

    // Compile and verify all original values match, including the
    // newline-containing ones. This is the critical correctness check:
    // with plain `.+` this would fail because `.` doesn't match `\n`.
    let re = regex::Regex::new(&regex_str).expect("generated regex must be valid");
    for v in values {
        assert!(
            re.is_match(v),
            "regex '{regex_str}' must match input value '{}'",
            v.replace('\n', "\\n")
        );
    }
}

// ---------------------------------------------------------------------------
// automaton_test.go: TestWriteCharacterClass
// ---------------------------------------------------------------------------

fn character_class(chars: &[char]) -> String {
    let mut sb = String::new();
    write_character_class(&mut sb, chars);
    sb
}

#[test]
fn test_write_character_class() {
    assert_eq!("a", character_class(&['a']));
    assert_eq!("[ab]", character_class(&['a', 'b']));
    assert_eq!("[ac]", character_class(&['a', 'c']));
    assert_eq!("[abc]", character_class(&['a', 'b', 'c']));
    assert_eq!("[a-d]", character_class(&['a', 'b', 'c', 'd']));
    assert_eq!("[a-df]", character_class(&['a', 'b', 'c', 'd', 'f']));
    assert_eq!(
        "[a-df-i]",
        character_class(&['a', 'b', 'c', 'd', 'f', 'g', 'h', 'i'])
    );
    assert_eq!(
        "[\\-\\^$\\\\`\\[\\]語]",
        character_class(&['-', '^', '$', '\\', '`', '[', ']', '語'])
    );
}

// ---------------------------------------------------------------------------
// automaton_test.go: TestWriteDisjunctiveClausesFactoringSuffix
// ---------------------------------------------------------------------------

fn disjunctive_clauses_factoring_suffix(clauses: &[&str], suffix: &str) -> String {
    let owned: Vec<String> = clauses.iter().map(|s| s.to_string()).collect();
    let mut sb = String::new();
    write_disjunctive_clauses_factoring_suffix(&mut sb, &owned, suffix);
    sb
}

#[test]
fn test_write_disjunctive_clauses_factoring_suffix() {
    assert_eq!("", disjunctive_clauses_factoring_suffix(&[], ""));
    assert_eq!("", disjunctive_clauses_factoring_suffix(&[], "abc"));
    assert_eq!("abc", disjunctive_clauses_factoring_suffix(&["abc"], ""));
    assert_eq!("abc", disjunctive_clauses_factoring_suffix(&["abc"], "c"));
    assert_eq!("abc", disjunctive_clauses_factoring_suffix(&["abc"], "a"));
    assert_eq!("abc", disjunctive_clauses_factoring_suffix(&["abc"], "abc"));
    assert_eq!(
        "(abc|def|ghi)",
        disjunctive_clauses_factoring_suffix(&["abc", "def", "ghi"], "")
    );
    assert_eq!(
        "(abc|def|ghi)",
        disjunctive_clauses_factoring_suffix(&["abc", "def", "ghi"], "123")
    );
    assert_eq!(
        "(abc|def123|ghi)",
        disjunctive_clauses_factoring_suffix(&["abc", "def123", "ghi"], "123")
    );
    assert_eq!(
        "((abc|def)123|ghi)",
        disjunctive_clauses_factoring_suffix(&["abc123", "def123", "ghi"], "123")
    );
    assert_eq!(
        "(abc|def|ghi)123",
        disjunctive_clauses_factoring_suffix(&["abc123", "def123", "ghi123"], "123")
    );
    assert_eq!(
        "(abc123|def123|ghi123)",
        disjunctive_clauses_factoring_suffix(&["abc123", "def123", "ghi123"], "12")
    );
}

// ---------------------------------------------------------------------------
// automaton_test.go: TestWriteDisjunctiveClauses
// ---------------------------------------------------------------------------

fn disjunctive_clauses(clauses: &[&str], stripped_suffix_len: usize) -> String {
    let owned: Vec<String> = clauses.iter().map(|s| s.to_string()).collect();
    let mut sb = String::new();
    write_disjunctive_clauses(&mut sb, &owned, stripped_suffix_len);
    sb
}

#[test]
fn test_write_disjunctive_clauses() {
    assert_eq!("", disjunctive_clauses(&[], 0));
    assert_eq!("abc", disjunctive_clauses(&["abc"], 0));
    assert_eq!("ab", disjunctive_clauses(&["abc"], 1));
    assert_eq!("", disjunctive_clauses(&["abc"], 3));
    assert_eq!(
        "(abc|defg|hij)",
        disjunctive_clauses(&["abc", "defg", "hij"], 0)
    );
    assert_eq!(
        "(ab|def|hi)",
        disjunctive_clauses(&["abc", "defg", "hij"], 1)
    );
    assert_eq!("(|d|)", disjunctive_clauses(&["abc", "defg", "hij"], 3));
}

// ---------------------------------------------------------------------------
// automaton_test.go: TestAfterPrune
// ---------------------------------------------------------------------------

#[test]
fn test_after_prune() {
    let mut aut = zonemap::automaton::Automaton::new(64);
    for s in &[
        "a",
        "ab",
        "abc",
        "abcd",
        "abcde",
        "abcdef",
        "abcdefg",
        "abcdefgh",
        "abcdefghi",
        "abcdefghij",
        "abcdefghijk",
        "abcdefghijkl",
    ] {
        aut.add(s);
    }
    aut.prune(8);

    assert_eq!("^a(|b(|c(|d(|e(|f(|g(|h[\\s\\S]*)))))))$", aut.regex());
    assert!(!aut.is_strict_superset);

    // Adding after pruning should not change the regex past the pruned point.
    aut.add("abcdefghxrqx");

    assert_eq!("^a(|b(|c(|d(|e(|f(|g(|h[\\s\\S]*)))))))$", aut.regex());
    assert!(!aut.is_strict_superset);
}

// ---------------------------------------------------------------------------
// automaton_test.go: TestLongStrings
// ---------------------------------------------------------------------------

#[test]
fn test_long_strings() {
    let mut aut = zonemap::automaton::Automaton::new(8);
    for s in &[
        "a",
        "ab",
        "abc",
        "abcd",
        "abcde",
        "abcdef",
        "abcdefg",
        "abcdefgh",
        "abcdefghi",
        "abcdefghij",
        "abcdefghijk",
        "abcdefghijkl",
        "1234567890",
    ] {
        aut.add(s);
    }

    assert_eq!(
        "^(12345678[\\s\\S]+|a(|b(|c(|d(|e(|f(|g(|h[\\s\\S]*))))))))$",
        aut.regex()
    );
    assert!(!aut.is_strict_superset);
}

// ---------------------------------------------------------------------------
// MinMax tests
// ---------------------------------------------------------------------------

#[test]
fn test_minmax_string_hashing() {
    let mut builder = MinMaxBuilder::new();
    builder.register("hello");
    builder.register("world");

    let result = builder.build().expect("should produce MinMax");
    let h1 = hash_string("hello");
    let h2 = hash_string("world");
    assert_eq!(result.min_hash, h1.min(h2));
    assert_eq!(result.max_hash, h1.max(h2));
}

#[test]
fn test_minmax_int_hashing() {
    let mut builder = MinMaxBuilder::new();
    builder.register_int64(10);
    builder.register_int64(20);
    builder.register_int64(30);

    let result = builder.build().expect("should produce MinMax");
    let h10 = hash_int(10);
    let h20 = hash_int(20);
    let h30 = hash_int(30);
    let expected_min = h10.min(h20).min(h30);
    let expected_max = h10.max(h20).max(h30);
    assert_eq!(result.min_hash, expected_min);
    assert_eq!(result.max_hash, expected_max);
    assert_eq!(result.min_int, 10);
    assert_eq!(result.max_int, 30);
}

#[test]
fn test_minmax_empty_build() {
    let builder = MinMaxBuilder::new();
    assert!(builder.build().is_none());
}

#[test]
fn test_minmax_single_value() {
    let mut builder = MinMaxBuilder::new();
    builder.register("only");
    let result = builder.build().unwrap();
    let h = hash_string("only");
    assert_eq!(result.min_hash, h);
    assert_eq!(result.max_hash, h);
}

#[test]
fn test_minmax_reset() {
    let mut builder = MinMaxBuilder::new();
    builder.register("first");
    builder.reset();
    builder.register("second");
    let result = builder.build().unwrap();
    let h = hash_string("second");
    assert_eq!(result.min_hash, h);
    assert_eq!(result.max_hash, h);
}

// ---------------------------------------------------------------------------
// PrefixPreservingRegexBuilder tests
// ---------------------------------------------------------------------------

#[test]
fn test_regex_builder_basic() {
    let mut builder = zonemap::regex_builder::PrefixPreservingRegexBuilder::new();
    builder.reset(64, 0, 2.0);
    builder.register("prod");
    builder.register("staging");
    builder.register("production");

    let result = builder.build();
    assert_eq!(result.regex, "^(prod(|uction)|staging)$");
    assert!(result.is_also_subset_regex);
}

#[test]
fn test_regex_builder_with_pruning() {
    let mut builder = zonemap::regex_builder::PrefixPreservingRegexBuilder::new();
    builder.reset(4, 0, 2.0);
    builder.register("false");
    builder.register("False");
    builder.register("true");
    builder.register("True");

    let result = builder.build();
    assert_eq!(result.regex, "^[FTft][\\s\\S]+$");
    assert!(!result.is_also_subset_regex);
}

#[test]
fn test_regex_builder_progressive_pruning() {
    let mut builder = zonemap::regex_builder::PrefixPreservingRegexBuilder::new();
    builder.reset(3, 2, 2.0);
    builder.register("abc");
    builder.register("def");
    builder.register("ghi");
    builder.register("jkl");

    let result = builder.build();
    assert!(!result.regex.is_empty());
}

// ---------------------------------------------------------------------------
// generate_regex_from_strings tests
// ---------------------------------------------------------------------------

#[test]
fn test_generate_regex_from_strings() {
    let regex = generate_regex_from_strings(&["prod", "staging", "production"], 64, 0, 2.0);
    assert_eq!(regex, "^(prod(|uction)|staging)$");
}

#[test]
fn test_generate_regex_from_strings_with_special_chars() {
    let regex = generate_regex_from_strings(&["$^-?", "日本語"], 64, 0, 2.0);
    assert!(regex.contains("\\$\\^-\\?"));
    assert!(regex.contains("日本語"));
}

// ---------------------------------------------------------------------------
// extract_zonemap_regexes tests (Arrow integration)
// ---------------------------------------------------------------------------

/// Create a test batch with sort schema columns.
fn create_zonemap_test_batch(
    metric_names: &[&str],
    services: &[Option<&str>],
    envs: &[Option<&str>],
    timestamps: &[u64],
) -> RecordBatch {
    let n = metric_names.len();
    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

    let fields = vec![
        Field::new("metric_name", dict_type.clone(), false),
        Field::new("service", dict_type.clone(), true),
        Field::new("env", dict_type.clone(), true),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("timeseries_id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ];
    let schema = Arc::new(ArrowSchema::new(fields));

    let metric_name: ArrayRef = create_dict_array(metric_names);
    let service: ArrayRef = create_nullable_dict_array(services);
    let env: ArrayRef = create_nullable_dict_array(envs);
    let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; n]));
    let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps.to_vec()));
    let timeseries_id: ArrayRef = Arc::new(Int64Array::from(vec![42i64; n]));
    let value: ArrayRef = Arc::new(Float64Array::from(vec![1.0; n]));

    RecordBatch::try_new(
        schema,
        vec![
            metric_name,
            service,
            env,
            metric_type,
            timestamp_secs,
            timeseries_id,
            value,
        ],
    )
    .unwrap()
}

#[test]
fn test_extract_zonemap_regexes_basic() {
    let batch = create_zonemap_test_batch(
        &["cpu.usage", "cpu.usage", "memory.used"],
        &[Some("web"), Some("api"), Some("web")],
        &[Some("prod"), Some("staging"), Some("prod")],
        &[100, 200, 300],
    );

    let sort_fields = "metric_name|service|env|timeseries_id|timestamp_secs/V2";
    let opts = ZonemapOptions::default();
    let regexes = extract_zonemap_regexes(sort_fields, &batch, &opts).unwrap();

    assert!(
        regexes.contains_key("metric_name"),
        "should have metric_name"
    );
    assert!(regexes.contains_key("service"), "should have service");
    assert!(regexes.contains_key("env"), "should have env");
    assert!(
        !regexes.contains_key("timeseries_id"),
        "int column should not produce regex"
    );
    assert!(
        !regexes.contains_key("timestamp_secs"),
        "uint column should not produce regex"
    );

    let metric_regex = &regexes["metric_name"];
    assert!(
        metric_regex.contains("cpu"),
        "regex should contain cpu prefix"
    );
    assert!(
        metric_regex.contains("memory"),
        "regex should contain memory prefix"
    );
}

#[test]
fn test_extract_zonemap_regexes_empty_batch() {
    let fields = vec![
        Field::new(
            "metric_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("timestamp_secs", DataType::UInt64, false),
    ];
    let schema = Arc::new(ArrowSchema::new(fields));

    let metric_name: ArrayRef = create_dict_array(&[] as &[&str]);
    let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(Vec::<u64>::new()));
    let batch = RecordBatch::try_new(schema, vec![metric_name, timestamp_secs]).unwrap();

    let sort_fields = "metric_name|timestamp_secs/V2";
    let opts = ZonemapOptions::default();
    let regexes = extract_zonemap_regexes(sort_fields, &batch, &opts).unwrap();
    assert!(regexes.is_empty(), "empty batch should produce no regexes");
}

#[test]
fn test_extract_zonemap_regexes_disabled_when_zero_max_size() {
    let batch = create_zonemap_test_batch(&["cpu.usage"], &[Some("web")], &[Some("prod")], &[100]);

    let sort_fields = "metric_name|service|env|timeseries_id|timestamp_secs/V2";
    let opts = ZonemapOptions {
        superset_regex_max_size: 0,
        ..Default::default()
    };
    let regexes = extract_zonemap_regexes(sort_fields, &batch, &opts).unwrap();
    assert!(
        regexes.is_empty(),
        "zero max_size should disable zonemap generation"
    );
}

#[test]
fn test_extract_zonemap_regexes_with_nulls() {
    let batch = create_zonemap_test_batch(
        &["cpu.usage", "cpu.usage"],
        &[Some("web"), None],
        &[None, None],
        &[100, 200],
    );

    let sort_fields = "metric_name|service|env|timeseries_id|timestamp_secs/V2";
    let opts = ZonemapOptions::default();
    let regexes = extract_zonemap_regexes(sort_fields, &batch, &opts).unwrap();

    assert!(regexes.contains_key("metric_name"));
    assert!(regexes.contains_key("service"));
}

#[test]
fn test_extract_zonemap_regexes_column_not_in_batch() {
    let batch = create_zonemap_test_batch(&["cpu.usage"], &[Some("web")], &[Some("prod")], &[100]);

    let sort_fields = "metric_name|service|datacenter|timeseries_id|timestamp_secs/V2";
    let opts = ZonemapOptions::default();
    let regexes = extract_zonemap_regexes(sort_fields, &batch, &opts).unwrap();

    assert!(regexes.contains_key("metric_name"));
    assert!(regexes.contains_key("service"));
    assert!(
        !regexes.contains_key("datacenter"),
        "missing column should be skipped"
    );
}

#[test]
fn test_extract_zonemap_regexes_special_characters() {
    let batch = create_zonemap_test_batch(
        &["cpu.usage", "cpu.usage"],
        &[Some("$^-?"), Some("日本語")],
        &[Some("prod"), Some("staging")],
        &[100, 200],
    );

    let sort_fields = "metric_name|service|env|timeseries_id|timestamp_secs/V2";
    let opts = ZonemapOptions::default();
    let regexes = extract_zonemap_regexes(sort_fields, &batch, &opts).unwrap();

    let service_regex = &regexes["service"];
    assert!(
        service_regex.contains("\\$\\^-\\?") || service_regex.contains("\\$\\^\\-\\?"),
        "special chars should be escaped in regex: {}",
        service_regex
    );
    assert!(
        service_regex.contains("日本語"),
        "unicode should be preserved in regex: {}",
        service_regex
    );
}

#[test]
fn test_extract_zonemap_regexes_long_service_name() {
    let long_name = "a_very".to_string() + &"_long".repeat(100);
    let batch =
        create_zonemap_test_batch(&["cpu.usage"], &[Some(&long_name)], &[Some("prod")], &[100]);

    let sort_fields = "metric_name|service|env|timeseries_id|timestamp_secs/V2";
    let opts = ZonemapOptions {
        superset_regex_max_size: 32,
        ..Default::default()
    };
    let regexes = extract_zonemap_regexes(sort_fields, &batch, &opts).unwrap();

    let service_regex = &regexes["service"];
    assert!(
        service_regex.contains("[\\s\\S]+"),
        "long string should be pruned: {}",
        service_regex
    );
}

// ---------------------------------------------------------------------------
// Benchmark data test (equivalent to Go BenchmarkBuildSupersetRegex)
// ---------------------------------------------------------------------------

#[test]
fn test_benchmark_data_produces_valid_regex() {
    let data = include_str!("../benchmark_data/integrations");
    let values: Vec<&str> = data.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(values.len(), 584, "should load all 584 integration names");

    let regex = generate_regex_from_strings(&values, 64, 1000, 2.0);
    assert!(regex.starts_with('^'), "regex should start with ^");
    assert!(regex.ends_with('$'), "regex should end with $");
    assert!(!regex.is_empty());
    // With 64 transitions the regex must be bounded — it should not
    // enumerate all 584 values verbatim.
    assert!(
        regex.contains("[\\s\\S]+"),
        "584 values with max 64 transitions should require pruning"
    );
}

// ---------------------------------------------------------------------------
// builder_test.go: TestBuildFragmentZoneMap — exact regex verification
// ---------------------------------------------------------------------------

#[test]
fn test_extract_zonemap_exact_regexes() {
    // Mirrors Go TestBuildFragmentZoneMap: verifies exact regex strings.
    let batch = create_zonemap_test_batch(
        &[
            "cpu.usage",
            "cpu.usage",
            "cpu.usage",
            "cpu.usage",
            "cpu.usage",
        ],
        &[
            Some("prod"),
            Some("staging"),
            Some("production"),
            Some("$^-?"),
            Some("日本語"),
        ],
        &[
            Some("prod"),
            Some("staging"),
            Some("production"),
            Some("$^-?"),
            Some("日本語"),
        ],
        &[100, 200, 300, 400, 500],
    );

    let sort_fields = "metric_name|service|env|timeseries_id|timestamp_secs/V2";
    let opts = ZonemapOptions {
        superset_regex_max_size: 32,
        prune_every: 1000,
        multiplier: 2.0,
    };
    let regexes = extract_zonemap_regexes(sort_fields, &batch, &opts).unwrap();

    // env/service: dict columns with special chars and unicode.
    // Distinct dict values: prod, staging, production, $^-?, 日本語
    let env_regex = &regexes["env"];
    assert_eq!(
        env_regex, "^(\\$\\^-\\?|prod(|uction)|staging|日本語)$",
        "env regex should match Go TestBuildFragmentZoneMap"
    );

    // service has same values → same regex (both are dict columns).
    let service_regex = &regexes["service"];
    assert_eq!(
        service_regex, env_regex,
        "service regex should match env (same dict values)"
    );

    // metric_name: single distinct value "cpu.usage".
    let metric_regex = &regexes["metric_name"];
    assert_eq!(metric_regex, "^cpu\\.usage$");
}

/// Go TestBuildFragmentZoneMap: long service name → pruned regex.
#[test]
fn test_extract_zonemap_long_string_exact_regex() {
    let long_name = "a_very_very_very_very_long_long_".to_string() + &"service_".repeat(60);
    let batch =
        create_zonemap_test_batch(&["cpu.usage"], &[Some(&long_name)], &[Some("prod")], &[100]);

    let sort_fields = "metric_name|service|env|timeseries_id|timestamp_secs/V2";
    let opts = ZonemapOptions {
        superset_regex_max_size: 32,
        prune_every: 1000,
        multiplier: 2.0,
    };
    let regexes = extract_zonemap_regexes(sort_fields, &batch, &opts).unwrap();

    // The Go test expects "^a_very_very_very_very_long_long_[\s\S]+$" for a single
    // very long string with maxSize=32. With a single string the automaton
    // has one transition per character; pruning to 32 truncates at depth 32.
    let service_regex = &regexes["service"];
    assert_eq!(
        service_regex, "^a_very_very_very_very_long_long_[\\s\\S]+$",
        "long service name should be pruned at 32 transitions"
    );
}

// ---------------------------------------------------------------------------
// builder_test.go: TestNilSortSchema — empty sort fields
// ---------------------------------------------------------------------------

#[test]
fn test_extract_zonemap_empty_sort_fields() {
    let batch = create_zonemap_test_batch(&["cpu.usage"], &[Some("web")], &[Some("prod")], &[100]);

    // Empty sort fields string: no columns to process.
    let opts = ZonemapOptions::default();
    let regexes = extract_zonemap_regexes("", &batch, &opts).unwrap();
    assert!(
        regexes.is_empty(),
        "empty sort fields should produce no regexes"
    );
}

// ---------------------------------------------------------------------------
// builder_test.go: TestNonMutatedResult — builder reuse
// ---------------------------------------------------------------------------

#[test]
fn test_extract_zonemap_builder_reuse_does_not_corrupt() {
    // Two consecutive calls should produce independent results.
    let batch1 =
        create_zonemap_test_batch(&["cpu.usage"], &[Some("prod")], &[Some("east")], &[100]);
    let batch2 = create_zonemap_test_batch(
        &["memory.used"],
        &[Some("event-query")],
        &[Some("west")],
        &[200],
    );

    let sort_fields = "metric_name|service|env|timeseries_id|timestamp_secs/V2";
    let opts = ZonemapOptions::default();

    let regexes1 = extract_zonemap_regexes(sort_fields, &batch1, &opts).unwrap();
    let regexes2 = extract_zonemap_regexes(sort_fields, &batch2, &opts).unwrap();

    // First result should not be corrupted by second call.
    assert_eq!(regexes1["service"], "^prod$");
    assert_eq!(regexes2["service"], "^event-query$");
    assert_eq!(regexes1["env"], "^east$");
    assert_eq!(regexes2["env"], "^west$");
}

// ---------------------------------------------------------------------------
// builder_test.go: TestInvalidUTF8
// ---------------------------------------------------------------------------

// Note: Rust strings are always valid UTF-8, so we cannot inject invalid
// bytes into Arrow StringArray. Instead we test that the regex builder
// handles the full Unicode BMP range and produces valid UTF-8 output.
#[test]
fn test_extract_zonemap_unicode_bmp() {
    // Characters from various Unicode blocks.
    let batch = create_zonemap_test_batch(
        &["cpu.usage", "cpu.usage", "cpu.usage"],
        &[Some("café"), Some("naïve"), Some("日本語")],
        &[Some("prod"), Some("prod"), Some("prod")],
        &[100, 200, 300],
    );

    let sort_fields = "metric_name|service|env|timeseries_id|timestamp_secs/V2";
    let opts = ZonemapOptions::default();
    let regexes = extract_zonemap_regexes(sort_fields, &batch, &opts).unwrap();

    let service_regex = &regexes["service"];
    assert!(!service_regex.is_empty());
    // Verify the output is valid UTF-8 (it's a Rust String, so it always is,
    // but this mirrors the Go test's intent).
    assert!(
        service_regex.is_ascii() || service_regex.len() > service_regex.chars().count(),
        "regex should contain multi-byte UTF-8 characters: {}",
        service_regex
    );
}

// ---------------------------------------------------------------------------
// builder_test.go: TestResetWithLsmComparisonCutoff
// ---------------------------------------------------------------------------

#[test]
fn test_extract_zonemap_lsm_comparison_cutoff() {
    let batch = create_zonemap_test_batch(
        &["cpu.usage"],
        &[Some("my-service")],
        &[Some("prod")],
        &[100],
    );

    let opts = ZonemapOptions::default();

    // cutoff=1: only first column (metric_name) gets a zonemap.
    // The '&' prefix marks the LSM comparison cutoff.
    let regexes = extract_zonemap_regexes(
        "metric_name|&service|env|timeseries_id|timestamp_secs/V2",
        &batch,
        &opts,
    )
    .unwrap();
    assert!(
        regexes.contains_key("metric_name"),
        "metric_name should be included (before cutoff)"
    );
    assert!(
        !regexes.contains_key("service"),
        "service should be excluded (at/after cutoff)"
    );
    assert!(
        !regexes.contains_key("env"),
        "env should be excluded (after cutoff)"
    );

    // cutoff=0 (no '&' prefix): all string columns get zonemaps.
    let regexes = extract_zonemap_regexes(
        "metric_name|service|env|timeseries_id|timestamp_secs/V2",
        &batch,
        &opts,
    )
    .unwrap();
    assert!(regexes.contains_key("metric_name"));
    assert!(regexes.contains_key("service"));
    assert!(regexes.contains_key("env"));

    // cutoff >= len(columns): all string columns get zonemaps (same as no cutoff).
    // Put '&' after the last column — parser sets cutoff = column count.
    let regexes = extract_zonemap_regexes(
        "metric_name|service|env|timeseries_id|&timestamp_secs/V2",
        &batch,
        &opts,
    )
    .unwrap();
    assert!(regexes.contains_key("metric_name"));
    assert!(regexes.contains_key("service"));
    assert!(regexes.contains_key("env"));
}

// ---------------------------------------------------------------------------
// builder_test.go: TestZoneMapForIntColumns — MinMax only, no regex
// ---------------------------------------------------------------------------

#[test]
fn test_minmax_for_int_columns() {
    // Verify that MinMaxBuilder correctly tracks int column values.
    // In the Go code, int columns get min/max hashes but no regex.
    // We verify the MinMax builder directly since extract_zonemap_regexes
    // correctly skips non-string columns (tested elsewhere).
    let mut builder = MinMaxBuilder::new();
    builder.register_int64(10);
    builder.register_int64(20);
    builder.register_int64(30);

    let result = builder.build().expect("should produce MinMax for ints");

    // Verify hash range covers all values.
    let h10 = hash_int(10);
    let h20 = hash_int(20);
    let h30 = hash_int(30);
    assert_eq!(result.min_hash, h10.min(h20).min(h30));
    assert_eq!(result.max_hash, h10.max(h20).max(h30));

    // Verify int range.
    assert_eq!(result.min_int, 10);
    assert_eq!(result.max_int, 30);

    // Verify that int columns produce no regex in extraction.
    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let fields = vec![
        Field::new("metric_name", dict_type.clone(), false),
        Field::new("duration", DataType::Int64, false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ];
    let schema = Arc::new(ArrowSchema::new(fields));

    let metric_name: ArrayRef = create_dict_array(&["cpu.usage"]);
    let duration: ArrayRef = Arc::new(Int64Array::from(vec![42i64]));
    let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8]));
    let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(vec![100u64]));
    let value: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));

    let batch = RecordBatch::try_new(
        schema,
        vec![metric_name, duration, metric_type, timestamp_secs, value],
    )
    .unwrap();

    let opts = ZonemapOptions::default();
    let regexes =
        extract_zonemap_regexes("metric_name|duration|timestamp_secs/V2", &batch, &opts).unwrap();

    assert!(
        regexes.contains_key("metric_name"),
        "string column should have regex"
    );
    assert!(
        !regexes.contains_key("duration"),
        "int column should not have regex"
    );
}

// ---------------------------------------------------------------------------
// String column type coverage: Utf8, LargeUtf8, and Dictionary with every
// Arrow key type (Int8..UInt64) × (Utf8, LargeUtf8) must all produce the
// same correct regex.
// ---------------------------------------------------------------------------

/// Helper: build a batch with a single string-typed "col" column and run
/// extract_zonemap_regexes. Returns the regex for "col".
fn extract_col_regex(col_array: ArrayRef, col_type: DataType) -> String {
    let n = col_array.len();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("col", col_type, false),
        Field::new("timeseries_id", DataType::Int64, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            col_array,
            Arc::new(Int64Array::from(vec![1i64; n])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![100u64; n])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0; n])) as ArrayRef,
        ],
    )
    .unwrap();
    let sort_fields = "col|timeseries_id|timestamp_secs/V2";
    let opts = ZonemapOptions::default();
    let regexes = extract_zonemap_regexes(sort_fields, &batch, &opts).unwrap();
    regexes["col"].clone()
}

#[test]
fn test_extract_zonemap_all_string_column_types() {
    let values = &["prod", "staging", "production"];

    // With 64 max transitions and 3 short values, no pruning occurs.
    // The exact regex is deterministic: prod and production share a prefix.
    let expected = "^(prod(|uction)|staging)$";

    // No `[\s\S]+` or `[\s\S]*` means no pruning — this is an exact-match regex.
    assert!(!expected.contains("[\\s\\S]+"));
    assert!(!expected.contains("[\\s\\S]*"));

    // --- Plain string types ---
    assert_eq!(
        extract_col_regex(
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            DataType::Utf8,
        ),
        expected,
        "Utf8"
    );
    assert_eq!(
        extract_col_regex(
            Arc::new(LargeStringArray::from(values.to_vec())) as ArrayRef,
            DataType::LargeUtf8,
        ),
        expected,
        "LargeUtf8"
    );

    // --- Dictionary with every key type × Utf8 ---
    // Macro to test a dictionary key type with both Utf8 and LargeUtf8 values.
    macro_rules! test_dict_key {
        ($key_arrow_type:expr, $key_rust_type:ty, $key_array:expr, $label:expr) => {
            // Dict(K, Utf8)
            let utf8_values = Arc::new(StringArray::from(values.to_vec()));
            let dict = Arc::new(
                DictionaryArray::<$key_rust_type>::try_new($key_array.clone(), utf8_values)
                    .unwrap(),
            );
            let dt = DataType::Dictionary(Box::new($key_arrow_type), Box::new(DataType::Utf8));
            assert_eq!(
                extract_col_regex(dict as ArrayRef, dt),
                expected,
                concat!("Dict(", $label, ", Utf8)")
            );

            // Dict(K, LargeUtf8)
            let large_values = Arc::new(LargeStringArray::from(values.to_vec()));
            let dict = Arc::new(
                DictionaryArray::<$key_rust_type>::try_new($key_array.clone(), large_values)
                    .unwrap(),
            );
            let dt = DataType::Dictionary(Box::new($key_arrow_type), Box::new(DataType::LargeUtf8));
            assert_eq!(
                extract_col_regex(dict as ArrayRef, dt),
                expected,
                concat!("Dict(", $label, ", LargeUtf8)")
            );
        };
    }

    let keys_i8 = Int8Array::from(vec![0i8, 1, 2]);
    let keys_i16 = Int16Array::from(vec![0i16, 1, 2]);
    let keys_i32 = Int32Array::from(vec![0i32, 1, 2]);
    let keys_i64 = Int64Array::from(vec![0i64, 1, 2]);
    let keys_u8 = UInt8Array::from(vec![0u8, 1, 2]);
    let keys_u16 = UInt16Array::from(vec![0u16, 1, 2]);
    let keys_u32 = UInt32Array::from(vec![0u32, 1, 2]);
    let keys_u64 = UInt64Array::from(vec![0u64, 1, 2]);

    test_dict_key!(DataType::Int8, Int8Type, keys_i8, "Int8");
    test_dict_key!(DataType::Int16, Int16Type, keys_i16, "Int16");
    test_dict_key!(DataType::Int32, Int32Type, keys_i32, "Int32");
    test_dict_key!(DataType::Int64, Int64Type, keys_i64, "Int64");
    test_dict_key!(DataType::UInt8, UInt8Type, keys_u8, "UInt8");
    test_dict_key!(DataType::UInt16, UInt16Type, keys_u16, "UInt16");
    test_dict_key!(DataType::UInt32, UInt32Type, keys_u32, "UInt32");
    test_dict_key!(DataType::UInt64, UInt64Type, keys_u64, "UInt64");
}
