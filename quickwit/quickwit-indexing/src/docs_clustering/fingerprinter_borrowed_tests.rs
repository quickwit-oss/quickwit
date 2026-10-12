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

use quickwit_doc_mapper::borrowed_json::parse_json_object;

use super::Fingerprinter;

fn fingerprinter(methods: serde_json::Value) -> Fingerprinter {
    Fingerprinter::new(&serde_json::from_value(methods).unwrap())
}

#[test]
fn raw_scalar_fingerprints_keep_original_tag_and_number_encoding() {
    let printer =
        fingerprinter(serde_json::json!([{"fingerprint":[{"kind":"raw","path":"value"}]}]));
    // Independent golden FNV-1a streams from the original rule: FB, type tag,
    // canonical serde Number bytes (no number length), FD; absent is FA, FD.
    // No usize lengths occur here, so these goldens are architecture independent.
    for (raw, expected) in [
        (r#"{}"#, 768364047392506928),
        (r#"{"value":null}"#, 2001526898067639145),
        (r#"{"value":false}"#, 17434945461777302858),
        (r#"{"value":true}"#, 17433815163823691175),
        (r#"{"value":0}"#, 15546953154752163599),
        (r#"{"value":1}"#, 15548083452705775282),
        (r#"{"value":1.0}"#, 9090247521291406000),
        (r#"{"value":-0}"#, 3523228072621034834),
        (r#"{"value":-0.0}"#, 3523228072621034834),
        (r#"{"value":-1}"#, 11481566044756043061),
        (r#"{"value":18446744073709551615}"#, 6285135104459765740),
        (
            r#"{"value":{"invalid":"superseded"},"v\u0061lue":1.0}"#,
            9090247521291406000,
        ),
    ] {
        let owned: serde_json::Value = serde_json::from_str(raw).unwrap();
        let borrowed = parse_json_object(raw.as_bytes()).unwrap();
        assert_eq!(&*printer.fingerprint(&owned), &[expected], "owned: {raw}");
        assert_eq!(
            &*printer.fingerprint_json(borrowed.value()),
            &[expected],
            "borrowed: {raw}"
        );
    }
}

#[test]
fn structure_arrays_are_leaves_and_tokenized_values_remain_raw_strings() {
    let printer = fingerprinter(serde_json::json!([
        {"fingerprint":[{"kind":"structure","exclude":["timestamp"]}]},
        {"fingerprint":[{"kind":"tokenized","path":"body","max_tokens":3}]}
    ]));
    for raw in [
        r#"{"body":"server 12 tail","value":[],"timestamp":{"ignored":0}}"#,
        r#"{"body":"server 99 different tail","value":[{"must_not":"descend"}]}"#,
        r#"{"body":"bad","b\u006fdy":"server 42","value":[null,true]}"#,
    ] {
        let expected = [16756079140178559348, 14895941878402920949];
        let owned: serde_json::Value = serde_json::from_str(raw).unwrap();
        let borrowed = parse_json_object(raw.as_bytes()).unwrap();
        assert_eq!(&*printer.fingerprint(&owned), &expected);
        assert_eq!(&*printer.fingerprint_json(borrowed.value()), &expected);
    }
}

#[test]
fn native_fingerprint_views_match_for_nested_order_boundaries_and_token_limits() {
    let printer = fingerprinter(serde_json::json!([
        {"fingerprint":[{"kind":"raw","path":"value"},{"kind":"raw","path":"missing"}]},
        {"fingerprint":[{"kind":"raw","path":"missing"},{"kind":"raw","path":"value"}]},
        {"fingerprint":[{"kind":"structure","exclude":["value.exclude"]}]},
        {"fingerprint":[{"kind":"tokenized","path":"value.message","max_tokens":0}]},
        {"fingerprint":[{"kind":"tokenized","path":"value.message"}]}
    ]));
    for raw in [
        r#"{}"#,
        r#"{"value":null}"#,
        r#"{"value":[]}"#,
        r#"{"value":{}}"#,
        r#"{"value":{"z":1,"a":[null,true,-1,1.0,-0,18446744073709551615]}}"#,
        r#"{"value":{"escaped\nkey":"line\ntext","message":"2024-01-02T03:04:05Z"}}"#,
        r#"{"value":{"message":123,"exclude":[{"shape":true}]}}"#,
        r#"{"value":{"z":1,"a":2,"z":3},"v\u0061lue":{"message":"server 12"}}"#,
    ] {
        let owned: serde_json::Value = serde_json::from_str(raw).unwrap();
        let borrowed = parse_json_object(raw.as_bytes()).unwrap();
        assert_eq!(
            printer.fingerprint(&owned),
            printer.fingerprint_json(borrowed.value()),
            "{raw}"
        );
    }
    // Preserve the original distinction between missing/present field positions.
    let value: serde_json::Value = serde_json::json!({"value":null});
    let fingerprint = printer.fingerprint(&value);
    assert_ne!(fingerprint[0], fingerprint[1]);
}
