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

use std::io;

use serde::Serialize;
use serde_json::{Map, Value};

/// Returns the size in bytes of the serialized JSON.
/// This is done without actually serializing to a `Vec<u8>`, to avoid the allocation.
#[inline]
pub fn serialized_json_size<T: ?Sized + Serialize>(value: &T) -> serde_json::Result<usize> {
    struct CountWrite {
        bytes: usize,
    }
    impl io::Write for CountWrite {
        #[inline]
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes += buf.len();
            Ok(buf.len())
        }
        #[inline]
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut cw = CountWrite { bytes: 0 };
    let mut ser = serde_json::Serializer::new(&mut cw);
    value.serialize(&mut ser)?;
    Ok(cw.bytes)
}

/// Returns an approximate size in bytes of the serialized JSON value.
/// This approximation is done without any allocation and should be within 10% of the actual size
#[inline]
pub fn serialized_json_size_approx(v: &Value) -> usize {
    match v {
        Value::Null => 4,        // "null"
        Value::Bool(true) => 4,  // "true"
        Value::Bool(false) => 5, // "false"
        Value::Number(n) => approx_number_len(n),
        Value::String(s) => 2 + s.len(), // quotes + raw bytes (ignores escapes)
        Value::Array(a) => approx_array_len(a),
        Value::Object(m) => serialized_json_obj_approx(m),
    }
}

/// Returns an approximate size in bytes of the serialized JSON object.
/// This approximation is done without any allocation and should be within 10% of the actual size
#[inline]
pub fn serialized_json_obj_approx(obj: &Map<String, Value>) -> usize {
    if obj.is_empty() {
        return 2; // {}
    }
    // '{' + '}' + commas + each: quoted key + ':' + value
    let mut len = 3; // account for braces; add fields and commas below
    for (k, v) in obj {
        len += 2 + k.len(); // quotes + raw bytes (ignores escapes)
        len += 1; // colon
        len += serialized_json_size_approx(v);
    }
    len
}

#[inline]
fn approx_array_len(array: &[Value]) -> usize {
    if array.is_empty() {
        2 // []
    } else {
        // '[' + ']' + commas + elements
        2 + (array.len() - 1) + array.iter().map(serialized_json_size_approx).sum::<usize>()
    }
}

#[inline]
fn approx_number_len(val: &serde_json::Number) -> usize {
    if let Some(i) = val.as_i64() {
        decimal_len_i64(i)
    } else if let Some(u) = val.as_u64() {
        decimal_len_u64(u)
    } else if let Some(f) = val.as_f64() {
        approx_f64_len(f)
    } else {
        // can only happen with `arbitrary_precision` feature enabled in serde_json
        0
    }
}

#[inline]
fn decimal_len_i64(val: i64) -> usize {
    let neg = val < 0;
    let u = val.unsigned_abs();
    decimal_len_u64(u) + if neg { 1 } else { 0 }
}

#[inline]
fn decimal_len_u64(val: u64) -> usize {
    if val == 0 {
        1
    } else {
        val.ilog10() as usize + 1
    }
}

#[inline]
fn approx_f64_len(val: f64) -> usize {
    // Use ryu's formatting like serde_json does.
    // serde_json also ensures a decimal point or exponent is present for f64
    // values that would otherwise look like integers (e.g., 1 -> 1.0).
    let mut buf = ryu::Buffer::new();
    let s = buf.format(val);
    let mut len = s.len();
    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
        // Match serde_json's behavior for integer-valued floats by appending ".0".
        len += 2;
    }
    len
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{serialized_json_size, serialized_json_size_approx};

    fn assert_within_ten_percent(v: &Value) {
        let exact = serialized_json_size(v).expect("serialize via serde_json");
        let approx = serialized_json_size_approx(v);
        assert!(exact > 0, "exact size should be positive for JSON values");
        let rel = ((approx as isize - exact as isize).unsigned_abs() as f64) / exact as f64;
        assert!(
            rel <= 0.10,
            "approx differs >10%: value={v}, exact={exact}, approx={approx}, rel={rel}",
        );
    }

    #[test]
    fn approx_within_10pct_on_various_values() {
        // primitives
        assert_within_ten_percent(&json!(2.5));
        assert_within_ten_percent(&json!(1.2345678901234567));
        assert_within_ten_percent(&json!(1.2345));
        assert_within_ten_percent(&json!(2000000000000000.5));
        assert_within_ten_percent(&json!(null));
        assert_within_ten_percent(&json!(true));
        assert_within_ten_percent(&json!(false));
        assert_within_ten_percent(&json!(0));
        assert_within_ten_percent(&json!(1234567890123456789i64));
        assert_within_ten_percent(&json!("simple-string-no-escapes"));

        // arrays
        assert_within_ten_percent(&json!([]));
        assert_within_ten_percent(&json!([1, 2, 3, 4, 5]));
        assert_within_ten_percent(&json!(["a", "bb", "ccc", "dddd"]));

        // objects
        assert_within_ten_percent(&json!({}));
        assert_within_ten_percent(&json!({"a": 1, "b": 2, "c": 3}));

        // mixed nested (built to avoid heavy escaping so approx stays close)
        let v = json!({
            "id": 42,
            "ok": true,
            "name": "example",
            "vals": [1,2,3,4,5,6,7,8,9,10],
            "nested": {"x": [ {"k": 1}, {"k": 2} ], "y": null}
        });
        assert_within_ten_percent(&v);
    }
}
