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

//! Generator of random JSON objects, used by differential tests comparing code paths that consume
//! JSON documents (for instance owned and borrowed JSON trees).
//!
//! Keys are drawn from a small vocabulary so objects regularly contain duplicate keys, and values
//! cover escapes, dates, IPs, numbers at the integer type boundaries, nested arrays and objects.

/// Deterministic generator of random JSON object documents.
pub struct RandomJsonDocs {
    rng: Rng,
}

impl RandomJsonDocs {
    /// The same seed always produces the same sequence of documents. `seed` must not be zero.
    pub fn new(seed: u64) -> Self {
        assert_ne!(seed, 0, "xorshift seed must not be zero");
        RandomJsonDocs { rng: Rng(seed) }
    }

    /// Returns a random JSON object, serialized. Keys may be duplicated.
    pub fn next_doc(&mut self) -> String {
        let mut json_doc = String::new();
        write_random_object(&mut self.rng, 0, &mut json_doc);
        json_doc
    }
}

/// Deterministic xorshift generator, to keep failures reproducible without a new dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.below(items.len() as u64) as usize]
    }
}

const KEYS: &[&str] = &[
    "timestamp",
    "service",
    "body",
    "count",
    "delta",
    "ratio",
    "flag",
    "ip",
    "payload",
    "hex_payload",
    "tags",
    "values",
    "attributes",
    "events",
    "resource",
    "host",
    "pid",
    "inner",
    "zone",
    "all_text",
    "unmapped",
    "a",
    "b",
    "é",
    "",
    "timestamp_nanos",
    "service_name",
    "severity_text",
    "k.with.dots",
];

const STRINGS: &[&str] = &[
    "",
    "text",
    "2024-01-02T03:04:05Z",
    "2024-01-02T03:04:05.123+02:00",
    "2024-01-02 03:04:05",
    "1704164645",
    "-12",
    "1.5",
    "192.168.1.1",
    "::ffff:10.0.0.1",
    "aGVsbG8=",
    "deadbeef",
    "true",
    "9 lives",
    "esc\\\"aped\\n",
    "\\u00e9t\\u00e9",
    "\\ud83d\\ude00",
];

const NUMBERS: &[&str] = &[
    "0",
    "1",
    "-1",
    "42",
    "1704164645",
    "1704164645123",
    "-9223372036854775808",
    "9223372036854775808",
    "18446744073709551615",
    "18446744073709551616",
    "0.5",
    "-0.0",
    "1e3",
    "1.7976931348623157e308",
    "3.0",
];

fn write_random_value(rng: &mut Rng, depth: usize, output: &mut String) {
    let kind = if depth >= 3 {
        rng.below(5)
    } else {
        rng.below(8)
    };
    match kind {
        0 => output.push_str("null"),
        1 => output.push_str(if rng.below(2) == 0 { "true" } else { "false" }),
        2 | 3 => output.push_str(rng.pick(NUMBERS)),
        4 => {
            output.push('"');
            output.push_str(rng.pick(STRINGS));
            output.push('"');
        }
        5 => {
            output.push('[');
            let num_elements = rng.below(4);
            for i in 0..num_elements {
                if i > 0 {
                    output.push(',');
                }
                write_random_value(rng, depth + 1, output);
            }
            output.push(']');
        }
        _ => write_random_object(rng, depth + 1, output),
    }
}

/// Keys are drawn from a small vocabulary, so objects regularly contain duplicate keys.
fn write_random_object(rng: &mut Rng, depth: usize, output: &mut String) {
    output.push('{');
    let num_entries = rng.below(6);
    for i in 0..num_entries {
        if i > 0 {
            output.push(',');
        }
        output.push('"');
        output.push_str(rng.pick(KEYS));
        output.push_str("\":");
        write_random_value(rng, depth, output);
    }
    output.push('}');
}
