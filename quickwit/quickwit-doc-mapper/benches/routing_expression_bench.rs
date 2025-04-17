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

use binggan::plugins::*;
use binggan::{BenchRunner, INSTRUMENTED_SYSTEM, PeakMemAlloc};
use quickwit_doc_mapper::RoutingExpr;
use serde_json::Value as JsonValue;

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

const JSON_TEST_DATA: &str = include_str!("data/simple-routing-expression-bench.json");

fn run_bench() {
    let json_lines: Vec<serde_json::Map<String, JsonValue>> = JSON_TEST_DATA
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    let mut runner: BenchRunner = BenchRunner::new();

    runner
        .add_plugin(CacheTrasher::default())
        .add_plugin(PeakMemAllocPlugin::new(GLOBAL));

    {
        let (input_name, size, data) = &("routing_expr", JSON_TEST_DATA.len(), &json_lines);
        let mut group = runner.new_group();
        group.set_name(input_name);
        group.set_input_size(*size);
        group.register_with_input("simple-eval-hash", data, |lines| {
            let routing_expr = RoutingExpr::new("seller.id").unwrap();
            for json in lines.iter() {
                routing_expr.eval_hash(json);
            }
        });

        group.run();
    }
}

fn main() {
    run_bench();
}
