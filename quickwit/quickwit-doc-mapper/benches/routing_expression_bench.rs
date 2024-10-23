// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use binggan::plugins::*;
use binggan::{BenchRunner, PeakMemAlloc, INSTRUMENTED_SYSTEM};
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
            Some(())
        });

        group.run();
    }
}

fn main() {
    run_bench();
}
