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

use binggan::{InputGroup, black_box};
use quickwit_processing::DatadogLogMsg;
use quickwit_proto::metastore::IndexRoutingRule;
use quickwit_serve::datadog_api::{IndexRouter, custom_field_accessor, tag_accessor};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use serde::Deserialize;

/// Fixed seed for deterministic message generation across benchmark runs.
const RNG_SEED: u64 = 0xDEADBEEF_CAFEBABE;

/// Struct matching the JSON format for real-world filters.
#[derive(Deserialize)]
struct IndexRule {
    index_id: String,
    filter: String,
}

/// Load real-world production filters from the test fixtures JSON file.
fn load_real_world_filters() -> Vec<IndexRoutingRule> {
    let json_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../quickwit-datadog-log-router/tests/fixtures/large-index-routing-table.json"
    );
    let content = std::fs::read_to_string(json_path).expect(
        "Failed to read \
         ../quickwit-datadog-log-router/tests/fixtures/large-index-routing-table.json",
    );
    let rules: Vec<IndexRule> = serde_json::from_str(&content).expect("Failed to parse JSON");

    rules
        .into_iter()
        .map(|r| IndexRoutingRule {
            filter: r.filter,
            index_id: r.index_id,
        })
        .collect()
}

/// Generate synthetic messages with randomized attributes for benchmarking.
/// Uses a deterministic RNG seeded with `RNG_SEED` for reproducible benchmarks.
fn generate_synthetic_messages(count: usize) -> Vec<DatadogLogMsg> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);

    let services = [
        "trebuchet-api",
        "appsec-reducer-signal",
        "eventplatform-worker",
        "chrono-api",
        "shard-orchestrator",
        "dogweb",
        "event-query",
        "logs-event-store-api",
        "dd-sts",
        "zoltron",
        "kafka-consumer",
        "marlo",
        "rocky",
        "mindy",
        "delancie-crawler",
        "chatbot-rpc",
        "federated-querier",
        "sketch-api",
        "reese-worker",
        "k8s-audit",
        "azure",
        "my-app",
        "user-service",
        "payment-api",
    ];
    let hostnames = [
        "i-abc123.ec2.internal",
        "gke-prod-node-1",
        "ip-10-0-1-42",
        "worker-us-east-1a",
        "k8s-node-pool-0",
        "achntrl-test-host",
    ];
    let sources = [
        "cloudtrail",
        "browser",
        "gcp",
        "azure",
        "dd_debugger",
        "snmp-traps",
        "vpc",
        "argo-rollouts",
        "rapid",
        "android",
        "ios",
        "slack-audit-logs",
        "driveline",
        "logs-probe",
        "nginx",
        "docker",
    ];
    let statuses = ["info", "warn", "error", "debug"];
    let envs = [
        "prod", "staging", "sandbox", "dev", "enclave", "test", "ci", "perf",
    ];
    let datacenters = [
        "us1.prod.dog",
        "us1.staging.dog",
        "eu1.prod.dog",
        "ap1.prod.dog",
        "us3.prod.dog",
        "asia.prod.dog",
    ];
    let teams = [
        "logs",
        "ramen",
        "apm-trace-intake",
        "apm-trace-storage",
        "streams",
        "debugger",
        "profiling",
        "event-platform-io",
        "event-platform-query",
        "network-edge",
        "remote-config",
        "xpq",
        "ci-app",
        "dbm",
        "sre",
        "infosec",
        "network-device-monitoring",
        "error-tracking",
        "incident-management",
        "feature-flagging-and-experimentation",
        "unknown-team",
        "test-team",
        "generic-backend",
    ];
    let namespaces = [
        "kube-system",
        "default",
        "chrono",
        "sre",
        "logs-storage",
        "eventplatform-automation",
        "service-discovery",
        "courier",
        "production",
        "testing",
        "development",
    ];
    let workflow_types = [
        "flex_logs_ingest",
        "flex_logs_query",
        "metric_processing",
        "alert_evaluation",
    ];
    let event_types = ["dataset_observed", "dataset_created", "monitor_alert"];

    (0..count)
        .map(|_| {
            let service = services[rng.random_range(0..services.len())];
            let hostname = hostnames[rng.random_range(0..hostnames.len())];
            let source = sources[rng.random_range(0..sources.len())];
            let status = statuses[rng.random_range(0..statuses.len())];
            let env = envs[rng.random_range(0..envs.len())];
            let datacenter = datacenters[rng.random_range(0..datacenters.len())];
            let team = teams[rng.random_range(0..teams.len())];
            let namespace = namespaces[rng.random_range(0..namespaces.len())];
            let workflow_type = workflow_types[rng.random_range(0..workflow_types.len())];
            let event_type = event_types[rng.random_range(0..event_types.len())];
            let is_security_signal_stats = rng.random_bool(0.1);
            let is_flex_rate_limit = rng.random_bool(0.1);

            // Build message as a structured JSON object
            let message_obj = serde_json::json!({
                "text": format!("[{}] Processing request for {}", status.to_uppercase(), service),
                "WorkflowType": workflow_type,
                "event_type": event_type,
                "isSecuritySignalStats": is_security_signal_stats,
                "isFlexRateLimit": is_flex_rate_limit,
            });

            DatadogLogMsg {
                message: quickwit_processing::MessageValue::Obj(message_obj.as_object().unwrap().clone()),
                service: Some(service.to_string()),
                hostname: Some(hostname.to_string()),
                ddsource: Some(source.to_string()),
                status: Some(status.to_string()),
                ddtags: {
                    let mut tags = vec![
                        format!("env:{env}"),
                        format!("datacenter:{datacenter}"),
                        format!("team:{team}"),
                        format!("kube_namespace:{namespace}"),
                    ];
                    // Pad to ~100 tags to match real-world volume
                    for i in 0..(100 + rng.random_range(0..10usize)) {
                        tags.push(format!("tag_{}:{:08x}", i, rng.random::<u32>()));
                    }
                    // Shuffle so relevant tags aren't always first
                    tags.shuffle(&mut rng);
                    tags
                },
                timestamp: None,
            }
        })
        .collect()
}

/// Displays the message distribution as an ASCII histogram.
fn print_distribution_histogram() {
    let rules = load_real_world_filters();
    let pairs: Vec<(&str, &str)> = rules
        .iter()
        .map(|r| (r.filter.as_str(), r.index_id.as_str()))
        .collect();
    let router = IndexRouter::for_test(&pairs);
    let messages = generate_synthetic_messages(10_000);

    // Count distribution
    let guard = router.get_router();
    let mut distribution = vec![0usize; rules.len()];
    for msg in messages.iter() {
        let index_id = guard.resolve_index(&tag_accessor(msg), &custom_field_accessor(msg));
        if let Some(i) = rules
            .iter()
            .position(|r| index_id == Some(r.index_id.as_str()))
        {
            distribution[i] += 1;
        }
    }

    let max_count = distribution.iter().copied().max().unwrap_or(1);
    let height = 15;
    let num_chars = (distribution.len() + 1) / 2;

    let matched_count = distribution.iter().filter(|&&c| c > 0).count();
    println!(
        "  Distribution histogram ({} indexes matched / {} total):",
        matched_count,
        distribution.len()
    );
    println!();

    // Print rows from top to bottom
    // Use ▌ (left half), ▐ (right half), █ (both), ' ' (none)
    for row in (0..height).rev() {
        let threshold = ((row + 1) as f64 / height as f64) * max_count as f64;
        print!("    ");
        for i in 0..num_chars {
            let left = distribution.get(i * 2).copied().unwrap_or(0);
            let right = distribution.get(i * 2 + 1).copied().unwrap_or(0);
            match (left as f64 >= threshold, right as f64 >= threshold) {
                (true, true) => print!("█"),
                (true, false) => print!("▌"),
                (false, true) => print!("▐"),
                (false, false) => print!(" "),
            }
        }
        println!();
    }
    // Print baseline
    print!("    ");
    for _ in 0..num_chars {
        print!("─");
    }
    println!();
    println!(
        "    {} rules, max matches for one index={} messages ({:.2}%)",
        distribution.len(),
        max_count,
        (max_count as f64 / messages.len() as f64 * 100.0)
    );
    println!();
}

fn main() {
    println!("Generating test data...");
    let messages_100k = generate_synthetic_messages(100_000);
    let messages_10k = messages_100k[..10_000].to_vec();

    let mut runner = InputGroup::new_with_inputs(vec![
        ("10k messages", messages_10k),
        ("100k messages", messages_100k),
    ]);
    runner.throughput(|msgs| msgs.len());

    {
        let router =
            IndexRouter::for_test(&[("service:dogweb", "dogweb-index"), ("*", "catch-all")]);
        runner.register(
            "simple_service_match",
            move |messages: &Vec<DatadogLogMsg>| {
                let guard = router.get_router();
                for msg in messages.iter() {
                    black_box(guard.resolve_index(&tag_accessor(msg), &custom_field_accessor(msg)));
                }
            },
        );
    }

    {
        let router = IndexRouter::for_test(&[("datacenter:us1*", "us1-prod"), ("*", "catch-all")]);
        runner.register("simple_tag_lookup", move |messages: &Vec<DatadogLogMsg>| {
            let guard = router.get_router();
            for msg in messages.iter() {
                black_box(guard.resolve_index(&tag_accessor(msg), &custom_field_accessor(msg)));
            }
        });
    }

    {
        let router = IndexRouter::for_test(&[
            (
                "(service:dogw* OR service:logs-backend) AND env:prod",
                "prod-index",
            ),
            ("team:(ramen OR str* OR logs)", "my-team-index"),
            ("*", "catch-all"),
        ]);
        runner.register(
            "realistic_3_indexes_table",
            move |messages: &Vec<DatadogLogMsg>| {
                let guard = router.get_router();
                for msg in messages.iter() {
                    black_box(guard.resolve_index(&tag_accessor(msg), &custom_field_accessor(msg)));
                }
            },
        );
    }

    {
        let real_world_rules = load_real_world_filters();
        let pairs: Vec<(&str, &str)> = real_world_rules
            .iter()
            .map(|r| (r.filter.as_str(), r.index_id.as_str()))
            .collect();
        let router = IndexRouter::for_test(&pairs);
        runner.register(
            "real_world_comically_large_table",
            move |messages: &Vec<DatadogLogMsg>| {
                let guard = router.get_router();
                for msg in messages.iter() {
                    black_box(guard.resolve_index(&tag_accessor(msg), &custom_field_accessor(msg)));
                }
            },
        );
    }

    runner.run();
    println!("(throughput is in messages/s, e.g., 20 MB/s = 20_000_000 messages/s)");

    println!("\nreal_world_filters message distribution across indexes");
    print_distribution_histogram();
}
