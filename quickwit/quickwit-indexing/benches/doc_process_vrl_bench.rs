use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use quickwit_actors::create_test_mailbox;
use quickwit_config::VrlSettings;
use quickwit_doc_mapper::DefaultDocMapper;
use quickwit_indexing::actors::DocProcessor;

const JSON_NORMAL: &str = include_str!("data/bench_data.json");
const JSON_LIGHT_TRANSFORM: &str = include_str!("data/bench_data_light_transform.json");
const JSON_HEAVY_TRANSFORM: &str = include_str!("data/bench_data_heavy_transform.json");

pub fn default_doc_mapper_for_bench() -> DefaultDocMapper {
    const JSON_CONFIG_VALUE: &str = r#"
        {
            "store_source": true,
            "default_search_fields": [],
            "timestamp_field": "timestamp",
            "tag_fields": ["id"],
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "datetime",
                    "output_format": "unix_timestamp_secs",
                    "fast": true,
                    "input_formats": ["iso8601"]
                },
                {
                    "name": "first_name",
                    "type": "text",
                    "stored": true
                },
                {
                    "name": "last_name",
                    "type": "text",
                    "stored": true
                },
                {
                    "name": "id",
                    "type": "u64",
                    "stored": true
                },
                {
                    "name": "email",
                    "type": "text",
                    "stored": true
                },
                {
                    "name": "job",
                    "type": "text",
                    "stored": true
                }
            ]
        }"#;
    serde_json::from_str::<DefaultDocMapper>(JSON_CONFIG_VALUE).unwrap()
}

fn doc_processor_no_transform() -> DocProcessor {
    create_doc_processor(None)
}

fn doc_processor_light_transform() -> DocProcessor {
    let source = r#"
        .last_name = "Doe"
        .job = upcase(string!(.job))
    "#;
    let vrl_settings = VrlSettings::for_test(source);
    create_doc_processor(Some(vrl_settings))
}

fn doc_processor_heavy_transform() -> DocProcessor {
    let source = r#"
        . = parse_json!(.body)
        .last_name = "Doe"
        .job = upcase(string!(.job))
        .timestamp = to_string(to_timestamp(now()))
    "#;
    let vrl_settings = VrlSettings::for_test(source);
    create_doc_processor(Some(vrl_settings))
}

fn create_doc_processor(vrl_settings: Option<VrlSettings>) -> DocProcessor {
    let index_id = "my-index";
    let source_id = "my-source";
    let doc_mapper = Arc::new(default_doc_mapper_for_bench());
    let (indexer_mailbox, _) = create_test_mailbox();

    DocProcessor::new(
        index_id.to_string(),
        source_id.to_string(),
        doc_mapper,
        indexer_mailbox,
        vrl_settings,
    )
    .unwrap()
}

fn bench_simple_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("Simple Json");
    let lines: Vec<&str> = JSON_NORMAL.lines().map(|line| line.trim()).collect();
    group.throughput(criterion::Throughput::Bytes(JSON_NORMAL.len() as u64));

    let mut doc_processor = doc_processor_no_transform();
    let mut doc_processor_light = doc_processor_light_transform();

    group.bench_function(BenchmarkId::new("No VRL", "Simple JSON"), |b| {
        b.iter(|| {
            for line in &lines {
                doc_processor._prepare_document(line).unwrap();
            }
        })
    });
    group.bench_function(BenchmarkId::new("Light VRL", "Simple JSON"), |b| {
        b.iter(|| {
            for line in &lines {
                doc_processor_light._prepare_document(line).unwrap();
            }
        })
    });
}

fn bench_light_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("Simple/Light Json");
    let normal_lines: Vec<&str> = JSON_NORMAL.lines().map(|line| line.trim()).collect();
    let transform_lines: Vec<&str> = JSON_LIGHT_TRANSFORM
        .lines()
        .map(|line| line.trim())
        .collect();

    let mut doc_processor = doc_processor_no_transform();
    let mut doc_processor_light = doc_processor_light_transform();

    group.throughput(criterion::Throughput::Bytes(JSON_NORMAL.len() as u64));

    group.bench_function(BenchmarkId::new("No VRL", "Simple JSON"), |b| {
        b.iter(|| {
            for line in &normal_lines {
                doc_processor._prepare_document(line).unwrap();
            }
        })
    });

    group.throughput(criterion::Throughput::Bytes(
        JSON_LIGHT_TRANSFORM.len() as u64
    ));

    group.bench_function(BenchmarkId::new("Light VRL", "Light JSON"), |b| {
        b.iter(|| {
            for line in &transform_lines {
                doc_processor_light._prepare_document(line).unwrap();
            }
        })
    });
}

fn bench_heavy_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("Simple/Light Json");
    let normal_lines: Vec<&str> = JSON_NORMAL.lines().map(|line| line.trim()).collect();
    let transform_lines: Vec<&str> = JSON_HEAVY_TRANSFORM
        .lines()
        .map(|line| line.trim())
        .collect();

    let mut doc_processor = doc_processor_no_transform();
    let mut doc_processor_heavy = doc_processor_heavy_transform();

    group.throughput(criterion::Throughput::Bytes(JSON_NORMAL.len() as u64));

    group.bench_function(BenchmarkId::new("No VRL", "Simple JSON"), |b| {
        b.iter(|| {
            for line in &normal_lines {
                doc_processor._prepare_document(line).unwrap();
            }
        })
    });

    group.throughput(criterion::Throughput::Bytes(
        JSON_HEAVY_TRANSFORM.len() as u64
    ));

    group.bench_function(BenchmarkId::new("Heavy VRL", "Heavy JSON"), |b| {
        b.iter(|| {
            for line in &transform_lines {
                doc_processor_heavy._prepare_document(line).unwrap();
            }
        })
    });
}

criterion_group!(
    benches,
    bench_simple_json,
    bench_light_json,
    bench_heavy_json
);
criterion_main!(benches);
