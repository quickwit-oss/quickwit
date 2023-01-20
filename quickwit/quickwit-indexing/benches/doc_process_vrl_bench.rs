use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use quickwit_actors::{ActorHandle, Mailbox, Universe};
use quickwit_config::TransformConfig;
use quickwit_doc_mapper::DefaultDocMapper;
use quickwit_indexing::actors::DocProcessor;
use quickwit_indexing::models::RawDocBatch;
use quickwit_metastore::checkpoint::SourceCheckpointDelta;

const JSON_NORMAL: &str = include_str!("data/bench_data.json");
const JSON_LIGHT_TRANSFORM: &str = include_str!("data/bench_data_light_transform.json");
const JSON_HEAVY_TRANSFORM: &str = include_str!("data/bench_data_heavy_transform.json");

macro_rules! bench_func {
    ($input:expr, $group:expr, $name:expr, $param:expr, $func:expr) => {{
        let lines: Vec<&str> = $input.lines().map(|line| line.trim()).collect();
        $group.throughput(criterion::Throughput::Bytes($input.len() as u64));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let checkpoint_delta = SourceCheckpointDelta::from(0..$input.len() as u64);

        $group.bench_function(BenchmarkId::new($name, $param), |b| {
            b.to_async(&runtime).iter_batched(
                || {
                    lines
                        .iter()
                        .map(|line| line.to_string())
                        .collect::<Vec<_>>()
                },
                |docs| async {
                    let (mailbox, handle, universe) = $func;
                    mailbox
                        .send_message(RawDocBatch {
                            docs,
                            checkpoint_delta: checkpoint_delta.clone(),
                        })
                        .await
                        .unwrap();

                    universe.send_exit_with_success(&mailbox).await.unwrap();
                    handle.join().await;
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }};
}

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

fn doc_processor_no_transform() -> (Mailbox<DocProcessor>, ActorHandle<DocProcessor>, Universe) {
    create_doc_processor(None)
}

fn doc_processor_light_transform() -> (Mailbox<DocProcessor>, ActorHandle<DocProcessor>, Universe) {
    let vrl_script = r#"
        .last_name = "Doe"
        .job = upcase(string!(.job))
    "#;
    let transform_config = TransformConfig::for_test(vrl_script);
    create_doc_processor(Some(transform_config))
}

fn doc_processor_heavy_transform() -> (Mailbox<DocProcessor>, ActorHandle<DocProcessor>, Universe) {
    let vrl_script = r#"
        . = parse_json!(.body)
        .last_name = "Doe"
        .job = upcase(string!(.job))
        .timestamp = to_string(to_timestamp(now()))
    "#;
    let transform_config = TransformConfig::for_test(vrl_script);
    create_doc_processor(Some(transform_config))
}

fn create_doc_processor(
    transform_config_opt: Option<TransformConfig>,
) -> (Mailbox<DocProcessor>, ActorHandle<DocProcessor>, Universe) {
    let index_id = "my-index".to_string();
    let source_id = "my-source".to_string();
    let doc_mapper = Arc::new(default_doc_mapper_for_bench());
    let universe = Universe::new();
    let (indexer_mailbox, _) = universe.create_test_mailbox();
    let doc_processor = DocProcessor::try_new(
        index_id,
        source_id,
        doc_mapper,
        indexer_mailbox,
        transform_config_opt,
    )
    .unwrap();
    let (mailbox, handle) = universe.spawn_builder().spawn(doc_processor);
    (mailbox, handle, universe)
}

fn bench_simple_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("Simple Json");
    bench_func!(
        JSON_NORMAL,
        group,
        "No VRL",
        "Simple JSON",
        doc_processor_no_transform()
    );
    bench_func!(
        JSON_NORMAL,
        group,
        "Light VRL",
        "Simple JSON",
        doc_processor_light_transform()
    );
}

fn bench_light_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("Simple/Light Json");
    bench_func!(
        JSON_NORMAL,
        group,
        "No VRL",
        "Simple JSON",
        doc_processor_no_transform()
    );
    bench_func!(
        JSON_LIGHT_TRANSFORM,
        group,
        "Light VRL",
        "Light JSON",
        doc_processor_light_transform()
    );
}

fn bench_heavy_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("Simple/Light Json");
    bench_func!(
        JSON_NORMAL,
        group,
        "No VRL",
        "Simple JSON",
        doc_processor_no_transform()
    );
    bench_func!(
        JSON_HEAVY_TRANSFORM,
        group,
        "Heavy VRL",
        "Heavy JSON",
        doc_processor_heavy_transform()
    );
}

criterion_group!(
    benches,
    bench_simple_json,
    bench_light_json,
    bench_heavy_json
);
criterion_main!(benches);
