# Doc-Transformers

Replicate the functionality of log processing pipelines of datadog.
See [datadog documentation](https://docs.datadoghq.com/logs/log_configuration/processors/?tab=ui)

# TODO

- Log events can be submitted up to 18 hours in the past and two hours in the future. 
=> Dropping events would need to be done after execting the pipelines, since the pipeline can contain date remapping.

- [ ] check how to handle arrays in remove_nested_from_map
- [ ] check how to handle arrays in get_nested_values

### Filters
* Handle more than numerical ranges in filter.rs (compare fn)
* Do field normalization after JSON parsing
* Replicate percolation on `message`: uncommon default field matching behaviour (see commented test)

### Transformers
- [ ] arithmetic-processor
- [x] attribute-remapper
- [x] category-processor
- [x] date-remapper
- [ ] geo-ip-parser
- [x] grok-parser
  - [x] Add rulenames parsing in grok.rs
- [ ] lookup-processor
- [x] message-remapper
- [x] pipeline
- [x] service-remapper
- [x] status-remapper
- [x] string-builder-processor
- [x] trace-id-remapper
- [ ] url-parser
- [ ] user-agent-parser

# Integration Processor

Update integration yaml files in the integrations folder.

```
python fetch_integrations.py

```

Generate `integrations_map.json` from the integration yaml files. This will also print the unsupported processors for each integration.

```
cargo run --bin convert_integrations
```

## Run Integration Tests

```
cargo test --test integration_test -- --nocapture
```


# Bench

```bash
cargo bench -p quickwit-doc-transforms --bench processors_bench
...
Running Syslog Processor Benchmarks
10k messages
syslog_processor              Avg: 169.40 MB/s (+3.83%)    Median: 169.42 MB/s (+3.44%)    [165.44 MB/s .. 174.56 MB/s]    
preprocessing_pipeline        Avg: 117.19 MB/s (+2.10%)    Median: 117.17 MB/s (+1.87%)    [114.55 MB/s .. 119.05 MB/s]    

Running Go Integration (Grok Parser) Benchmarks
10k Go messages
go_integration_processor        Avg: 8.4369 MB/s (-1.56%)    Median: 8.5250 MB/s (-0.54%)    [7.8501 MB/s .. 8.7929 MB/s]    
```
