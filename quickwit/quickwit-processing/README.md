# Pomchi

Pomchi is a Rust library that tries to replicate Datadog-SaaS log processing pipelines. It provides:

- A Pipeline to apply processors (grok, remapping, string building, categorization).
- Utilities for importing Datadog integrations and validating support coverage.
- Pre-processing: tag flattening, JSON parsing and common field normalization (timestamp/host/service/message, etc.).
- A compatible JSON config format with Datadog-SaaS. (drop-in replacement for existing configs)

## Supported Processors

- [x] attribute-remapper
- [x] category-processor
- [x] date-remapper
- [x] grok-parser
- [x] message-remapper
- [x] pipeline
- [x] service-remapper
- [x] status-remapper
- [x] string-builder-processor
- [x] trace-id-remapper
- [x] span-id-remapper
- [x] user-agent-parser
- [ ] url-parser
- [ ] geo-ip-parser
- [ ] arithmetic-processor
- [ ] lookup-processor

## Quick Start

- Build: `cargo build`
- Run tests: `cargo test`
- Run integration tests: `cargo test --test integration_test -- --nocapture`

## Working With Integrations

The `integrations/` folder contains Datadog integration pipeline definitions (`*.yaml`) and associated tests.

1) Refresh integration YAMLs (requires Python 3 and Git, clones public Datadog repos):

   ```bash
   python fetch_integrations.py
   ```

2) Convert and validate support, generating `integrations_map.json` and printing unsupported processors per integration:

   ```bash
   cargo run --bin convert_integrations
   ```

3) Run the integration tests to see end-to-end parity numbers:

   ```bash
   cargo test --test integration_test -- --nocapture
   ```

## TODO

- Replace VRL matching with https://github.com/DataDog/event-percolation


# Bench

```bash
cargo bench --bench processors_bench
...
Running Syslog Processor Benchmarks
10k messages
syslog_processor              Avg: 169.40 MB/s (+3.83%)    Median: 169.42 MB/s (+3.44%)    [165.44 MB/s .. 174.56 MB/s]    
preprocessing_pipeline        Avg: 117.19 MB/s (+2.10%)    Median: 117.17 MB/s (+1.87%)    [114.55 MB/s .. 119.05 MB/s]    

Running Go Integration (Grok Parser) Benchmarks
10k Go messages
go_integration_processor        Avg: 8.4369 MB/s (-1.56%)    Median: 8.5250 MB/s (-0.54%)    [7.8501 MB/s .. 8.7929 MB/s]    
```
