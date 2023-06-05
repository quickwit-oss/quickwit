<!--
# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Fixed
- (Jaeger) Query resource attributes when Jaeger request carries tags

### Changed

### Deprecated

### Removed

### Security

--->

## [0.6.0] - 2023-06-03

### Added
- Elasticsearch/Opensearch compatible API.
- New columnar format:
    - Fast fields can now have any cardinality (Optional, Multivalued, restricted). In fact cardinality is now only used to format the output.
    - Dynamic Fields are now fast fields.
- String fast fields now can be normalized.
- Various parameters of object storages can now be configured.
- The ingest API makes it possible to force a commit, or wait for a scheduled commit to occur.
- Ability to parse non-JSON data using VRL to extract some structure from documents.
- Object storage can now use the `virtual-hosted–style`.
- `date_histogram` aggregation.
- `percentiles` aggregation.
- Added support for Prefix Phrase query.
- Added support for range queries.
- The query language now supports different date formats.
- Added support for base16 input/output configuration for bytes field. You can search for bytes fields using base16 encoded values.
- Autotagging: fields used in the partition key are automatically added to tags.
- Added arm64 docker image.
- Added CORS configuration for the REST API.


### Fixed
- Major bug fix that required to restart quickwit when deleting and recreating an index with the same name.
- The number of concurrent GET requests to object stores is now limited. This fixes a bug observed with when requested a lot of documents from MinIO.
- Quickwit now searches into resource attributes when receiving a Jaeger request carrying tags
- Object storage can be figured to:
    - avoid Bulk delete API (workaround for Google Cloud Storage).
    - Use virtual-host style addresses (workaround for Alibaba Object Storage Service).
- Fix aggregation min doc_count empty merge bug.
- Fix: Sort order for term aggregations.
- Switch to ms in histogram for date type (aligning with ES).

### Improvements

- Search performance improvement.
- Aggregation performance improvement.
- Aggregation memory improvement.

More details in tantivy's [changelog](https://github.com/quickwit-oss/tantivy/blob/main/CHANGELOG.md).

### Changed
- Datetime now have up to a nanosecond precision.
- By default, quickwit now uses the node's hostname as the default node ID.
- By default, Quickwit is in dynamic mode and all dynamic fields are marked as fast fields.
- JSON field uses by default the raw tokanizer and is set to fast field.
- Various performance/compression improvements.
- OTEL indexes Trace ID and Span ID are now bytes fields.
- OTEL indexes stores timestamps with nanosecond precision.
- pan status is now indexed in the OTEL trace index.
- Default and raw tokenizers filter tokesn longer than 255 bytes instead of 40 bytes.


## [0.5.0] - 2023-03-16

### Added
- gRPC OpenTelemetry Protocol support for traces
- gRPC OpenTelemetry Protocol support for logs
- Control plane (indexing tasks scheduling)
- Ingest API rate limiter
- Pulsar source
- VRL transform for data sources
- REST API enhanced to fully manage indexes, sources, and splits
- OpenAPI specification and swagger UI for all REST available endpoints
- Large responses from REST API can be compressed
- Add bulk stage splits method to metastore
- MacOS M1 binary
- Doc mapping field names starting with `_` are now valid

### Fixed
- Fix UI index completion on search page
- Fix CLI index describe command to show stats on published splits
- Fix REST API to always return on error a body formatted as `{"message": "error message"}`
- Fixed REST status code when deleting unexisting index, source and when fetching splits on unexisting index

### Changed
- Source config schema (breaking or not? use serde rename to be not breaking?)
- RocksDB replaced by [mrecordlog](https://github.com/quickwit-oss/mrecordlog) to store ingest API queues records
- (Breaking) Indexing partition key new DSL
- (Breaking) Helm chart updated with the new CLI
- (Breaking) CLI indexes, sources, and splits commands use the REST API
- (Breaking) Index new format: you need to reindex all your data

## [0.4.0] - 2022-12-03

### Added
- Boolean, datetime, and IP address fields
- Chinese tokenizer
- Distributed indexing (Kafka only)
- gRPC metastore server
- Index partitioning
- Kubernetes
- Node config templating
- Prometheus metrics
- Retention policies
- REST API for CRUD operations on indexes/sources
- Support for Azure Blob Storage
- Support for BM25 document scoring
- Support for deletions
- Support for slop in phrase queries
- Support for snippeting

### Fixed
- Fixed cache misses during search fetch docs phase
- Fixed credentials leak in metastore URI
- Fixed GC scalability issues
- Fixed support for multi-source

### Changed
- Changed default docstore block size to 1 MiB and compression algorithm to ZSTD

- Quickwit now relies on sqlx rather than Diesel for PostgreSQL interactions.
Migrating from 0.3 should work as expected. Migrating from earlier version however is
not supported.

### Removed
- Removed support for i64 as timestamp field
- Removed support for sorting index by field

### Security
- Forbid access to paths with `..` at storage level

## [0.3.1] - 2022-06-22

### Added
- Add support for Google Cloud Storage
- Sort hits by timestamp desc by default in search UI
- Add `description` attribute to field mappings
- Display split state in output of `quickwit split list` command

### Fixed
- Clean up local split cache after index deletion
- Fix API URLs displayed for copy and paste in UI
- Fix custom S3 endpoint with trailing `/`
- Fix `quickwit index create` command with `--overwrite` option

## [0.3.0] - 2022-05-31

### Added
- Embedded UI for displaying search hits and cluster state
- Schemaless indexing with JSON field
- Ingest API (Elasticsearch-compatible)
- Aggregation queries
- Support for Amazon Kinesis

### Fixed
- Switched cluster membership algorithm from S.W.I.M. to Chitchat

### Removed
- u64 as date field

## [0.2.1] - 2022-02-28

### Added
- Query validation against index schema before dispatch to leaf nodes (#1109, @linxGnu)
- Support for custom S3 endpoint (#1108)
- Warm up terms and fastfields concurrently (#1147)

### Fixed
- Minor bug in leaf search stream (#1110)
- Default index root URI and metastore URI correctly default to data dir (#1140, @ddelemeny)

### Removed
- QW_ENV environment variable

### Security
- Compiled binaries with Rust 1.58.1, which fixes CVE-2022-21658

## [0.2.0] - 2022-01-12

## [0.1.0] - 2021-07-13
