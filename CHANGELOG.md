# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Fixed

### Changed

### Deprecated

### Removed

### Security

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
