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

# [0.9.0]

### Added
- Add Ingest V2 (#5600, #5566, #5463, #5375, #5350, #5252 #5202)
- Add SQS source (#5374, #5335, #5148)
- Disable control plane check for searcher (#5599, #5360)
- Partially implement `_elastic/_cluster/health` (#5595)
- Make Jaeger span attribute-to-tag conversion exhaustive (#5574)
- Use `content_length_limit` for ES bulk limit (#5573)
- Limit and monitor warmup memory usage (#5568)
- Add eviction metrics to caches (#5523)
- Record object storage request latencies (#5521)
- Add some kind of throttling on the janitor to prevent it from overloading (#5510)
- Prevent single split searches from different `leaf_search` from interleaving (#5509)
- Retry on S3 internal error (#5504)
- Allow specifying OTEL index ID in header (#5503)
- Add a metric to count storage errors and their error code (#5497)
- Add support for concatenated fields (#4773, #5369, #5331) 
- Add number of splits per root/leaf search histograms (#5472)
- Introduce a searcher config option to timeout get requests (#5467)
- Add fingerprint to task in cluster state (#5464)
- Enrich root/leaf search spans with number of docs and splits (#5450)
- Add some additional search metrics (#5447)
- Improve GC resilience and add metrics (#5420)
- Enable force shutdown with 2nd Ctrl+C (#5414)
- Add request_timeout_secs config to searcher config (#5402)
- Memoize S3 client (#5377)
- Add more env var config for Postgres (#5365)
- Enable str fast field range queries (#5324)
- Allow querying non-existing fields (#5308)
- Support updating doc mapper through api (#5253) 
- Add optional special handling for hex in code tokenizer (#5200)
- Added a circuit breaker layer (#5134)
- Various performance optimizations in Tantivy (https://github.com/quickwit-oss/tantivy/blob/main/CHANGELOG.md)

### Changed
- Parse datetimes and timestamps with leading and/or trailing whitespace (#5544)
- Restrict maturity period to retention (#5543)
- Wait for merge at end of local ingest (#5542)
- Log PostgreSQL metastore error (#5530)
- Update azure multipart policy (#5553)
- Stop relying on our own version of pulsar-rs (#5487)
- Handle nested OTLP values in attributes and log bodies (#5485)
- Improve merge pipeline finalization (#5475)
- Allow failed splits in root search (#5440)
- Batch delete from GC (#5404, #5380)
- Make some S3 errors retryable (#5384)
- Change default timestamps in OTEL logs (#5366)
- Only return root spans for Jaeger HTTP API (#5358)
- Share aggregation limit on node (#5357)

### Fixed
- Fix existence queries for nested fields (#5581)
- Fix lenient option with wildcard queries (#5575)
- Fix incompatible ES Java date format (#5462)
- Fix bulk api response order (#5434)
- Fix pulsar finalize (#5471)
- Fix pulsar URI scheme (#5470)
- Fix grafana searchers dashboard (#5455)
- Fix jaeger http endpoint (#5378)
- Fix file re-ingestion after EOF (#5330)
- Fix source path in Lambda distrib (#5327)
- Fix configuration interpolation (#5403)
- Fix jaeger duration parse error (#5518)
- Fix unit conversion in jaeger http search endpoint (#5519)

### Removed
- Remove support for 2-digit years in java datetime parser (#5596)
- Remove DocMapper trait (#5508)


# [0.8.1]

### Fixed

- Bug in the chitchat digest message serialization (chitchat#144)

## [0.8.0]

### Added

- Remove some noisy logs (#4447)
- Add `/{index}/_stats` and `/_stats` ES API (#4442)
- Use `search_after` in ES scroll API (#4280)
- Add support for wildcard exclusion in index patterns (#4458)
- Add `.` support in DSL indentifiers (#3989)
- Add cat indices ES API (#4465)
- Limit concurrent merges (#4473)
- Add Index Template API and auto create index (#4456) (only available with ingest V2)
- Add support for compressed ES `_bulk` requests (#4506)
- Add support for slash `/` character in field names (#4510)
- Handle SIGTERM shutdown signal (#4539)
- Add `start_timestamp` and `end_timestamp` filter to ES `_field_caps` API (#4547)
- Limit the number of merge pipelines that can be spawned concurrently (#4574)
- Add support for `_source_excludes` and `_source_includes` query parameters in ES API (#4572)
- Add gRPC metrics layer to clients and servers (#4591)
- Add additional cluster metrics (#4597)
- Add index patterns query param on GET `/indexes` endpoint (#4600)
- Add support for GCS file backed metastore (#4604)
- Add default search fields for OTEL traces index (#4602)
- Add support for delete index in ES API (#4606)
- Add a handler to dynamically change the log level (#4662)
- Add REST endpoint to parse a query into a query AST (#4652)
- Add postgresql index and use `IN` instead of many `OR` (#4670)
- Add support for `_source_excludes`, `_source_includes`, `extra_filters` in `_msearch` ES API (#4696)
- Handle `track_total_size` on request ES body (#4710)
- Add a metric for the number number of indexes (#4711)
- Add various performance optimizations in Quickwit and Tantivy

More details in tantivy's [changelog](https://github.com/quickwit-oss/tantivy/blob/main/CHANGELOG.md).

### Fixed

- Fix aggregation result on empty index (#4449)
- Fix Gzip file source (#4457)
- Rate limit noisy logs (#4483)
- Prevent the exponential backoff from overflowing after 64 attempts (#4501)
- Remove field presence in ES `_field_caps` API (#4492)
- Remove `source` in ES parameter, remove unsupported field `fields` in response (#4590)
- Fix aggregation `split_size` parameter, add docs and test (#4627)
- Various fixes in chitchat (gossip): more details in [chitchat commit history](https://github.com/quickwit-oss/chitchat/commits/main/?since=2024-01-08&until=2024-03-13)
- Various fixes in mrecordlog (WAL): more details in [mrecordlog commit history](https://github.com/quickwit-oss/mrecordlog/commits/main/?since=2024-01-08&until=2024-03-13)

### Changed

- (Breaking) [Add ZSTD compression to chitchat's Deltas](https://github.com/quickwit-oss/chitchat/pull/112)

### Removed

### Migration from 0.7.x to 0.8.0

To deploy Quickwit 0.8.0, you must either:
- **shutdown down** your cluster **entirely** before deploying, or
- **restart all** the nodes of your cluster after deploying.

Because we made some breaking changes in the gossip protocol (chitchat), nodes running different versions of Quickwit cannot communicate with each other and crash upon receiving messages that do not match their release version. The new protocol is now versioned, and future updates of the gossip protocol will be backward compatible.


## [0.7.1]

### Added

- Add es _count API (#4410)
- Add _elastic/_field_caps API (#4350)
- Make gRPC message size configurable (#4388)
- Add API endpoint to get some control-plan internal info (#4339)
- Add Google Cloud Storage Implementation available for storage paths starting with `gs://` (#4344)

### Changed

- Return 404 on index not found in ES Bulk API (#4425)
- Allow $ and @ characters in field names (#4413)

### Fixed
- Assign all sources/shards, even if this requires exceeding the indexer #4363
- Fix traces doc mapping (service name set as  fast) and update default otel logs index ID to `otel-logs-v0_7` (#4401)
- Fix parsing multi-line queries (#4409)
- Fix range query for optional fast field panics with Index out of bounds (#4362)

### Migration from 0.7.0 to 0.7.1

Quickwit 0.7.1 will create the new index `otel-logs-v0_7` which is now used by default when ingesting data with the OTEL gRPC and HTTP API.

In the traces index `otel-traces-v0_7`, the `service_name` field is now fast. No migration is done if `otel-traces-v0_7` already exists. If you want `service_name` field to be fast, you have to delete first the existing `otel-traces-v0_7` index or create your own index.

## [0.7.0]

### Added

- Elasticsearch-compatible API
  - Added scroll and search_after APIs and support for multi-index search queries
  - Added exists, multi-match, match phrase prefix, match bool prefix, bool queries
  - Added `_field_caps` API
- Added support for OTLP over HTTP API (Protobuf only) (#4335)
- Added Jaeger REST endpoints for Grafana tracing support (#4197)
- Added support for injecting custom HTTP headers and moved REST config parameters into REST config section (#4198)
- Added support for OTLP trace data in arbitrary sources
- Commit Kafka offsets on suggest truncate (#3638)
- Honor `auto.offset.reset` parameter in Kafka source (#4095)
- Added exact count optimization (#4019)
- Added stream splits gRPC (#4109)
- Adding a split cache in Searchers (#3857)
- Added `coerce` and `output_format` options for numeric fields (#3704)
- Added `PhraseMatchQuery` and `MultiMatchQuery` (#3727)
- Added Elasticsearch's `TermsQuery` (#3747)
- Added GCP PubSub source (#3720)
- Parse timestamp strings (#3639)
- Added Digital Ocean storage flavor (#3632)
- Added new tokenizers: `source_code_default`, `source_code`, `multilang` (#3647, #3655, #3608)


### Fixed

- Fixed dates in UI (#4277)
- Fixed duplicate splits planned on pipeline crash-respawn (#3854)
- Fixed sorting (#3799)

More details in tantivy's [changelog](https://github.com/quickwit-oss/tantivy/blob/main/CHANGELOG.md).

### Changed

- Improve OTEL traces index config (#4311)
  - OTEL endpoints are now using by default indexes `otel-logs-v0_7` and `otel-traces-v0_7` instead of `otel-logs-v0_6` and `otel-traces-v0_6`
  - OTEL indexes have more fields stored as "fast" and have Trace and Span ID bytes field in hex format

- Increased the gRPC payload limits from 10MiB to 20MiB (#4227)
- Reject malformed Elasticsearch API requests (#4175)
- Better logging when doc processing fails (#4323)
- Search performance improvements
- Indexing performance improvements

### Removed

### Migration from 0.6.x to 0.7

The format of the index and internal objects stored in the metastore of 0.7 is backward compatible with 0.6.

If you are using the OTEL indexes and ingesting data into indexes the `otel-logs-v0_6` and `otel-traces-v0_6`, you must stop indexing before upgrading.
Indeed, the first time you start Quickwit 0.7, it will update the doc mapping fields of Trace ID and Span ID of those two indexes by changing their input/output formats from base64 to hex. This is automatic: you don't have to perform any manual operation.
Quickwit 0.7 will create new indexes `otel-logs-v0_7` and `otel-traces-v0_7`, which are now used by default when ingesting data with the OTEL gRPC and HTTP API. The Jaeger gRPC and HTTP APIs will query both `otel-traces-v0_6` and `otel-traces-v0_7` by default.
It's possible to define the index ID you want to use for OTEL gRPC endpoints and Jaeger gRPC API by setting the request header `qw-otel-logs-index` or `qw-otel-traces-index` to the index ID you want to target.


## [0.6.1]

### Added
- Support of phrase prefix queries in the query language.

### Fixed
- Fix timestamp field which was not allowed when defined in an object mapping.
- Fix querying of integer on a JSON field (no document were returned).


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
