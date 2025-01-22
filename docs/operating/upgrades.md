---
title: Version 0.7 upgrade
sidebar_position: 4
---

## Migration from 0.6.x to 0.7.0

The format of the index and internal objects stored in the metastore of 0.7 is backward compatible with 0.6.

If you are using the OTEL indexes and ingesting data into indexes the `otel-logs-v0_6` and `otel-traces-v0_6`, you must stop indexing before upgrading. Indeed, the first time you start Quickwit 0.7, it will update the doc mapping fields of Trace ID and Span ID of those two indexes by changing their input/output formats from `base64` to `hex`. This is automatic: you don't have to perform any manual operation.

Quickwit 0.7 will also create the new index `otel-traces-v0_7`, which is now used by default when ingesting data with the OTEL gRPC and HTTP API. The Jaeger gRPC and HTTP APIs will query both `otel-traces-v0_6` and `otel-traces-v0_7` by default. It's possible to define the index ID you want to use for OTEL gRPC endpoints and Jaeger gRPC API by setting the request header `qw-otel-logs-index` or `qw-otel-traces-index` to the index ID you want to target.


## Migration from 0.7.0 to 0.7.1

Quickwit 0.7.1 will create the new index `otel-logs-v0_7` which is now used by default when ingesting data with the OTEL gRPC and HTTP API.

In the traces index `otel-traces-v0_7`, the `service_name` field is now `fast`. 
No migration is done if `otel-traces-v0_7` already exists. If you want `service_name` field to be `fast`, you have to delete first the existing `otel-traces-v0_7` index or you need to create your own index.

## Migration from 0.8 to 0.9

Quickwit 0.9 introduces a new ingestion service to to power the ingest and bulk APIs (v2). The new ingest is enabled and used by default, even though the legacy one (v1) remains enabled to finish indexing residual data in the legacy write ahead logs. Note that `ingest_api.max_queue_disk_usage` is enforced on both ingest versions separately, which means that the cumulated disk usage might be up to twice this limit.

The control plane should be upgraded first in order to enable the new ingest source (v2) on all existing indexes. Ingested data into previously existing indexes on upgraded indexer nodes will not be picked by the indexing pipelines until the control plane is upgraded.
