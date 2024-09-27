# Ingest V2

Ingest V2 is a new ingestion API that is designed to be more efficient and scalable for thousands of indexes than the previous version. It is currently in beta and is not yet enabled by default.

## Enabling Ingest V2

To enable Ingest V2, you need to set the `QW_ENABLE_INGEST_V2` environment variable to `1` on the indexer, control-plane, and metastore services.

You also have to activate the `enable_cooperative_indexing` option in the indexer configuration. The indexer configuration is in the node configuration:

```yaml
version: 0.8
# [...]
indexer:
  enable_cooperative_indexing: true
```

See [full configuration example](https://github.com/quickwit-oss/quickwit/blob/main/config/quickwit.yaml).

The only way to use the ingest API V2 is to use the [bulk endpoint](../reference/es_compatible_api.md#_bulk--batch-ingestion-endpoint) of the Elasticsearch-compatible API. The native Quickwit API is not yet compatible with the ingest V2 API.
