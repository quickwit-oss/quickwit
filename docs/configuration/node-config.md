---
title: Node configuration
sidebar_position: 1
---

The node configuration allows you to customize and optimize the settings for individual nodes in your cluster. It is divided into several sections:

- Common configuration settings: shared top-level properties
- Storage settings: defined in the [storage](#storage-configuration) section
- Metastore settings: defined in the [metastore](#metastore-configuration) section
- Ingest settings: defined in the [ingest_api](#ingest-api-configuration) section
- Indexer settings: defined in the [indexer](#indexer-configuration) section
- Searcher settings: defined in the [searcher](#searcher-configuration) section
- Jaeger settings: defined in the [jaeger](#jaeger-configuration) section

A commented example is available here: [quickwit.yaml](https://github.com/quickwit-oss/quickwit/blob/main/config/quickwit.yaml).

## Common configuration

| Property | Description | Env variable | Default value |
| --- | --- | --- | --- |
| `version` | Config file version. `0.7` is the only available value with a retro compatibility on `0.5` and `0.4`. | | |
| `cluster_id` | Unique identifier of the cluster the node will be joining. Clusters sharing the same network should use distinct cluster IDs.| `QW_CLUSTER_ID` | `quickwit-default-cluster` |
| `node_id` | Unique identifier of the node. It must be distinct from the node IDs of its cluster peers. Defaults to the instance's short hostname if not set. | `QW_NODE_ID` | short hostname |
| `enabled_services` | Enabled services (control_plane, indexer, janitor, metastore, metastore_read_replica, searcher) | `QW_ENABLED_SERVICES` | all services except metastore_read_replica |
| `listen_address` | The IP address or hostname that Quickwit service binds to for starting REST and GRPC server and connecting this node to other nodes. By default, Quickwit binds itself to 127.0.0.1 (localhost). This default is not valid when trying to form a cluster. | `QW_LISTEN_ADDRESS` | `127.0.0.1` |
| `advertise_address` | IP address advertised by the node, i.e. the IP address that peer nodes should use to connect to the node for RPCs. | `QW_ADVERTISE_ADDRESS` | `listen_address` |
| `gossip_listen_port` | The port which to listen for the Gossip cluster membership service (UDP). | `QW_GOSSIP_LISTEN_PORT` | `rest.listen_port` |
| `grpc_listen_port` | The port on which gRPC services listen for traffic. | `QW_GRPC_LISTEN_PORT` | `rest.listen_port + 1` |
| `peer_seeds` | List of IP addresses or hostnames used to bootstrap the cluster and discover the complete set of nodes. This list may contain the current node address and does not need to be exhaustive. If the list of peer seeds contains a host name, Quickwit will resolve it by querying the DNS every minute. On kubernetes for instance, it is a good practise to set it to a [headless service](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services). | `QW_PEER_SEEDS` | |
| `data_dir` | Path to directory where data (tmp data, splits kept for caching purpose) is persisted. This is mostly used in indexing. | `QW_DATA_DIR` | `./qwdata` |
| `metastore_uri` | Metastore URI. Can be a local directory or `s3://my-bucket/indexes` or `postgres://username:password@localhost:5432/metastore`. [Learn more about the metastore configuration](metastore-config.md). | `QW_METASTORE_URI` | `{data_dir}/indexes` |
| `metastore_read_replica_uri` | Optional PostgreSQL read replica URI. Nodes running the `metastore_read_replica` service connect to it over a read-only connection and serve stale-tolerant read-only metastore requests. Searchers use those nodes only when `searcher.use_metastore_read_replica` is enabled. | `QW_METASTORE_READ_REPLICA_URI` | |
| `default_index_root_uri` | Default index root URI that defines the location where index data (splits) is stored. The index URI is built following the scheme: `{default_index_root_uri}/{index-id}` | `QW_DEFAULT_INDEX_ROOT_URI` | `{data_dir}/indexes` |
| environment variable only | Log level of Quickwit. Can be a direct log level, or a comma separated list of `module_name=level` | `RUST_LOG` | `info` |

## REST configuration

This section contains the REST API configuration options.

| Property | Description | Env variable | Default value |
| --- | --- | --- | --- |
| `listen_port` | The port on which the REST API listens for HTTP traffic. | `QW_REST_LISTEN_PORT` | `7280` |
| `cors_allow_origins` | Configure the CORS origins which are allowed to access the API. [Read more](#configuring-cors-cross-origin-resource-sharing) | |
| `extra_headers` | List of header names and values | | |
| `tls` | Enables HTTPS for the REST API. [Read more](#tls-configuration) | | |

### Configuring CORS (Cross-origin resource sharing)

CORS (Cross-origin resource sharing) describes which address or origins can access the REST API from the browser.
By default, sharing resources cross-origin is not allowed.

A wildcard, single origin, or multiple origins can be specified as part of the `cors_allow_origins` parameter:


Example of a REST configuration:

```yaml
rest:
  listen_port: 1789
  extra_headers:
    x-header-1: header-value-1
    x-header-2: header-value-2
  cors_allow_origins: '*'

#   cors_allow_origins: https://my-hdfs-logs.domain.com   # Optionally we can specify one domain
#   cors_allow_origins:                                   # Or allow multiple origins
#     - https://my-hdfs-logs.domain.com
#     - https://my-hdfs.other-domain.com
```

## gRPC configuration

This section contains the configuration options for gRPC services and clients used for internal communication between nodes.

| Property | Description | Env variable | Default value |
| --- | --- | --- | --- |
| `max_message_size` | The maximum size (in bytes) of messages exchanged by internal gRPC clients and services. | | `20 MiB` |
| `tls` | Enables TLS for gRPC services and clients. [Read more](#tls-configuration) | | |

Example of a gRPC configuration:

```yaml
grpc:
  max_message_size: 30 MiB
```

:::warning
We advise changing the default value of 20 MiB only if you encounter the following error:
`Error, message length too large: found 24732228 bytes, the limit is: 20971520 bytes.` In that case, increase `max_message_size` by increments of 10 MiB until the issue disappears. This is a temporary fix: the next version of Quickwit will rely exclusively on gRPC streaming endpoints and handle messages of any length.
:::

## Health check configuration

This section configures an optional, **plaintext (no TLS)** HTTP server that exposes only the health endpoints `/health/livez` (liveness) and `/health/readyz` (readiness). Its purpose is to let liveness/readiness probes (for example from Kubernetes or a load balancer) reach the node even when the main REST API is put behind [TLS or mTLS](#tls-configuration), which a simple HTTP probe cannot negotiate.

The health server is **disabled by default**. It starts only when `listen_port` is set (or the `QW_HEALTH_LISTEN_PORT` environment variable is provided). The same `/health/*` endpoints always remain available on the main REST API as well.

| Property | Description | Env variable | Default value |
| --- | --- | --- | --- |
| `listen_port` | The port on which the plaintext health server listens for HTTP traffic. When unset, the health server is disabled. | `QW_HEALTH_LISTEN_PORT` | _(disabled)_ |

Pick a free port that is not already used by the REST or gRPC servers.

Example of a health check configuration:

```yaml
health:
  listen_port: 7282
```

:::warning
This server performs no TLS termination and no client authentication. Bind it to a cluster-internal interface (via `listen_address`) and do not expose it publicly.
:::

## TLS configuration

Both the REST API (`rest.tls`) and the internal gRPC services (`grpc.tls`) can be secured with TLS, optionally with mutual TLS (mTLS). The two sections share the same properties:

:::tip
When the REST API is behind mTLS, simple HTTP health probes can no longer reach it. Enable the plaintext [health check server](#health-check-configuration) to keep liveness/readiness probes working.
:::

| Property | Description | Default value |
| --- | --- | --- |
| `cert_path` | Path to the PEM-encoded X.509 certificate (or chain) presented by the server. Setting this enables TLS. | |
| `key_path` | Path to the PEM-encoded private key matching `cert_path`. | |
| `ca_path` | Path to a PEM file holding the trusted CA certificate(s). Used by the server to validate client certificates when `verify_client_cert` is enabled, and by the gRPC client to validate peer certificates. Multiple CA certificates may be concatenated in the same file: all of them are trusted (see [CA rotation](#ca-rotation)). | |
| `verify_client_cert` | If `true`, require clients (REST) or peers (gRPC) to present a certificate signed by `ca_path`, i.e. enforce mutual TLS. | `false` |
| `expected_name` | gRPC only. The hostname the gRPC client checks against the peer certificate's Subject Alternative Name (SAN). Defaults to the peer's address. | |
| `cert_poll_interval` | How often `cert_path` and `key_path` are polled for on-disk changes and hot-reloaded, without restarting the process. An immediate reload can also be triggered by sending `SIGHUP` to the process. | `5m` |

Certificates are hot-reloaded: when `cert_path`/`key_path` change on disk, new connections pick up the new certificate within `cert_poll_interval` (or immediately on `SIGHUP`), while in-flight connections keep the certificate they negotiated. A new certificate is only applied if it parses and matches its key; otherwise the previous certificate is kept. Note that the CA trust roots (`ca_path`) are **not** hot-reloaded — rotating them still requires a restart.

### CA rotation

Because `ca_path` accepts multiple CA certificates concatenated in a single PEM file, you can rotate the CA without downtime by temporarily trusting both the old and the new CA:

1. Append the **new** CA certificate to the `ca_path` file, so it contains both the old and the new CA.
2. Roll-restart every node. Each node now trusts certificates signed by either CA, while peers may still present certificates signed by the old CA.
3. Re-issue every node's `cert_path`/`key_path` with certificates signed by the new CA (these are hot-reloaded, no restart needed).
4. Remove the **old** CA certificate from the `ca_path` file, leaving only the new CA.
5. Roll-restart every node again to drop trust in the old CA.

Since the CA file is read once at startup, both restarts are required to pick up the changes to `ca_path`.

Example of a REST configuration with mTLS:

```yaml
rest:
  tls:
    cert_path: /path/to/server.crt
    key_path: /path/to/server.key
    ca_path: /path/to/ca.crt
    verify_client_cert: true
    cert_poll_interval: 5m
```

Example of a gRPC configuration with mTLS:

```yaml
grpc:
  tls:
    cert_path: /path/to/server.crt
    key_path: /path/to/server.key
    ca_path: /path/to/ca.crt
    expected_name: quickwit.local
    verify_client_cert: true
    cert_poll_interval: 5m
```

## Storage configuration

Please refer to the dedicated [storage configuration](storage-config) page to learn more about configuring Quickwit for various storage providers.

Here are also some minimal examples of how to configure Quickwit with Amazon S3 or Alibaba OSS:

```bash
AWS_ACCESS_KEY_ID=<your access key ID>
AWS_SECRET_ACCESS_KEY=<your secret access key>
```

*Amazon S3*

```yaml
storage:
  s3:
    region: us-east-1
```

*Alibaba*

```yaml
storage:
  s3:
    region: us-east-1
    endpoint: https://oss-us-east-1.aliyuncs.com
```

## Metastore configuration

This section may contain one configuration subsection per available metastore implementation. The specific configuration parameters for each implementation may vary. Currently, the available metastore implementations are:
- File-backed
- PostgreSQL

### File-backed metastore configuration

File-backed metastore doesn't have any node level configuration. You can configure the poll interval [at the index level](./metastore-config.md#polling-configuration).

### PostgreSQL metastore configuration

| Property | Description | Default value |
| --- | --- | --- |
| `min_connections` | Minimum number of connections to maintain in the pool at all times. | `0` |
| `max_connections` | Maximum number of connections to maintain in the pool. | `10` |
| `acquire_connection_timeout` | Maximum amount of time to spend waiting for an available connection before aborting a query. | `10s` |
| `idle_connection_timeout` | Maximum idle duration before closing individual connections. | `10min` |
| `max_connection_lifetime` | Maximum lifetime of individual connections. | `30min` |

Example of a metastore configuration for PostgreSQL in YAML format:

```yaml
metastore:
  postgres:
    min_connections: 10
    max_connections: 50
    acquire_connection_timeout: 30s
    idle_connection_timeout: 1h
    max_connection_lifetime: 1d
```

## Indexer configuration

This section contains the configuration options for an indexer. The split store is documented in the [indexing document](../overview/concepts/indexing.md#split-store).

| Property | Description | Default value |
| --- | --- | --- |
| `split_store_max_num_bytes` | Maximum size in bytes allowed in the split store. | `100G` |
| `split_store_max_num_splits` | Maximum number of files allowed in the split store. | `1000` |
| `max_concurrent_split_uploads` | Maximum number of concurrent split uploads allowed on the node. | `12` |
| `merge_concurrency` | Maximum number of merge operations that can be executed on the node at one point in time. | `(2 x num threads available) / 3` |
| `enable_otlp_endpoint` | If true, enables the OpenTelemetry exporter endpoint to ingest logs and traces via the OpenTelemetry Protocol (OTLP). | `false` |
| `cpu_capacity` | Advisory parameter used by the control plane. The value can expressed be in threads (e.g. `2`) or in term of millicpus (`2000m`). The control plane will attempt to schedule indexing pipelines on the different nodes proportionally to the cpu capacity advertised by the indexer. It is NOT used as a limit. All pipelines will be scheduled regardless of whether the cluster has sufficient capacity or not. The control plane does not attempt to spread the work equally when the load is well below the `cpu_capacity`. Users who need a balanced load on all of their indexer nodes can set the `cpu_capacity` to an arbitrarily low value as long as they keep it proportional to the number of threads available. | `num threads available` |
| `enable_cooperative_indexing` | Enable sharing resources more efficiently when the number of indexes actively written to is significantly higher than the number of cores but might decrease the overall indexing throughput. | `false` |

Example:

```yaml
indexer:
  split_store_max_num_bytes: 100G
  split_store_max_num_splits: 1000
  max_concurrent_split_uploads: 12
  enable_otlp_endpoint: true
```

## Ingest API configuration

| Property | Description | Default value |
| --- | --- | --- |
| `max_queue_memory_usage` | Maximum size in bytes of the in-memory Ingest queue. | `2GiB` |
| `max_queue_disk_usage` | Maximum disk-space in bytes taken by the Ingest queue. The minimum size is at least `256M` and be at least `max_queue_memory_usage`. | `4GiB` |
| `content_length_limit` | Maximum payload size uncompressed. Increasing this is discouraged, use a [file source](../ingest-data/sqs-files.md) instead. | `10MiB` |
| `grpc_compression_algorithm` | Compression algorithm (`gzip` or `zstd`) to use for gRPC traffic between nodes for the ingest service | `None` |

Example:

```yaml
ingest_api:
  max_queue_memory_usage: 2GiB
  max_queue_disk_usage: 4GiB
  content_length_limit: 10MiB
  grpc_compression_algorithm: zstd
```

## Searcher configuration

This section contains the configuration options for a Searcher.

| Property | Description | Default value |
| --- | --- | --- |
| `aggregation_memory_limit` | Controls the maximum amount of memory that can be used for aggregations before aborting. This limit is per searcher node. A node may run concurrent queries, which share the limit. The first query that will hit the limit will be aborted and frees its memory. It is used to prevent excessive memory usage during the aggregation phase, which can lead to performance degradation or crashes. | `500M`|
| `aggregation_bucket_limit` | Determines the maximum number of buckets returned to the client. | `65000` |
| `fast_field_cache_capacity` | Fast field in memory cache capacity on a Searcher. If your filter by dates, run aggregations, range queries, or even for tracing, it might worth increasing this parameter. The [metrics](../reference/metrics.md) starting by `quickwit_cache_fastfields_cache` can help you make an informed choice when setting this value. | `1G` |
| `split_footer_cache_capacity` | Split footer in memory cache (it is essentially the hotcache) capacity on a Searcher.| `500M` |
| `partial_request_cache_capacity` | Partial request in memory cache capacity on a Searcher. Cache intermediate state for a request, possibly making subsequent requests faster. It can be disabled by setting the size to `0`. | `64M` |
| `max_num_concurrent_split_searches` | Maximum number of concurrent split search requests running on a Searcher. | `100` |
| `split_cache` | Searcher split cache configuration options defined in the section below. Cache disabled if unspecified. | |
| `request_timeout_secs` | The time before a search request is cancelled. This should match the timeout of the stack calling into quickwit if there is one set.  | `30` |
| `use_metastore_read_replica` | If true, routes read-only metastore requests from searchers, including DataFusion when enabled, to nodes running the `metastore_read_replica` service. Searchers require at least one `metastore_read_replica` node at startup and do not fall back to the primary metastore. | `false` |

### Searcher split cache configuration

This section contains the configuration options for the on-disk searcher split cache. Files are stored in the data directory under `searcher-split-cache/`.

| Property | Description | Default value |
| --- | --- | --- |
| `max_num_bytes` | Maximum disk size in bytes allowed in the split cache. Can be exceeded by the size of one split. | |
| `max_num_splits` | Maximum number of splits allowed in the split cache.   | `10000` |
| `num_concurrent_downloads` | Maximum number of concurrent download of splits. | `1` |


Example:

```yaml
searcher:
  use_metastore_read_replica: false
  fast_field_cache_capacity: 1G
  split_footer_cache_capacity: 500M
  partial_request_cache_capacity: 64M
  split_cache:
    max_num_bytes: 1G
    max_num_splits: 10000
    num_concurrent_downloads: 1
```

## Jaeger configuration

| Property | Description | Default value |
| --- | --- | --- |
| `enable_endpoint` | If true, enables the gRPC endpoint that allows the Jaeger Query Service to connect and retrieve traces. | `false` |

Example:

```yaml
jaeger:
  enable_endpoint: true
```


## Using environment variables in the configuration

You can use environment variable references in the config file to set values that need to be configurable during deployment. To do this, use:

`${VAR_NAME}`

where `VAR_NAME` is the name of the environment variable.

Each variable reference is replaced at startup by the value of the environment variable. The replacement is case-sensitive and occurs before the configuration file is parsed. Referencing undefined variables throws an error unless you specify a default value or custom error text.

To specify a default value, use:

`${VAR_NAME:-default_value}`

where `default_value` is the value to use if the environment variable is unset.

```
<config_field>: ${VAR_NAME}
or
<config_field>: ${VAR_NAME:-default value}
```

For example:

```bash
export QW_LISTEN_ADDRESS=0.0.0.0
```

```yaml
# config.yaml
version: 0.7
cluster_id: quickwit-cluster
node_id: my-unique-node-id
listen_address: ${QW_LISTEN_ADDRESS}
rest:
  listen_port: ${QW_LISTEN_PORT:-1111}
```

Will be interpreted by Quickwit as:

```yaml
version: 0.7
cluster_id: quickwit-cluster
node_id: my-unique-node-id
listen_address: 0.0.0.0
rest:
  listen_port: 1111
```
