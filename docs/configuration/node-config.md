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
| `enabled_services` | Enabled services (control_plane, indexer, janitor, metastore, searcher) | `QW_ENABLED_SERVICES` | all services |
| `listen_address` | The IP address or hostname that Quickwit service binds to for starting REST and GRPC server and connecting this node to other nodes. By default, Quickwit binds itself to 127.0.0.1 (localhost). This default is not valid when trying to form a cluster. | `QW_LISTEN_ADDRESS` | `127.0.0.1` |
| `advertise_address` | IP address advertised by the node, i.e. the IP address that peer nodes should use to connect to the node for RPCs. | `QW_ADVERTISE_ADDRESS` | `listen_address` |
| `gossip_listen_port` | The port which to listen for the Gossip cluster membership service (UDP). | `QW_GOSSIP_LISTEN_PORT` | `rest.listen_port` |
| `grpc_listen_port` | The port on which gRPC services listen for traffic. | `QW_GRPC_LISTEN_PORT` | `rest.listen_port + 1` |
| `peer_seeds` | List of IP addresses or hostnames used to bootstrap the cluster and discover the complete set of nodes. This list may contain the current node address and does not need to be exhaustive. If the list of peer seeds contains a host name, Quickwit will resolve it by querying the DNS every minute. On kubernetes for instance, it is a good practise to set it to a [headless service](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services). | `QW_PEER_SEEDS` | |
| `data_dir` | Path to directory where data (tmp data, splits kept for caching purpose) is persisted. This is mostly used in indexing. | `QW_DATA_DIR` | `./qwdata` |
| `metastore_uri` | Metastore URI. Can be a local directory or `s3://my-bucket/indexes` or `postgres://username:password@localhost:5432/metastore`. [Learn more about the metastore configuration](metastore-config.md). | `QW_METASTORE_URI` | `{data_dir}/indexes` |
| `default_index_root_uri` | Default index root URI that defines the location where index data (splits) is stored. The index URI is built following the scheme: `{default_index_root_uri}/{index-id}` | `QW_DEFAULT_INDEX_ROOT_URI` | `{data_dir}/indexes` |
| environment variable only | Log level of Quickwit. Can be a direct log level, or a comma separated list of `module_name=level` | `RUST_LOG` | `info` |

## REST configuration

This section contains the REST API configuration options.

| Property | Description | Env variable | Default value |
| --- | --- | --- | --- |
| `listen_port` | The port on which the REST API listens for HTTP traffic. | `QW_REST_LISTEN_PORT` | `7280` |
| `cors_allow_origins` | Configure the CORS origins which are allowed to access the API. [Read more](#configuring-cors-cross-origin-resource-sharing) | |
| `extra_headers` | List of header names and values | | |

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

Example of a gRPC configuration:

```yaml
grpc:
  max_message_size: 30 MiB
```

:::warning
We advise changing the default value of 20 MiB only if you encounter the following error:
`Error, message length too large: found 24732228 bytes, the limit is: 20971520 bytes.` In that case, increase `max_message_size` by increments of 10 MiB until the issue disappears. This is a temporary fix: the next version of Quickwit will rely exclusively on gRPC streaming endpoints and handle messages of any length.
:::

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

Example:

```yaml
ingest_api:
  max_queue_memory_usage: 2GiB
  max_queue_disk_usage: 4GiB
  content_length_limit: 10MiB
```

## Searcher configuration

This section contains the configuration options for a Searcher.

| Property | Description | Default value |
| --- | --- | --- |
| `aggregation_memory_limit` | Controls the maximum amount of memory that can be used for aggregations before aborting. This limit is per searcher node. A node may run concurrent queries, which share the limit. The first query that will hit the limit will be aborted and frees its memory. It is used to prevent excessive memory usage during the aggregation phase, which can lead to performance degradation or crashes. | `500M`|
| `aggregation_bucket_limit` | Determines the maximum number of buckets returned to the client. | `65000` |
| `fast_field_cache_capacity` | Fast field in memory cache capacity on a Searcher. If your filter by dates, run aggregations, range queries, or if you use the search stream API, or even for tracing, it might worth increasing this parameter. The [metrics](../reference/metrics.md) starting by `quickwit_cache_fastfields_cache` can help you make an informed choice when setting this value. | `1G` |
| `split_footer_cache_capacity` | Split footer in memory cache (it is essentially the hotcache) capacity on a Searcher.| `500M` |
| `partial_request_cache_capacity` | Partial request in memory cache capacity on a Searcher. Cache intermediate state for a request, possibly making subsequent requests faster. It can be disabled by setting the size to `0`. | `64M` |
| `max_num_concurrent_split_searches` | Maximum number of concurrent split search requests running on a Searcher. | `100` |
| `max_num_concurrent_split_streams` | Maximum number of concurrent split stream requests running on a Searcher. | `100` |
| `split_cache` | Searcher split cache configuration options defined in the section below. Cache disabled if unspecified. | |
| `request_timeout_secs` | The time before a search request is cancelled. This should match the timeout of the stack calling into quickwit if there is one set.  | `30` |

### Searcher split cache configuration

This section contains the configuration options for the on disk searcher split cache.

| Property | Description | Default value |
| --- | --- | --- |
| `max_num_bytes` | Maximum disk size in bytes allowed in the split cache. Can be exceeded by the size of one split. | |
| `max_num_splits` | Maximum number of splits allowed in the split cache.   | `10000` |
| `num_concurrent_downloads` | Maximum number of concurrent download of splits. | `1` |


Example:

```yaml
searcher:
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
