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
| `version` | Config file version. `0.6` is the only available value with a retro compatibility on `0.5` and `0.4`. | | |
| `cluster_id` | Unique identifier of the cluster the node will be joining. Clusters sharing the same network should use distinct cluster IDs.| `QW_CLUSTER_ID` | `quickwit-default-cluster` |
| `node_id` | Unique identifier of the node. It must be distinct from the node IDs of its cluster peers. Defaults to the instance's short hostname if not set. | `QW_NODE_ID` | short hostname |
| `enabled_services` | Enabled services (control_plane, indexer, janitor, metastore, searcher) | `QW_ENABLED_SERVICES` | all services |
| `listen_address` | The IP address or hostname that Quickwit service binds to for starting REST and GRPC server and connecting this node to other nodes. By default, Quickwit binds itself to 127.0.0.1 (localhost). This default is not valid when trying to form a cluster. | `QW_LISTEN_ADDRESS` | `127.0.0.1` |
| `advertise_address` | IP address advertised by the node, i.e. the IP address that peer nodes should use to connect to the node for RPCs. | `QW_ADVERTISE_ADDRESS` | `listen_address` |
| `rest_listen_port` | The port which to listen for HTTP REST API. | `QW_REST_LISTEN_PORT` | `7280` |
| `gossip_listen_port` | The port which to listen for the Gossip cluster membership service (UDP). | `QW_GOSSIP_LISTEN_PORT` | `rest_listen_port` |
| `grpc_listen_port` | The port which to listen for the gRPC service.| `QW_GRPC_LISTEN_PORT` | `rest_listen_port + 1` |
| `peer_seeds` | List of IP addresses or hostnames used to bootstrap the cluster and discover the complete set of nodes. This list may contain the current node address and does not need to be exhaustive. | `QW_PEER_SEEDS` | |
| `data_dir` | Path to directory where data (tmp data, splits kept for caching purpose) is persisted. This is mostly used in indexing. | `QW_DATA_DIR` | `./qwdata` |
| `metastore_uri` | Metastore URI. Can be a local directory or `s3://my-bucket/indexes` or `postgres://username:password@localhost:5432/metastore`. [Learn more about the metastore configuration](metastore-config.md). | `QW_METASTORE_URI` | `{data_dir}/indexes` |
| `default_index_root_uri` | Default index root URI that defines the location where index data (splits) is stored. The index URI is built following the scheme: `{default_index_root_uri}/{index-id}` | `QW_DEFAULT_INDEX_ROOT_URI` | `{data_dir}/indexes` |
| `rest_cors_allow_origins` | Configure the CORS origins which are allowed to access the API. [Read more](#configuring-cors-cross-origin-resource-sharing) | |


There are also other parameters that can be only defined by env variables:

| Env variable | Description |
| --- | --- |
| `QW_S3_ENDPOINT` | Custom S3 endpoint. |
| `QW_S3_MAX_CONCURRENCY` | Limit the number of concurent requests to S3 |
| `QW_ENABLE_JAEGER_EXPORTER` | Enable trace export to Jaeger. |
| `QW_AZURE_STORAGE_ACCOUNT` | Azure Blob Storage account name. |
| `QW_AZURE_STORAGE_ACCESS_KEY` | Azure Blob Storage account access key. |

More details about [storage configuration](../reference/storage-uri.md).

## Storage configuration

This section may contain one configuration subsection per storage provider. The specific configuration parameters for each provider may vary. Currently, the supported storage providers are:
- Azure
- Amazon S3 or S3-compatible providers

If a storage configuration is not explicitly set, Quickwit will rely on the default settings provided by the SDK ([Azure SDK for Rust](https://github.com/Azure/azure-sdk-for-rust), [AWS SDK for Rust](https://github.com/awslabs/aws-sdk-rust)) of each storage provider.

### Azure storage configuration

| Property | Description | Default value |
| --- | --- | --- |
| `account` | The Azure storage account name. | |
| `access_key` | The Azure storage account access key. | |

Example of a storage configuration for Azure in YAML format:

```yaml
storage:
  azure:
    account: your-azure-account-name
    access_key: your-azure-access-key
```

### S3 storage configuration

| Property | Description | Default value |
| --- | --- | --- |
| `endpoint` | Custom endpoint for use with S3-compatible providers. | SDK default |
| `force_path_style_access` | Disables [virtual-hosted–style](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html) requests. Required by some S3-compatible providers (Ceph, MinIO). | `false` |
| `disable_multi_object_delete_requests` | Disables [Multi-Object Delete](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html) requests. Required by some S3-compatible providers (GCS). | `false` |

Example of a storage configuration for S3 in YAML format:

```yaml
storage:
  s3:
    endpoint: http://localhost:9000
    force_path_style_access: true
```

## Metastore configuration

This section may contain one configuration subsection per available metastore implementation. The specific configuration parameters for each implementation may vary. Currently, the available metastore implementations are:
- File-backed
- PostgreSQL

### File-backed metastore configuration

| Property | Description | Default value |
| --- | --- | --- |
| `polling_interval` | Time interval between successive polling attempts to detect metastore changes. | `30s` |

Example of a metastore configuration for a file-backed implementation in YAML format:

```yaml
metastore:
  file:
    polling_interval: 1m
```

### PostgreSQL metastore configuration

| Property | Description | Default value |
| --- | --- | --- |
| `max_num_connections` | Determines the maximum number of concurrent connections to the database server. | `10` |

Example of a metastore configuration for PostgreSQL in YAML format:

```yaml
metastore:
  postgres:
    max_num_connections: 50
```

## Indexer configuration

This section contains the configuration options for an indexer. The split store is documented in the  [indexing document](../overview/concepts/indexing.md#split-store).

| Property | Description | Default value |
| --- | --- | --- |
| `split_store_max_num_bytes` | Maximum size in bytes allowed in the split store for each index-source pair. | `100G` |
| `split_store_max_num_splits` | Maximum number of files allowed in the split store for each index-source pair. | `1000` |
| `max_concurrent_split_uploads` | Maximum number of concurrent split uploads allowed on the node. | `12` |
| `enable_otlp_endpoint` | If true, enables the OpenTelemetry exporter endpoint to ingest logs and traces via the OpenTelemetry Protocol (OTLP). | `false` |

## Ingest API configuration

| Property | Description | Default value |
| --- | --- | --- |
| `max_queue_memory_usage` | Maximum size in bytes of the in-memory Ingest queue. | `2GiB` |
| `max_queue_disk_usage` | Maximum disk-space in bytes taken by the Ingest queue. This is typically higher than the max in-memory queue. | `4GiB` |


## Searcher configuration

This section contains the configuration options for a Searcher.

| Property | Description | Default value |
| --- | --- | --- |
| `aggregation_memory_limit` | Controls the maximum amount of memory that can be used for aggregations before aborting. This limit is per request and single leaf query (a leaf query is querying one or multiple splits concurrently). It is used to prevent excessive memory usage during the aggregation phase, which can lead to performance degradation or crashes. Since it is per request, concurrent requests can exceed the limit. | `500M`|
| `aggregation_bucket_limit` | Determines the maximum number of buckets returned to the client. | `65000` |
| `fast_field_cache_capacity` | Fast field cache capacity on a Searcher. If your filter by dates, run aggregations, range queries, or if you use the search stream API, or even for tracing, it might worth increasing this parameter. The [metrics](../reference/metrics.md) starting by `quickwit_cache_fastfields_cache` can help you make an informed choice when setting this value. | `1G` |
| `split_footer_cache_capacity` | Split footer cache (it is essentially the hotcache) capacity on a Searcher.| `500M` |
| `partial_request_cache_capacity` | Partial request cache capacity on a Searcher. Cache intermediate state for a request, possibly making subsequent requests faster. It can be disabled by setting the size to `0`. | `64M` |
| `max_num_concurrent_split_searches` | Maximum number of concurrent split search requests running on a Searcher. | `100` |
| `max_num_concurrent_split_streams` | Maximum number of concurrent split stream requests running on a Searcher. | `100` |

## Jaeger configuration

| Property | Description | Default value |
| --- | --- | --- |
| `enable_endpoint` | If true, enables the gRPC endpoint that allows the Jaeger Query Service to connect and retrieve traces. | `false` |


## Using environment variables in the configuration

You can use environment variable references in the config file to set values that need to be configurable during deployment. To do this, use:

`${VAR_NAME}`

Where `VAR_NAME` is the name of the environment variable.

Each variable reference is replaced at startup by the value of the environment variable. The replacement is case-sensitive and occurs before the configuration file is parsed. References to undefined variables throw an error unless you specify a default value or custom error text.

To specify a default value, use:

`${VAR_NAME:-default_value}`

Where `default_value` is the value to use if the environment variable is not set.

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
version: 0.6
cluster_id: quickwit-cluster
node_id: my-unique-node-id
listen_address: ${QW_LISTEN_ADDRESS}
rest_listen_port: ${QW_LISTEN_PORT:-1111}
```

Will be interpreted by Quickwit as:

```yaml
version: 0.6
cluster_id: quickwit-cluster
node_id: my-unique-node-id
listen_address: 0.0.0.0
rest_listen_port: 1111
```

## Configuring CORS (Cross-origin resource sharing)

CORS (Cross-origin resource sharing) describes what address/origins can access the REST API from the browser,
by default no origins are allowed.

A wildcard, single origin or multiple origins can be specified as part of the `rest_cors_allow_origins` parameter:

```yaml
version: 0.6
index_id: hdfs

rest_cors_allow_origins: '*'                                 # Allow all origins
# rest_cors_allow_origins: https://my-hdfs-logs.domain.com   # Optionally we can specify one domain
# rest_cors_allow_origins:                                   # Or allow multiple origins
#   - https://my-hdfs-logs.domain.com
#   - https://my-hdfs.other-domain.com
```
