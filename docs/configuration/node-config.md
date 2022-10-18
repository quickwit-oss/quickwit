---
title: Node configuration
sidebar_position: 1
---

This page documents the Quickwit configuration properties. It is divided into three parts:

- Common properties.
- Indexer properties: defined in `[indexer]` section of the configuration file.
- Searcher properties: defined in `[searcher]` section of the configuration file.

A commented example is accessible here: [quickwit.yaml](https://github.com/quickwit-oss/quickwit/blob/main/config/quickwit.yaml).

## Common configuration

| Property | Description | Default value |
| --- | --- | --- |
| version | Config file version. 0 is the only available value. |  |
| node_id | Node ID of the instance (searcher or indexer). It must be unique in your cluster. If not set, a random ID is generated at each boot. |  |
| cluster_id | Unique Id for the cluster this node will be joining. Should be set to a unique name to ensure clusters do not accidentally merge together. | "quickwit-test-cluster" |
| listen_address | The IP address or hostname that Quickwit service binds to for starting REST and GRPC server and connecting this node to other nodes. By default, Quickwit binds itself to 127.0.0.1 (localhost). This default is not valid when trying to form a cluster. | 127.0.0.1 |
| rest_listen_port | The port which to listen for HTTP REST API. | 7280 |
| peer_seeds | List of IP addresses used by gossip for bootstrapping new nodes joining a cluster. This list may contain the current node address, and it does not need to be exhaustive on every node. |  |
| data_dir_path | Path to directory where data (tmp data, splits kept for caching purpose) is persisted. This is mostly used in indexing. | `./qwdata` |
| metastore_uri | Metastore URI. Can be a local directory or `s3://my-bucket/indexes` or `postgres://username:password@localhost:5432/metastore`. [Learn more about the metastore configuration](metastore-config.md). | `{data_dir}/indexes` |
| default_index_root_uri | Default index root URI that defines the location where index data (splits) is stored. The index uri is build following the the scheme: `{default_index_root_uri}/{index-id}` | `{data_dir}/indexes` |

## Indexer configuration

This section contains the configuration options for an indexer. The split store is documented in the  [indexing document](../concepts/indexing.md#split-store).

| Property | Description | Default value |
| --- | --- | --- |
| split_store_max_num_bytes | Maximum size in bytes allowed in the split store for each index-source pair. | 200G |
| split_store_max_num_splits | Maximum number of files allowed in the split store for each index-source pair. | 10000 |
| max_concurrent_split_uploads | Maximum number of concurrent split uploads allowed on the node. | 4 |

## Searcher configuration

This section contains the configuration options for a Searcher.

| Property | Description | Default value |
| --- | --- | --- |
| fast_field_cache_capacity | Fast field cache capacity on a Searcher. | 10G |
| split_footer_cache_capacity | Split footer cache (it is essentially the hotcache) capacity on a Searcher. | 1G |
| max_num_concurrent_split_streams | Maximum number of concurrent split stream requests running on a Searcher. | 100 |

## Using environment variables in the configuration

You can use environment variable references in the config file to set values that need to be configurable during deployment. To do this, use:

${VAR_NAME}

Where `VAR_NAME` is the name of the environment variable.

Each variable reference is replaced at startup by the value of the environment variable. The replacement is case-sensitive and occurs before the configuration file is parsed. References to undefined variables throw an error unless you specify a default value or custom error text.

To specify a default value, use:

${VAR_NAME:-default_value}

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
version: 0
cluster_id: quickwit-cluster
node_id: my-unique-node-id
listen_address: ${QW_LISTEN_ADDRESS}
rest_listen_port: ${QW_LISTEN_PORT:-1111}
```

Will be interpreted by Quickwit as:

```yaml
version: 0
cluster_id: quickwit-cluster
node_id: my-unique-node-id
listen_address: 0.0.0.0
rest_listen_port: 1111
```
