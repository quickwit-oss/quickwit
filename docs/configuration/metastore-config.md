---
title: Metastore configuration
sidebar_position: 4
---

Quickwit needs a place to store meta-information about its indexes.

For instance:

- The index configuration.
- Meta-information about its splits. For instance, their IDs, the number of documents they contain, their sizes, their min/max timestamp, and the set of tags present in the split.
- The different sources checkpoints.
- Some extra information such as the index creation time.

The metastore is entirely defined by a single URI. One can set it by editing the `metastore_uri` parameter of the [node configuration file](./node-config.md) (often named `quickwit.yaml`).

Currently, Quickwit offers two implementations:

- **PostgreSQL**: recommended for distributed usage.
- **File-backed implementation**.

# PostgreSQL Metastore

We recommend the PostgreSQL metastore for any distributed usage.

The PostgreSQL metastore can be configured by setting a PostgreSQL URI in the `metastore_uri` parameter of the Quickwit configuration file. The URI takes the following format:

```
postgres://[user]:[password]@[host]:[port]/[dbname]
```

Some of those parameters can be omitted. The following PostgreSQL URIs are for instance valid:

```
postgres://localhost/mydb
postgres://user@localhost
postgres://user:secret@localhost
```

The database has to be created in advance.

On its first execution, Quickwit will transparently create the necessary tables.

Likewise, if you upgrade Quickwit to a version that includes some changes in the PostgreSQL schema, Quickwit will transparently operate the migration startup.

## PostgreSQL connection pool

Each Quickwit node running the `metastore` service maintains a PostgreSQL connection pool. Nodes that do not run the `metastore` service use gRPC to reach those metastore nodes and do not open their own PostgreSQL pool.

You can configure the pool in the `metastore.postgres` section of the [node configuration file](./node-config.md):

```yaml
metastore:
  postgres:
    min_connections: 0
    max_connections: 10
    acquire_connection_timeout: 10s
    idle_connection_timeout: 10min
    max_connection_lifetime: 30min
```

The default `max_connections` is `10` per metastore node. Database-backed metastore nodes admit at most `2 * max_connections` in-flight metastore requests, so the default is `20` in-flight requests per metastore node. For production deployments backed by PostgreSQL or Amazon RDS, size this value together with the database connection limit:

```text
metastore_nodes * metastore.postgres.max_connections
```

Keep that total below the PostgreSQL or RDS connection limit with headroom for migrations, administration, monitoring, and other applications sharing the database. If Quickwit logs errors such as `pool timed out while waiting for an open connection`, the local PostgreSQL pool on a metastore node was exhausted for at least `acquire_connection_timeout`. In that case:

- Check `quickwit_metastore_active_connections`, `quickwit_metastore_idle_connections`, and `quickwit_metastore_acquire_connections`.
- If active connections stay close to `max_connections` and the database has spare capacity, increase `metastore.postgres.max_connections`.
- If active connections are high and PostgreSQL or RDS is already saturated, investigate slow metastore queries, locks, database CPU or I/O pressure, and RDS `DatabaseConnections` before increasing the pool.
- Increase `acquire_connection_timeout` only to tolerate short bursts. It does not add database capacity.

# File-backed metastore

For convenience, Quickwit also makes it possible to store its metadata in files using a file-backed metastore. In that case, Quickwit will write one file per index.

The metastore is then configured by passing a [storage URI](storage-config#storage-uris) that will serve as the root of the metastore storage.

The metadata file associated with a given index will then be stored under

  `[storage_uri]/[index_id]/metastore.json`

For the moment, Quickwit supports two types of storage types:

- a local file system URI (e.g., `file:///opt/toto`). It is also valid to pass a file path directly (without file://). `/var/quickwit`. Relative paths will be resolved with respect to the current working directory.
- S3-compatible storage URI (e.g., `s3://my-bucket/some-path`). See the [storage config](storage-config) documentation to configure S3 or S3-compatible storage providers.

### Polling configuration

By default, the File-Backed Metastore is only read once when you start a Quickwit process (searcher, indexer, ...).

You can also configure it to poll the File-Backed Metastore periodically to keep a fresh view of it. This is useful for a Searcher instance that needs to be aware of new splits published by an Indexer running in parallel.

To configure the polling interval (in seconds), add a URI fragment to the storage URI as follows: `s3://quickwit/my-indexes#polling_interval=30s`

:::note
The polling interval can be configured in seconds only; other units, such as minutes or hours, are not supported.
:::

:::tip
Amazon S3 charges $0.0004 per 1000 GET requests. Polling a metastore every 30 seconds costs $0.04 per month and index.
:::

### Examples

The following file-backed metastore URIs for instance are valid:

```markdown
s3://my-indexes
s3://quickwit/my-indexes
s3://quickwit/my-indexes#polling_interval=30s
file:///local/indices
file:///local/indices#polling_interval=30s
/local/indices
./quickwit-metastores
```

:::caution
The file-backed metastore does not support multiple instances running at the same time because it does not implement any locking mechanism to prevent concurrent writes from overwriting each other. Ensure that only one file-backed metastore instance is running at all times.
:::
