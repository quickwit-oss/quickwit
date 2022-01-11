---
title: Metastore configuration
position: 3
---

Quickwit needs a place to store meta-information about its indexes.

For instance:

- The index configuration.
- Meta-information about its splits. For instance, their IDs, the number of documents they contain, their sizes, their min/max timestamp, and the set of tags present in the split.
- The different sources checkpoints.
- Some extra information such as the index creation time.

The metastore is entirely defined by a single URI. One can set it by editing the `metastore_uri` parameter of the [Quickwit configuration file](https://www.notion.so/Quickwit-configuration-MERGED-3fab5a181a9a43cba83db2fb25b46729) (often named `quickwit.yaml`).

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
postgres://host1:123,host2:456/mydb
```

The database has to be created in advance.

On its first execution, Quickwit will transparently create the necessary tables.

Likewise, if you upgrade Quickwit to a version that includes some changes in the PostgreSQL schema, Quickwit will transparently operate the migration startup.

# File-backed metastore

For convenience, Quickwit also makes it possible to store its metadata in files using a file-backed metastore. In that case, Quickwit will write one file per index.

The metastore is then configured by passing a [Storage URI](https://www.notion.so/Storage-URI-APPROVED-176d8befb8d144fb820bcd0df077a728) that will serve as the root of the metastore storage.

The metadata file associated with a given index will then be stored under 

  `[storage_uri]/[index_id]/metastore.json`  

For the moment, Quickwit supports two types of storage types:

- a local file system URI (e.g., `file:///opt/toto`). It is also valid to pass a file path directly (without file://). `/var/quickwit`. Relative paths will be resolved with respect to the current working directory.
- S3-compatible storage URI (e.g. `s3://my-bucket/some-path`] ). See the [Storage URI](https://www.notion.so/Storage-URI-APPROVED-176d8befb8d144fb820bcd0df077a728) documentation to configure S3 or S3-compatible storage.

### Polling configuration

By default, the File-Backed Metastore is only read once when you start a Quickwit process (searcher, indexer,...).

You can also configure it to poll the File-Backed Metastore periodically to keep a fresh view of it. This is useful for a Searcher instance that needs to be aware of new splits published by an Indexer running in parallel.

To configure the polling interval (in seconds only), add a URI fragment to the storage URI like this: `s3://quickwit/my-indexes#polling_interval=30s`

<aside>
👌 Amazon S3 charges $0.0004 per 1000 GET requests. Polling a metastore every 30 seconds will induce a cost of $0.04 per month and per index.

</aside>

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

<aside>
⛔ The file-backed metastore does not allow concurrent writes. For this reason, it should not be used in distributed settings. 
Running several indexer services on the same file-backed metastore can lead to the corruption of the metastore.
Running several search services, on the other hand, is perfectly safe.

</aside>