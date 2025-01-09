---
title: Command-line options
sidebar_position: 50
---

Quickwit command line tool lets you start a Quickwit server and manage indexes (create, delete, ingest), splits and sources (create, delete, toggle). To start a server, `quickwit` needs a [node config file path](../configuration/node-config.md) that you can specify with `QW_CONFIG` environment variable: `export QW_CONFIG=./config/quickwit.yaml`.

This page documents all the available commands, related options, and environment variables.

### Common Options

To manage indexes, splits and sources on a remote cluster you might need to specify the connection to a Quickwit node. The following options are supported:

| Option              | Description                 | Default                 |
|---------------------|-----------------------------|------------------------:|
| `--endpoint`        | The url of a Quickwit node. | `http://127.0.0.1:7280` |
| `--timeout`         | Command timeout.            | *See below*             |
| `--connect-timeout` | Connect timeout.            | `5s`                    |

The default timeouts are command specific:
- **search** - 1 minute
- **ingest** (without force or wait) - 1 minute
- **ingest** (with force or wait) - 30 minute
- all other operations - 10 seconds

The timeout can be expressed as in seconds, minutes, hours or days. For example:

- `10s` - 10 seconds timeout
- `1m` - 1 minute timeout
- `2h` - 2 hours timeout
- `1d` - 1 day timeout
- `none` - no timeout is applied.

:::caution

Before using Quickwit with object storage, consult our [guidelines](../operating/aws-costs.md) for deploying on AWS S3 to avoid surprises on your next bill.

:::


## Commands

[Command-line synopsis syntax](https://developers.google.com/style/code-syntax)

### Help

`quickwit` or `quickwit --help` displays the list of available commands.

`quickwit <command name> --help` displays the documentation for the command and a usage example.

### Version

`quickwit --version` displays the version. It is helpful for reporting bugs.


### Syntax

The CLI is structured into high-level commands with subcommands.
`quickwit [command] [subcommand] [args]`.

* `command`: `run`, `index`, `split`, `source` and `tool`.


<!--
    Insert auto-generated CLI docs here...
-->
## run
Starts a Quickwit node with all services enabled by default: `indexer`, `searcher`, `metastore`, `control-plane`, and `janitor`.


### Indexer service

The indexer service runs indexing pipelines assigned by the control plane.

### Searcher service 
Starts a web server at `rest_listing_address:rest_list_port` that exposes the [Quickwit REST API](rest-api.md)
where `rest_listing_address` and `rest_list_port` are defined in Quickwit config file (quickwit.yaml).
The node can optionally join a cluster using the `peer_seeds` parameter.
This list of node addresses is used to discover the remaining peer nodes in the cluster through a gossip protocol, see [chitchat](https://github.com/quickwit-oss/chitchat).

### Metastore service

The metastore service exposes Quickwit metastore over the network. This is a core internal service that is needed to operate Quickwit. As such, at least one running instance of this service is required for other services to work.

### Control plane service

The control plane service schedules indexing tasks to indexers. It listens to metastore events such as
an source create, delete, toggle, or index delete and reacts accordingly to update the indexing plan.

### Janitor service

The Janitor service runs maintenance tasks on indexes: garbage collection, documents delete, and retention policy tasks.

:::note
Quickwit needs to open the following port for cluster formation and workload distribution:

    TCP port (default is 7280) for REST API
    TCP and UDP port (default is 7280) for cluster membership protocol
    TCP port + 1 (default is 7281) for gRPC address for the distributed search

If ports are already taken, the serve command will fail.
:::
  
`quickwit  run [args]`

*Synopsis*

```bash
quickwit run
    [--config <config>]
    [--service <service>]
```

*Options*

| Option | Description | Default |
|-----------------|-------------|--------:|
| `--config` | Config file location | `config/quickwit.yaml` |
| `--service` | Services (`indexer`, `searcher`, `metastore`, `control-plane`, or `janitor`) to run. If unspecified, all the supported services are started. |  |

*Examples*

*Starts an indexer and a metastore services*
```bash
quickwit run --service indexer --service metastore --endpoint=http://127.0.0.1:7280
```

*Start a control plane, metastore and janitor services*
```bash
quickwit run --service control_plane --service metastore --service janitor --config=./config/quickwit.yaml
```

*Make a search request on a wikipedia index*
```bash
# To create wikipedia index and ingest data, go to our tutorials https://quickwit.io/docs/get-started/.
# Start a searcher.
quickwit run --service searcher --service metastore --config=./config/quickwit.yaml
# Make a request.
curl "http://127.0.0.1:7280/api/v1/wikipedia/search?query=barack+obama"

```

## index
Manages indexes: creates, updates, deletes, ingests, searches, describes...

### index create

Creates an index of ID `index` at `index-uri` configured by a [YAML config file](../configuration/index-config.md) located at `index-config`.
The index config lets you define the mapping of your document on the index and how each field is stored and indexed.
If `index-uri` is omitted, `index-uri` will be set to `{default_index_root_uri}/{index}`, more info on [Quickwit config docs](../configuration/node-config.md).
The command fails if an index already exists unless `overwrite` is passed.
When `overwrite` is enabled, the command deletes all the files stored at `index-uri` before creating a new index.
  
`quickwit index create [args]`

*Synopsis*

```bash
quickwit index create
    --index-config <index-config>
    [--overwrite]
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index-config` | Location of the index config file. |
| `--overwrite` | Overwrites pre-existing index. This will delete all existing data stored at `index-uri` before creating a new index. |

*Examples*

*Create a new index.*
```bash
# Start a Quickwit server.
quickwit run --config=./config/quickwit.yaml
# Open a new terminal and run:
curl -o wikipedia_index_config.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/wikipedia/index-config.yaml
quickwit index create --endpoint=http://127.0.0.1:7280 --index-config wikipedia_index_config.yaml

```

### index update

Updates an index using an index config file.  
`quickwit index update [args]`

*Synopsis*

```bash
quickwit index update
    --index <index>
    --index-config <index-config>
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |
| `--index-config` | Location of the index config file. |
### index clear

Clears an index: deletes all splits and resets checkpoint.  
`quickwit index clear [args]`
`quickwit index clr [args]`

*Synopsis*

```bash
quickwit index clear
    --index <index>
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | Index ID |
### index delete

Deletes an index.  
`quickwit index delete [args]`
`quickwit index del [args]`

*Synopsis*

```bash
quickwit index delete
    --index <index>
    [--dry-run]
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |
| `--dry-run` | Executes the command in dry run mode and only displays the list of splits candidates for deletion. |

*Examples*

*Delete your index*
```bash
# Start a Quickwit server.
quickwit run --service metastore --config=./config/quickwit.yaml
# Open a new terminal and run:
quickwit index delete --index wikipedia --endpoint=http://127.0.0.1:7280

```

### index describe

Displays descriptive statistics of an index.  
`quickwit index describe [args]`

*Synopsis*

```bash
quickwit index describe
    --index <index>
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |

*Examples*

*Displays descriptive statistics of your index*
```bash
# Start a Quickwit server.
quickwit run --service metastore --config=./config/quickwit.yaml
# Open a new terminal and run:
quickwit index describe --endpoint=http://127.0.0.1:7280 --index wikipedia

1. General infos
===============================================================================
Index id:                           wikipedia
Index uri:                          file:///home/quickwit-indices/qwdata/indexes/wikipedia
Number of published splits:         1
Number of published documents:      300000
Size of published splits:           448 MB

2. Statistics on splits
===============================================================================
Document count stats:
Mean ± σ in [min … max]:            300000 ± 0 in [300000 … 300000]
Quantiles [1%, 25%, 50%, 75%, 99%]: [300000, 300000, 300000, 300000, 300000]

Size in MB stats:
Mean ± σ in [min … max]:            448 ± 0 in [448 … 448]
Quantiles [1%, 25%, 50%, 75%, 99%]: [448, 448, 448, 448, 448]

```

### index list

List indexes.  
`quickwit index list [args]`
`quickwit index ls [args]`

*Examples*

*List indexes*
```bash
# Start a Quickwit server.
quickwit run --config=./config/quickwit.yaml
# Open a new terminal and run:
quickwit index list --endpoint=http://127.0.0.1:7280
# Or with alias.
quickwit index ls --endpoint=http://127.0.0.1:7280

                                    Indexes                                     
+-----------+--------------------------------------------------------+
| Index ID  |                       Index URI                        |
+-----------+--------------------------------------------------------+
| hdfs-logs | file:///home/quickwit-indices/qwdata/indexes/hdfs-logs |
+-----------+--------------------------------------------------------+
| wikipedia | file:///home/quickwit-indices/qwdata/indexes/wikipedia |
+-----------+--------------------------------------------------------+


```

### index ingest

Indexes a dataset consisting of newline-delimited JSON objects located at `input-path` or read from *stdin*.
The data is appended to the target index of ID `index` unless `overwrite` is passed. `input-path` can be a file or another command output piped into stdin.
Currently, only local datasets are supported.
By default, Quickwit's indexer will work with a heap of 2 GiB of memory. Learn how to change `heap-size` in the [index config doc page](../configuration/index-config.md).
  
`quickwit index ingest [args]`

*Synopsis*

```bash
quickwit index ingest
    --index <index>
    [--input-path <input-path>]
    [--batch-size-limit <batch-size-limit>]
    [--wait]
    [--force]
    [--commit-timeout <commit-timeout>]
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |
| `--input-path` | Location of the input file. |
| `--batch-size-limit` | Size limit of each submitted document batch. |
| `--wait` | Wait for all documents to be committed and available for search before exiting. Applies only to the last batch, see [#5417](https://github.com/quickwit-oss/quickwit/issues/5417). |
| `--force` | Force a commit after the last document is sent, and wait for all documents to be committed and available for search before exiting. Applies only to the last batch, see [#5417](https://github.com/quickwit-oss/quickwit/issues/5417). |
| `--commit-timeout` | Timeout for ingest operations that require waiting for the final commit (`--wait` or `--force`). This is different from the `commit_timeout_secs` indexing setting, which sets the maximum time before committing splits after their creation. |

*Examples*

*Indexing a dataset from a file*
```bash
# Start a Quickwit server.
quickwit run --config=./config/quickwit.yaml
# Open a new terminal and run:
curl -o wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json
quickwit index ingest --endpoint=http://127.0.0.1:7280 --index wikipedia --input-path wiki-articles-10000.json

```

*Indexing a dataset from stdin*
```bash
# Start a Quickwit server.
quickwit run --config=./config/quickwit.yaml
# Open a new terminal and run:
cat wiki-articles-10000.json | quickwit index ingest --endpoint=http://127.0.0.1:7280 --index wikipedia

```

### index search

Searches an index with ID `--index` and returns the documents matching the query specified with `--query`.
More details on the [query language page](query-language.md).
The offset of the first hit returned and the number of hits returned can be set with the `start-offset` and `max-hits` options.
It's possible to override the default search fields `search-fields` option to define the list of fields that Quickwit will search into if 
the user query does not explicitly target a field in the query. Quickwit will return snippets of the matching content when requested via the `snippet-fields` options.
Search can also be limited to a time range using the `start-timestamp` and `end-timestamp` options.
These timestamp options are useful for boosting query performance when using a time series dataset.

:::warning
The `start_timestamp` and `end_timestamp` should be specified in seconds regardless of the timestamp field precision. The timestamp field precision only affects the way it's stored as fast-fields, whereas the document filtering is always performed in seconds.
:::
  
`quickwit index search [args]`

*Synopsis*

```bash
quickwit index search
    --index <index>
    --query <query>
    [--aggregation <aggregation>]
    [--max-hits <max-hits>]
    [--start-offset <start-offset>]
    [--search-fields <search-fields>]
    [--snippet-fields <snippet-fields>]
    [--start-timestamp <start-timestamp>]
    [--end-timestamp <end-timestamp>]
    [--sort-by-score]
```

*Options*

| Option | Description | Default |
|-----------------|-------------|--------:|
| `--index` | ID of the target index |  |
| `--query` | Query expressed in natural query language ((barack AND obama) OR "president of united states"). Learn more on https://quickwit.io/docs/reference/search-language. |  |
| `--aggregation` | JSON serialized aggregation request in tantivy/elasticsearch format. |  |
| `--max-hits` | Maximum number of hits returned. | `20` |
| `--start-offset` | Offset in the global result set of the first hit returned. | `0` |
| `--search-fields` | List of fields that Quickwit will search into if the user query does not explicitly target a field in the query. It overrides the default search fields defined in the index config. Space-separated list, e.g. "field1 field2".  |  |
| `--snippet-fields` | List of fields that Quickwit will return snippet highlight on. Space-separated list, e.g. "field1 field2".  |  |
| `--start-timestamp` | Filters out documents before that timestamp (time-series indexes only). |  |
| `--end-timestamp` | Filters out documents after that timestamp (time-series indexes only). |  |
| `--sort-by-score` | Sorts documents by their BM25 score. |  |

*Examples*

*Searching a index*
```bash
# Start a Quickwit server.
quickwit run --config=./config/quickwit.yaml
# Open a new terminal and run:
quickwit index search --endpoint=http://127.0.0.1:7280 --index wikipedia --query "Barack Obama"
# If you have jq installed.
quickwit index search --endpoint=http://127.0.0.1:7280 --index wikipedia --query "Barack Obama" | jq '.hits[].title'

```

*Sorting documents by their BM25 score*
```bash
# Start a Quickwit server.
quickwit run --config=./config/quickwit.yaml
# Open a new terminal and run:
quickwit index search --endpoint=http://127.0.0.1:7280 --index wikipedia --query "obama" --sort-by-score

```

*Limiting the result set to 50 hits*
```bash
# Start a Quickwit server.
quickwit run --config=./config/quickwit.yaml
# Open a new terminal and run:
quickwit index search --endpoint=http://127.0.0.1:7280 --index wikipedia --query "Barack Obama" --max-hits 50
# If you have jq installed.
quickwit index search --endpoint=http://127.0.0.1:7280 --index wikipedia --query "Barack Obama" --max-hits 50 | jq '.num_hits'

```

*Looking for matches in the title only*
```bash
# Start a Quickwit server.
quickwit run --config=./config/quickwit.yaml
# Open a new terminal and run:
quickwit index search --endpoint=http://127.0.0.1:7280 --index wikipedia --query "obama" --search-fields body
# If you have jq installed.
quickwit index search --endpoint=http://127.0.0.1:7280 --index wikipedia --query "obama" --search-fields body | jq '.hits[].title'

```

## source
Manages sources: creates, updates, deletes sources...

### source create

Adds a new source to an index.  
`quickwit source create [args]`

*Synopsis*

```bash
quickwit source create
    --index <index>
    --source-config <source-config>
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |
| `--source-config` | Path to source config file. Please, refer to the documentation for more details. |
### source enable

Enables a source for an index.  
`quickwit source enable [args]`

*Synopsis*

```bash
quickwit source enable
    --index <index>
    --source <source>
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |
| `--source` | ID of the source. |
### source disable

Disables a source for an index.  
`quickwit source disable [args]`

*Synopsis*

```bash
quickwit source disable
    --index <index>
    --source <source>
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |
| `--source` | ID of the source. |
### source ingest-api

Enables/disables the ingest API of an index.  
`quickwit source ingest-api [args]`

*Synopsis*

```bash
quickwit source ingest-api
    --index <index>
    [--enable]
    [--disable]
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |
| `--enable` | Enables the ingest API. |
| `--disable` | Disables the ingest API. |
### source delete

Deletes a source from an index.  
`quickwit source delete [args]`
`quickwit source del [args]`

*Synopsis*

```bash
quickwit source delete
    --index <index>
    --source <source>
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |
| `--source` | ID of the source. |

*Examples*

*Delete a `wikipedia-source` source*
```bash
# Start a Quickwit server.
quickwit run --service metastore --config=./config/quickwit.yaml
# Open a new terminal and run:
quickwit source delete --endpoint=http://127.0.0.1:7280 --index wikipedia --source wikipedia-source

```

### source describe

Describes a source.  
`quickwit source describe [args]`
`quickwit source desc [args]`

*Synopsis*

```bash
quickwit source describe
    --index <index>
    --source <source>
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |
| `--source` | ID of the source. |
### source list

Lists the sources of an index.  
`quickwit source list [args]`
`quickwit source ls [args]`

*Synopsis*

```bash
quickwit source list
    --index <index>
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |

*Examples*

*List `wikipedia` index sources*
```bash
# Start a Quickwit server.
quickwit run --service metastore --config=./config/quickwit.yaml
# Open a new terminal and run:
quickwit source list --endpoint=http://127.0.0.1:7280 --index wikipedia

```

### source reset-checkpoint

Resets a source checkpoint.  
`quickwit source reset-checkpoint [args]`
`quickwit source reset [args]`

*Synopsis*

```bash
quickwit source reset-checkpoint
    --index <index>
    --source <source>
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | Index ID |
| `--source` | Source ID |
## split
Manages splits: lists, describes, marks for deletion...

### split list

Lists the splits of an index.  
`quickwit split list [args]`
`quickwit split ls [args]`

*Synopsis*

```bash
quickwit split list
    --index <index>
    [--offset <offset>]
    [--limit <limit>]
    [--states <states>]
    [--create-date <create-date>]
    [--start-date <start-date>]
    [--end-date <end-date>]
    [--output-format <output-format>]
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | Target index ID |
| `--offset` | Number of splits to skip. |
| `--limit` | Maximum number of splits to retrieve. |
| `--states` | Selects the splits whose states are included in this comma-separated list of states. Possible values are `staged`, `published`, and `marked`. |
| `--create-date` | Selects the splits whose creation dates are before this date. |
| `--start-date` | Selects the splits that contain documents after this date (time-series indexes only). |
| `--end-date` | Selects the splits that contain documents before this date (time-series indexes only). |
| `--output-format` | Output format. Possible values are `table`, `json`, and `pretty-json`. |
### split describe

Displays metadata about a split.  
`quickwit split describe [args]`
`quickwit split desc [args]`

*Synopsis*

```bash
quickwit split describe
    --index <index>
    --split <split>
    [--verbose]
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |
| `--split` | ID of the target split |
| `--verbose` | Displays additional metadata about the hotcache. |
### split mark-for-deletion

Marks one or multiple splits of an index for deletion.  
`quickwit split mark-for-deletion [args]`
`quickwit split mark [args]`

*Synopsis*

```bash
quickwit split mark-for-deletion
    --index <index>
    --splits <splits>
    [--yes]
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | Target index ID |
| `--splits` | Comma-separated list of split IDs |
| `--yes` | Assume "yes" as an answer to all prompts and run non-interactively. |
## tool
Performs utility operations. Requires a node config.

### tool local-ingest

Indexes NDJSON documents locally.  
`quickwit tool local-ingest [args]`

*Synopsis*

```bash
quickwit tool local-ingest
    --index <index>
    [--input-path <input-path>]
    [--input-format <input-format>]
    [--overwrite]
    [--transform-script <transform-script>]
    [--keep-cache]
```

*Options*

| Option | Description | Default |
|-----------------|-------------|--------:|
| `--index` | ID of the target index |  |
| `--input-path` | Location of the input file. |  |
| `--input-format` | Format of the input data. | `json` |
| `--overwrite` | Overwrites pre-existing index. |  |
| `--transform-script` | VRL program to transform docs before ingesting. |  |
| `--keep-cache` | Does not clear local cache directory upon completion. |  |
### tool extract-split

Downloads and extracts a split to a directory.  
`quickwit tool extract-split [args]`

*Synopsis*

```bash
quickwit tool extract-split
    --index <index>
    --split <split>
    [--target-dir <target-dir>]
```

*Options*

| Option | Description |
|-----------------|-------------|
| `--index` | ID of the target index |
| `--split` | ID of the target split |
| `--target-dir` | Directory to extract the split to. |
### tool gc

Garbage collects stale staged splits and splits marked for deletion.  
:::note
Intermediate files are created while executing Quickwit commands.
These intermediate files are always cleaned at the end of each successfully executed command.
However, failed or interrupted commands can leave behind intermediate files that need to be removed.
Also, note that using a very short grace period (like seconds) can cause the removal of intermediate files being operated on, especially when using Quickwit concurrently on the same index.
In practice, you can settle with the default value (1 hour) and only specify a lower value if you really know what you are doing.

:::
`quickwit tool gc [args]`

*Synopsis*

```bash
quickwit tool gc
    --index <index>
    [--grace-period <grace-period>]
    [--dry-run]
```

*Options*

| Option | Description | Default |
|-----------------|-------------|--------:|
| `--index` | ID of the target index |  |
| `--grace-period` | Threshold period after which stale staged splits are garbage collected. | `1h` |
| `--dry-run` | Executes the command in dry run mode and only displays the list of splits candidates for garbage collection. |  |

<!--
    End of auto-generated CLI docs
-->

## Environment Variables

### QW_CLUSTER_ENDPOINT

Specifies the address of the cluster to connect to. Management commands `index`, `split` and `source` require the `cluster_endpoint`, which you can set once and for all with the `QW_CLUSTER_ENDPOINT` environment variable.

### QW_CONFIG

Specifies the path to the [quickwit config](../configuration/node-config.md). Commands `run` and `tools` require the `config`, which you can set once and for all with the `QW_CONFIG` environment variable.

*Example*

`export QW_CONFIG=config/quickwit.yaml`

### QW_DISABLE_TELEMETRY

Disables [telemetry](../telemetry.md) when set to any non-empty value.

*Example*

`QW_DISABLE_TELEMETRY=1 quickwit help`

### QW_POSTGRES_SKIP_MIGRATIONS

Don't run database migrations (but verify that migrations were run successfully before, and no that unknown migration was run).

### QW_POSTGRES_SKIP_MIGRATION_LOCKING

Don't lock the database during migration. This may increase compatibility with alternative databases using the PostgreSQL wire protocol. However, it
is dangerous to use this if you can't guarantee that only one node will run the migrations.

### RUST_LOG

Configure quickwit log level.

*Examples*

```
# run with higher verbosity
RUST_LOG=debug quickwit run
# run with log level info, except for indexing related logs
RUST_LOG=info,quickwit_indexing=debug quickwit run
```
