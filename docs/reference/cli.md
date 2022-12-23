---
title: Command-line options
sidebar_position: 4
---

Quickwit command line tool lets you create, ingest, search, start search and indexer servers. For configuration, `quickwit` needs a [config file path](../configuration/node-config.md) that you can specify with `QW_CONFIG` environment variable: `export QW_CONFIG=./config/quickwit.yaml`.

This page documents all the available commands, related options, and environment variables.

:::caution

Before using Quickwit with object storage, consult our [guidelines](../guides/aws-costs.md) for deploying on AWS S3 to avoid surprises on your next bill.

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

* `command`: `index`, `split`, `source` and `service`.


<!--
    Insert auto-generated CLI docs here...
-->

## run
Runs quickwit services. By default, `metastore`, `indexer`, `searcher` and `janitor` are started.

### Indexer service

The indexer service will list indexes and their associated sources, and run an indexing pipeline for every single source.

### Janitor service

The Janitor service is a maintenance service in charge of keeping Quickwit cluster clean by running maintenance tasks. Garbage collection, executing delete tasks and applying retention policies to indexes are all the Janitor service duties.

### Metastore service

The metastore service exposes Quickwit metastore over the network. This is a core internal service that is needed to operate Quickwit. As such, at least one running instance of this service is required for other services to work.

### Searcher service 
Starts a web server at `rest_listing_address:rest_list_port` that exposes the [Quickwit REST API](rest-api.md)
where `rest_listing_address` and `rest_list_port` are defined in Quickwit config file (quickwit.yaml).
The node can optionally join a cluster using the `peer_seeds` parameter.
This list of node addresses is used to discover the remaining peer nodes in the cluster through a gossip protocol, see [chitchat](https://github.com/quickwit-oss/chitchat).

:::note
Behind the scenes, Quickwit needs to open the following port for cluster formation and workload distribution:

    TCP port (default is 7280) for REST API
    TCP and UDP port (default is 7280) for cluster membership protocol
    TCP port + 1 (default is 7281) for gRPC address for the distributed search

If ports are already taken, the serve command will fail.
:::
  
`quickwit  run [args]`

*Synopsis*

```bash
quickwit run
    [--service <service>]
```

*Options*

`--service` Services (indexer|searcher|janitor|metastore) to run. If unspecified, all the supported services are started. \

*Examples*

*Starts an indexer service*
```bash
quickwit run --service indexer --config=./config/quickwit.yaml
```

*Start a searcher service*
```bash
quickwit run --service searcher --config=./config/quickwit.yaml
```

*Start a janitor service*
```bash
quickwit run --service janitor --config=./config/quickwit.yaml
```

*Start a metastore service*
```bash
quickwit run --service metastore --config=./config/quickwit.yaml
```

*Make a search request on a wikipedia index*
```bash
# To create wikipedia index and ingest data, go to our tutorial https://quickwit.io/docs/get-started/quickstart.
# Start a searcher.
quickwit run --service searcher --config=./config/quickwit.yaml
# Make a request.
curl "http://127.0.0.1:7280/api/v1/wikipedia/search?query=barack+obama"

```

*Make a search stream request on a Github archive index*
```bash
# Create gh-archive index.
curl -o gh_archive_index_config.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/gh-archive/index-config.yaml
quickwit index create --index-config gh_archive_index_config.yaml --config ./config/quickwit.yaml
# Download a data sample and ingest it.
curl https://quickwit-datasets-public.s3.amazonaws.com/gh-archive-2022-01-text-only-10000.json.gz | gunzip | cargo r index ingest --index gh-archive --config=./config/quickwit.yaml
# Start server.
quickwit run --service searcher --config=./config/quickwit.yaml
# Finally make the search stream request.
curl "http://127.0.0.1:7280/api/v1/gh-archive/search/stream?query=log4j&fastField=id&outputFormat=csv"
# Make a search stream request with HTTP2.
curl --http2-prior-knowledge "http://127.0.0.1:7280/api/v1/gh-archive/search/stream?query=log4j&fastField=id&outputFormat=csv"

```

*Add a source to an index and start an Indexer*
```bash
quickwit source add --index wikipedia --source wikipedia-source --type file --params '{"filepath":"wiki-articles-10000.json"}'
quickwit run --service indexer --indexes wikipedia --config=./config/quickwit.yaml

```

## index
Create your index, ingest data, search, describe... every command you need to manage indexes.

### index list

List indexes.  
`quickwit index list [args]`
`quickwit index ls [args]`

*Synopsis*

```bash
quickwit index list
    [--metastore-uri <metastore-uri>]
```

*Options*

`--metastore-uri` Metastore URI. Override the `metastore_uri` parameter defined in the config file. Defaults to file-backed, but could be Amazon S3 or PostgreSQL. \

*Examples*

*List indexes*
```bash
quickwit index list --config ./config/quickwit.yaml
# Or with alias.
quickwit index ls --config ./config/quickwit.yaml

                                    Indexes                                     
+-----------+--------------------------------------------------------+
| Index ID  |                       Index URI                        |
+-----------+--------------------------------------------------------+
| hdfs-logs | file:///home/quickwit-indices/qwdata/indexes/hdfs-logs |
+-----------+--------------------------------------------------------+
| wikipedia | file:///home/quickwit-indices/qwdata/indexes/wikipedia |
+-----------+--------------------------------------------------------+


```

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
    [--yes]
```

*Options*

`--index-config` Location of the index config file. \
`--overwrite` Overwrites pre-existing index. This will delete all existing data stored at `index-uri` before creating a new index. \
`--yes` Assume "yes" as an answer to all prompts and run non-interactively. \

*Examples*

*Create a new index.*
```bash
curl -o wikipedia_index_config.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/wikipedia/index-config.yaml
quickwit index create --index-config wikipedia_index_config.yaml  --config=./config/quickwit.yaml

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
    [--overwrite]
    [--keep-cache]
```

*Options*

`--index` ID of the target index \
`--input-path` Location of the input file. \
`--overwrite` Overwrites pre-existing index. \
`--keep-cache` Does not clear local cache directory upon completion. \

*Examples*

*Indexing a dataset from a file*
```bash
curl -o wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json
quickwit index ingest --index wikipedia --config=./config/quickwit.yaml --input-path wiki-articles-10000.json

```

*Indexing a dataset from stdin*
```bash
cat hdfs-log.json | quickwit index ingest --index wikipedia --config=./config/quickwit.yaml
```

### index ingest-api

Enables/disables the ingest API of an index.  
`quickwit index ingest-api [args]`

*Synopsis*

```bash
quickwit index ingest-api
    --index <index>
    --enable
    [--disable]
```

*Options*

`--index` ID of the target index \
`--enable` Enables the ingest API. \
`--disable` Disables the ingest API. \
### index describe

Displays descriptive statistics of an index: number of published splits, number of documents, splits min/max timestamps, size of splits.  
`quickwit index describe [args]`

*Synopsis*

```bash
quickwit index describe
    --index <index>
```

*Options*

`--index` ID of the target index \

*Examples*

*Displays descriptive statistics of your index*
```bash
quickwit index describe --index wikipedia --config ./config/quickwit.yaml

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

`--index` ID of the target index \
`--query` Query expressed in natural query language ((barack AND obama) OR "president of united states"). Learn more on https://quickwit.io/docs/reference/search-language. \
`--aggregation` JSON serialized aggregation request in tantivy/elasticsearch format. \
`--max-hits` Maximum number of hits returned. (default: 20) \
`--start-offset` Offset in the global result set of the first hit returned. (default: 0) \
`--search-fields` List of fields that Quickwit will search into if the user query does not explicitly target a field in the query. It overrides the default search fields defined in the index config. Space-separated list, e.g. "field1 field2".  \
`--snippet-fields` List of fields that Quickwit will return snippet highlight on. Space-separated list, e.g. "field1 field2".  \
`--start-timestamp` Filters out documents before that timestamp (time-series indexes only). \
`--end-timestamp` Filters out documents after that timestamp (time-series indexes only). \
`--sort-by-score` Setting this flag calculates and sorts documents by their BM25 score. \

*Examples*

*Searching a index*
```bash
quickwit index search --index wikipedia --query "Barack Obama" --config ./config/quickwit.yaml
# If you have jq installed.
quickwit index search --index wikipedia --query "Barack Obama" --config ./config/quickwit.yaml | jq '.hits[].title'

```

*Sorting documents by their BM25 score*
```bash
quickwit index search --index wikipedia --query "Barack Obama" --sort-by-score --config ./config/quickwit.yaml
```

*Limiting the result set to 50 hits*
```bash
quickwit index search --index wikipedia --query "Barack Obama" --max-hits 50 --config ./config/quickwit.yaml
# If you have jq installed.
quickwit index search --index wikipedia --query "Barack Obama" --max-hits 50 --config ./config/quickwit.yaml | jq '.num_hits'

```

*Looking for matches in the title only*
```bash
quickwit index search --index wikipedia --query "search" --search-fields title --config ./config/quickwit.yaml
# If you have jq installed.
quickwit index search --index wikipedia --query "search" --search-fields title --config ./config/quickwit.yaml | jq '.hits[].title'

```

### index gc

Garbage collects stale staged splits and splits marked for deletion.  
:::note
Intermediate files are created while executing Quickwit commands.
These intermediate files are always cleaned at the end of each successfully executed command.
However, failed or interrupted commands can leave behind intermediate files that need to be removed.
Also, note that using a very short grace period (like seconds) can cause the removal of intermediate files being operated on, especially when using Quickwit concurrently on the same index.
In practice, you can settle with the default value (1 hour) and only specify a lower value if you really know what you are doing.

:::
`quickwit index gc [args]`

*Synopsis*

```bash
quickwit index gc
    --index <index>
    [--grace-period <grace-period>]
    [--dry-run]
```

*Options*

`--index` ID of the target index \
`--grace-period` Threshold period after which stale staged splits are garbage collected. (default: 1h) \
`--dry-run` Executes the command in dry run mode and only displays the list of splits candidates for garbage collection. \
### index clear

Clears and index. Deletes all its splits and resets its checkpoint. This operation is destructive and cannot be undone, proceed with caution.  
`quickwit index clear [args]`
`quickwit index clr [args]`

*Synopsis*

```bash
quickwit index clear
    --index <index>
    [--yes]
```

*Options*

`--index` Index ID \
`--yes`  \
### index delete

Deletes an index. This operation is destructive and cannot be undone, proceed with caution.  
`quickwit index delete [args]`
`quickwit index del [args]`

*Synopsis*

```bash
quickwit index delete
    --index <index>
    [--dry-run]
```

*Options*

`--index` ID of the target index \
`--dry-run` Executes the command in dry run mode and only displays the list of splits candidates for deletion. \

*Examples*

*Delete your index*
```bash
quickwit index delete --index wikipedia --config ./config/quickwit.yaml
```

## source
Manages sources.

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

`--index` ID of the target index \
`--source-config` Path to source config file. Please, refer to the documentation for more details. \
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

`--index` ID of the target index \
`--source` ID of the source. \
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

`--index` ID of the target index \
`--source` ID of the source. \
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

`--index` ID of the target index \
`--source` ID of the source. \

*Examples*

*Delete a `wikipedia-source` source*
```bash
quickwit source delete --index wikipedia --source wikipedia-source --config ./config/quickwit.yaml

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

`--index` ID of the target index \
`--source` ID of the source. \

*Examples*

*Describe a `wikipedia-source` source*
```bash
quickwit source describe --index wikipedia --source wikipedia-source --config ./config/quickwit.yaml

```

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

`--index` ID of the target index \

*Examples*

*List `wikipedia` index sources*
```bash
quickwit source list --index wikipedia --config ./config/quickwit.yaml

```

### source reset-checkpoint

Resets a source checkpoint. This operation is destructive and cannot be undone. Proceed with caution.  
`quickwit source reset-checkpoint [args]`
`quickwit source reset [args]`

*Synopsis*

```bash
quickwit source reset-checkpoint
    --index <index>
    --source <source>
```

*Options*

`--index` Index ID \
`--source` Source ID \
## split
Performs operations on splits (list, describe, mark for deletion, extract).

### split list

Lists the splits of an index.  
`quickwit split list [args]`
`quickwit split ls [args]`

*Synopsis*

```bash
quickwit split list
    --index <index>
    [--states <states>]
    [--create-date <create-date>]
    [--start-date <start-date>]
    [--end-date <end-date>]
    [--tags <tags>]
    [--output-format <output-format>]
```

*Options*

`--index` Target index ID \
`--states` Selects the splits whose states are included in this comma-separated list of states. Possible values are `staged`, `published`, and `marked`. \
`--create-date` Selects the splits whose creation dates are before this date. \
`--start-date` Selects the splits that contain documents after this date (time-series indexes only). \
`--end-date` Selects the splits that contain documents before this date (time-series indexes only). \
`--tags` Selects the splits whose tags are all included in this comma-separated list of tags. \
`--output-format` Output format. Possible values are `table`, `json`, and `prettyjson`. \
### split mark-for-deletion

Marks one or multiple splits of an index for deletion.  
`quickwit split mark-for-deletion [args]`
`quickwit split mark [args]`

*Synopsis*

```bash
quickwit split mark-for-deletion
    --index <index>
    --splits <splits>
```

*Options*

`--index` Target index ID \
`--splits` Comma-separated list of split IDs \

<!--
    End of auto-generated CLI docs
-->

## Environment Variables

### QW_CONFIG

Specifies the path to the [quickwit config](../configuration/node-config.md). Every command requires the `config`, which you can set once and for all with the `QW_CONFIG` environment variable.

*Example*

`export QW_CONFIG=config/quickwit.yaml`


### QW_DISABLE_TELEMETRY

Disables [telemetry](../telemetry.md) when set to any non-empty value.

*Example*

`QW_DISABLE_TELEMETRY=1 quickwit help`
