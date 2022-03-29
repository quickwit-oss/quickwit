---
title: CLI Reference
sidebar_position: 1
---

Quickwit command line tool lets you create, ingest, search, start search and indexer servers. For configuration, `quickwit` needs a [config file path](quickwit-config.md) that you can specify with `QW_CONFIG` environment variable: `export QW_CONFIG=./config/quickwit.yaml`.

This page documents all the available commands, related options, and environment variables.

:::caution

Before using Quickwit with object storage, check out our [advice](../administration/cloud-env.md) for deploying on AWS S3 to avoid some nasty surprises at the end of the month.

:::


## Commands

[Command-line synopsis syntax](https://developers.google.com/style/code-syntax)

### Help

`quickwit` or `quickwit --help` displays the list of available commands.

`quickwit <command name> --help` displays the documentation for the command and a usage example.


### Note on telemetry
Quickwit collects some [anonymous usage data](telemetry.md), you can disable it. When it's enabled, you will see this
output:
```
quickwit
Quickwit 0.1.0 (commit-hash: 98de4ce)

Index your dataset on object storage & make it searchable from the command line.
  Find more information at https://quickwit.io/docs

Telemetry: enabled

[...]
```

The line `Telemetry enabled` disappears when you disable it.


### Version

`quickwit --version` displays the version. It is helpful for reporting bugs.


### Syntax

The CLI is structured into high-level commands with subcommands.
`quickwit [command] [subcommand] [args]`.

* `command`: `index`, `split`, `source` and `service`.



{/* Insert auto generated CLI docs from here. */}



## index
Create your index, ingest data, search, describe... every command you need to manage indexes.

### index create

Creates an index of ID `index` at `index-uri` configured by a [YAML config file](index-config.md) located at `index-config`.
The index config lets you define the mapping of your document on the index and how each field is stored and indexed.
If `index-uri` is omitted, `index-uri` will be set to `{default_index_root_uri}/{index}`, more info on [Quickwit config docs](quickwit-config.md).
The command fails if an index already exists unless `overwrite` is passed.
When `overwrite` is enabled, the command deletes all the files stored at `index-uri` before creating a new index.

`quickwit index create [args]`

*Synopsis*

```bash
quickwit index create
    --index-config <index-config>
    --config <config>
    [--overwrite]
```

*Options*

`--index-config` Location of the index config file.
`--config` Quickwit config file.
`--overwrite` Overwrites pre-existing index.

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
By default, Quickwit's indexer will work with a heap of 2 GiB of memory. Learn how to change `heap-size` in the [index config doc page](index-config.md).

`quickwit index ingest [args]`

*Synopsis*

```bash
quickwit index ingest
    --index <index>
    --config <config>
    [--data-dir <data-dir>]
    [--input-path <input-path>]
    [--overwrite]
    [--clean_cache]
```

*Options*

`--index` ID of the target index.
`--config` Quickwit config file.
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.
`--input-path` Location of the input file.
`--overwrite` Overwrites pre-existing index.
`--clean_cache` Removes split cache from local split store.

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

### index describe

Displays descriptive statistics of an index: number of published splits, number of documents, splits min/max timestamps, size of splits.
`quickwit index describe [args]`

*Synopsis*

```bash
quickwit index describe
    --index <index>
    --config <config>
```

*Options*

`--index` ID of the target index.
`--config` Quickwit config file.

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
It's possible to override the default search fields `search-fields` option to define the list of fields that Quickwit will search into if the user query does not explicitly target a field in the query.
Search can also be limited to a time range using the `start-timestamp` and `end-timestamp` options.
These timestamp options are useful for boosting query performance when using a time series dataset.

`quickwit index search [args]`

*Synopsis*

```bash
quickwit index search
    --index <index>
    --config <config>
    --query <query>
    [--max-hits <max-hits>]
    [--start-offset <start-offset>]
    [--search-fields <search-fields>]
    [--start-timestamp <start-timestamp>]
    [--end-timestamp <end-timestamp>]
```

*Options*

`--index` ID of the target index.
`--config` Quickwit config file.
`--query` Query expressed in natural query language (barack AND obama) OR "president of united states"). Learn more on [query language](./query-language)
`--max-hits` Maximum number of hits returned. (Default: 20)
`--start-offset` Offset in the global result set of the first hit returned. (Default: 0)
`--search-fields` List of fields that Quickwit will search into if the user query does not explicitly target a field in the query. It overrides the default search fields defined in the index config. Space-separated list, e.g. "field1 field2".
`--start-timestamp` Filters out documents before that timestamp (time-series indexes only).
`--end-timestamp` Filters out documents after that timestamp (time-series indexes only).

*Examples*

*Searching a index*
```bash
quickwit index search --index wikipedia --query "Barack Obama" --config ./config/quickwit.yaml
# If you have jq installed.
quickwit index search --index wikipedia --query "Barack Obama" --config ./config/quickwit.yaml | jq '.hits[].title'

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
    --config <config>
    [--data-dir <data-dir>]
    [--grace-period <grace-period>]
    [--dry-run]
```

*Options*

`--index` ID of the target index.
`--config` Quickwit config file.
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.
`--grace-period` Threshold period after which stale staged splits are garbage collected. (Default: 1h)
`--dry-run` Executes the command in dry run mode and only displays the list of splits candidates for garbage collection.
### index delete

Delete an index.
`quickwit index delete [args]`

*Synopsis*

```bash
quickwit index delete
    --index <index>
    --config <config>
    [--data-dir <data-dir>]
    [--dry-run]
```

*Options*

`--index` ID of the target index.
`--config` Quickwit config file.
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.
`--dry-run` Executes the command in dry run mode and only displays the list of splits candidate for deletion.

*Examples*

*Delete your index*
```bash
quickwit index delete --index wikipedia --config ./config/quickwit.yaml
```

## split
Operations (list, add, delete, describe...) on splits.

### split list

Lists the splits of an index.
`quickwit split list [args]`

*Synopsis*

```bash
quickwit split list
    --index <index>
    [--states <states>]
    [--start-date <start-date>]
    [--end-date <end-date>]
    [--tags <tags>]
    --config <config>
    [--data-dir <data-dir>]
```

*Options*

`--index` ID of the target index.
`--states` Comma-separated list of split states to filter on.
Possible values are `staged`, `published`, and `marked`.

`--start-date` Filters out splits containing documents from this timestamp onwards (time-series indexes only).
`--end-date` Filters out splits containing documents before this timestamp (time-series indexes only).
`--tags` Comma-separated list of tags, only splits that contain all of the tags will be returned.
`--config` Quickwit config file.
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.
## service
Launches services.

### service run

Starts a service. Currently, the only services available are `indexer` and `searcher`.
`quickwit service run [args]`
### service run searcher

Starts a web server at `rest_listing_address:rest_list_port` that exposes the [Quickwit REST API](rest-api.md)
where `rest_listing_address` and `rest_list_port` are defined in Quickwit config file (quickwit.yaml).
The node can optionally join a cluster using the `peer_seeds` parameter.
This list of node addresses is used to discover the remaining peer nodes in the cluster through a gossip protocol (SWIM).

:::note
Behind the scenes, Quickwit needs to open the following port for cluster formation and workload distribution:

    TCP port (default is 7280) for REST API
    TCP and UDP port (default is 7280) for cluster membership protocol
    TCP port + 1 (default is 7281) for gRPC address for the distributed search

If ports are already taken, the serve command will fail.

:::
`quickwit service run searcher [args]`

*Synopsis*

```bash
quickwit service run searcher
    --config <config>
    [--data-dir <data-dir>]
```

*Options*

`--config` Quickwit config file.
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.

*Examples*

*Start a Searcher*
```bash
quickwit service run searcher --config=./config/quickwit.yaml
```

*Make a search request on a wikipedia index*
```bash
# To create wikipedia index and ingest data, go to our tutorial https://quickwit.io/docs/get-started/quickstart.
# Start a searcher.
quickwit service run searcher --config=./config/quickwit.yaml
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
quickwit service run searcher --config=./config/quickwit.yaml
# Finally make the search stream request.
curl "http://127.0.0.1:7280/api/v1/gh-archive/search/stream?query=log4j&fastField=id&outputFormat=csv"
# Make a search stream request with HTTP2.
curl --http2-prior-knowledge "http://127.0.0.1:7280/api/v1/gh-archive/search/stream?query=log4j&fastField=id&outputFormat=csv"
```

### service run indexer

Starts an indexing server that consumes the sources of index IDs passed in `--indexes` argument.

`quickwit service run indexer [args]`

*Synopsis*

```bash
quickwit service run indexer
    --config <config>
    [--data-dir <data-dir>]
    --indexes <indexes>
```

*Options*

`--config` Quickwit config file.
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.
`--indexes` IDs of the indexes to run the indexer for.

*Examples*

*Add a source to an index and start an Indexer*
```bash
quickwit source add --index wikipedia --source wikipedia-source --type file --params '{"filepath":"wiki-articles-10000.json"}'
quickwit service run indexer --indexes wikipedia --config=./config/quickwit.yaml

```

## source
Manages sources.

### source add

Adds a new source.
`quickwit source add [args]`

*Synopsis*

```bash
quickwit source add
    --index <index>
    --source <source>
    --type <type>
    --params <params>
    --config <config>
```

*Options*

`--index` ID of the target index.
`--source` ID of the source.
`--type` Type of the source. Available types are: `file` and `kafka`.
`--params` Parameters for the source formatted as a JSON object passed inline or via a file. Parameters are source-specific. Please, refer to the source's documentation for more details.
`--config` Quickwit config file.

*Examples*

*Add a file source to `wikipedia` index*
```bash
quickwit source add --index wikipedia --source wikipedia-source --type file --params '{"filepath":"wiki-articles-10000.json"}' --config ./config/quickwit.yaml
```

*Add a Kafka source to `wikipedia` index*
```bash
cat << EOF > wikipedia-kafka-source.json
{
  "topic": "wikipedia",
  "client_params": {
    "bootstrap.servers": "localhost:9092",
    "group.id": "my-group-id",
    "security.protocol": "SSL"
  }
}
EOF
quickwit source add --index wikipedia --source wikipedia-source --type kafka --params wikipedia-kafka-source.json --config ./config/quickwit.yaml
```

### source delete

Deletes a source.
`quickwit source delete [args]`

*Synopsis*

```bash
quickwit source delete
    --index <index>
    --source <source>
    --config <config>
```

*Options*

`--index` ID of the target index.
`--source` ID of the target source.
`--config` Quickwit config file.

*Examples*

*Delete a `wikipedia-source` source*
```bash
quickwit source delete --index wikipedia --source wikipedia-source --config ./config/quickwit.yaml
```

### source describe

Describes a source.
`quickwit source describe [args]`

*Synopsis*

```bash
quickwit source describe
    --index <index>
    --source <source>
    --config <config>
```

*Options*

`--index` ID of the target index.
`--source` ID of the target source.
`--config` Quickwit config file.

*Examples*

*Describe a `wikipedia-source` source*
```bash
quickwit source describe --index wikipedia --source wikipedia-source --config ./config/quickwit.yaml
```

### source list

Lists the sources of an index.
`quickwit source list [args]`

*Synopsis*

```bash
quickwit source list
    --index <index>
    --config <config>
```

*Options*

`--index` ID of the target index.
`--config` Quickwit config file.

*Examples*

*List `wikipedia` index sources*
```bash
quickwit source list --index wikipedia --config ./config/quickwit.yaml
```



{/* End of auto generated CLI docs. */}


## Environment Variables

### QW_CONFIG

Specifies the path to the [quickwit config](quickwit-config.md). Every command requires the `config`, which you can set once for all with `QW_CONFIG` environment variable.

*Example*

`export QW_CONFIG=./config/quickwit.yaml`


### QW_DISABLE_TELEMETRY

Disables [telemetry](telemetry.md) when set to any non-empty value.
