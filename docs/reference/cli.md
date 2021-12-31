---
title: CLI Reference
sidebar_position: 1
---

Quickwit is a single binary that makes it easy to index and search structured or unstructured data from the command line. It consumes datasets consisting of newline-delimited JSON objects with arbitrary keys. It produces indexes that can be stored locally or remotely on an object storage such as Amazon S3 and queried with subsecond latency.

This page documents all the available commands, related options, and environment variables.

:::caution

Before using Quickwit with an object storage, check out our [advice](../administration/cloud-env.md) for deploying on AWS S3 to avoid some nasty surprises at the end of the month.

:::


## Commands

[Command-line synopsis syntax](https://developers.google.com/style/code-syntax)

### Help

`quickwit` or `quickwit --help` displays the list of available commands.

`quickwit <command name> --help` displays the documentation for the command and a usage example.

## Environment Variables

### QW_CONFIG

Specifies the path to the [quickwit config](quickwit-config.md). The `config` is required by every command, via the environment variable it can be set once for all.

*Example*

`export QW_CONFIG=./config/quickwit.yaml`

### QW_ENV

Specifies the nature of the current working environment. Currently, this environment variable is used exclusively for testing purposes, and `LOCAL` is the only supported value.

### QW_DISABLE_TELEMETRY

Disables [telemetry](telemetry.md) when set to any non-empty value.


#### Note on telemetry
Quickwit collects some [anonymous usage data](telemetry.md), you can disable it. When it's enabled you will see this
output:
```
quickwit 
Quickwit 0.1.0 (commit-hash: 98de4ce)

Index your dataset on object storage & making it searchable from the command line.
  Find more information at https://quickwit.io/docs

Telemetry: enabled

[...]
```

The line `Telemetry enabled` disappears when you disable it.


### Version

`quickwit --version` displays the version. It is useful for reporting bugs.


### Syntax

The cli is structured into highlevel commands with subcommands.
`quickwit [command] [subcommand] [args]`.

* `command`:index, split, source, service. 









## index
Index operation commands such as `create`, `index`, or `search`...

### index create

Creates an index at `index-uri`. 
`index-uri` can either be passed as parameter, or it will take the default_root_index_uri from quickwit `config` in the format `{default_index_root_uri}/{index-id}` (index-id is the index parameter). 
The command fails if an index already exists unless `overwrite` is passed. 
When `overwrite` is enabled, the command deletes all the files stored at `index-uri` before creating a new index. 
The index config defines how a document and fields it contains, are stored and indexed, see the [index config documentation](index-config.md).
  
`quickwit index create [args]`

*Synopsis*

```bash
quickwit index create
    --index <index>
    --index-config <index-config>
    [--index-uri <index-uri>]
    --config <config>
    [--overwrite]
```

*Options*

`--index` ID of the target index.    
`--index-config` Location of the index config file.    
`--index-uri` Index data (or splits) storage URI.    
`--config` Quickwit config file.    
`--overwrite` Overwrites pre-existing index.    

*Examples*

*Create a new index with metastore in the local `metastore` folder.*
```bash
quickwit index create --index-config-uri ./wikipedia_index_config.yaml  --config=quickwit.yaml
```

### index ingest

Indexes a dataset consisting of newline-delimited JSON objects located at `input-path` or read from *stdin*. 
The data is appended to the target index specified by `index` unless `overwrite` is passed. `input-path` can be a file or another command output piped into stdin. 
Currently, only local datasets are supported. 
By default, quickwit's indexer will work with a heap of 2 GiB of memory, but this can be set with the `heap-size` option in the indexing_settings.resources section in the config. 
This does not directly reflect the overall memory usage of `quickwit index ingest`, but doubling this value should give a fair approximation.
  
`quickwit index ingest [args]`

*Synopsis*

```bash
quickwit index ingest
    --index <index>
    --config <config>
    [--data-dir <data-dir>]
    [--input-path <input-path>]
    [--overwrite]
```

*Options*

`--index` ID of the target index.    
`--config` Quickwit config file.    
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.    
`--input-path` Location of the input file.    
`--overwrite` Overwrites pre-existing index.    

*Examples*

*Indexing a dataset from a file*
```bash
quickwit index ingest --index wikipedia --config=quickwit.yaml --data-dir wikipedia --input-path wikipedia.json
```

*Indexing a dataset from stdin*
```bash
cat hdfs-log.json | quickwit index ingest --index wikipedia --config=quickwit.yaml --data-dir wikipedia
```

### index describe

Display descriptive statistics about the index.  
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
### index search

Searches the index with the index id `index` and returns the documents matching the query specified with `query`. 
The offset of the first hit returned and the number of hits returned can be set with the `start-offset` and `max-hits` options. 
It's possible to restrict the search on specified fields using the `search-fields` option. 
Search can also be limited to a time range using the `start-timestamp` and `end-timestamp` options. 
These timestamp options can particularly be useful in boosting query performance when using a time series dataset and only need to query a particular window.
  
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

`--index` id of the target index.    
`--config` Quickwit config file.    
`--query` Query expressed in Tantivy syntax.    
`--max-hits` Maximum number of hits returned. (Default: 20)    
`--start-offset` Offset in the global result set of the first hit returned. (Default: 0)    
`--search-fields` Searches only in those fields.    
`--start-timestamp` Filters out documents before that timestamp (time-series indexes only).    
`--end-timestamp` Filters out documents after that timestamp (time-series indexes only).    

*Examples*

*Searching a index*
```bash
quickwit search --index wikipedia --config ./quickwit.yaml --query "Barack Obama"
```

*Limiting the result set to 50 hits*
```bash
quickwit search --index wikipedia --config ./quickwit.yaml --query "Barack Obama" --max-hits 50
```

*Looking for matches in the title and url fields only*
```bash
 quickwit search --index wikipedia --config ./quickwit.yaml --query "Barack Obama" --search-fields title,url
```

### index merge

Merges an index.  
`quickwit index merge [args]`

*Synopsis*

```bash
quickwit index merge
    --index <index>
    --config <config>
    [--data-dir <data-dir>]
```

*Options*

`--index` ID of the target index.    
`--config` Quickwit config file.    
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.    
### index gc

Garbage collects stale staged splits and splits marked for deletion.  
:::note
Intermediate files are created while executing Quickwit commands. 
These intermediate files are always cleaned at the end of each successfully executed command. 
However, failed or interrupted commands can leave behind intermediate files that need to be removed. 
Also note that using very short grace-period (like seconds) can cause removal of intermediate files being operated on especially when using Quickwit concurently on the same index. 
In practice you can settle with the default value (1 hour) and only specify a lower value if you really know what you are doing.

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
`--dry-run` Executes the command in dry run mode and only displays the list of splits candidate for garbage collection.    
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
### split extract

Downloads and extracts a split to a directory.  
`quickwit split extract [args]`

*Synopsis*

```bash
quickwit split extract
    --index <index>
    --split <split>
    --config <config>
    [--data-dir <data-dir>]
    --target-dir <target-dir>
```

*Options*

`--index` ID of the target index.    
`--split` ID of the target split.    
`--config` Quickwit config file.    
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.    
`--target-dir` Directory to extract the split to.    
### split describe

Displays metadata about the split.  
`quickwit split describe [args]`

*Synopsis*

```bash
quickwit split describe
    --index <index>
    --split <split>
    --config <config>
    [--data-dir <data-dir>]
    [--verbose]
```

*Options*

`--index` ID of the target index.    
`--split` ID of the target split.    
`--config` Quickwit config file.    
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.    
`--verbose` Displays additional metadata about the hotcache.    
## service
Launches services.

### service run

Starts a service. Currently, the only services available are `indexer` and `searcher`.  
`quickwit service run [args]`
### service run searcher

Starts a web server that exposes the [Quickwit REST API](search-api.md). 
The node can optionally join a cluster using the `peer_seed` parameter (quickwit.yaml). 
This list of node addresses is used to discover the remaining peer nodes in the cluster through the use of a gossip protocol (SWIM).
  
:::note
Behind the scenes, Quickwit need to open the following port for cluster formation and workload distribution:

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

*curl request and specify fields to search*
```bash
 curl -L "http://127.0.0.1:7280/api/v1/wikipedia/search/stream?query=clinton&searchField=body,title" 
```

*curl request stream fast field, HTTP1.1 chunked transfer encoding*
```bash
 curl -L "http://127.0.0.1:7280/api/v1/wikipedia/search/stream?query=tangkhul&searchField=body,title&fastField=number&outputFormat=csv"  
```

*curl request stream fast field as binary, force HTTP2*
```bash
 curl -L --http2-prior-knowledge "http://127.0.0.1:7280/api/v1/myindex/search/stream?query=clinton&searchField=body,title&fastField=number&outputFormat=clickHouseRowBinary"  
```

### service run indexer

Starts a indexing server that consumes the sources from the `quickwit source add` command. The server can be terminated when all sources are consumed.
  
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

*Add and consume source*
```bash
 
quickwit source add --id wikipedia_data_1 --index wikipedia --params '{"filepath":"wiki-articles_1.json"}' --type file
quickwit service run indexer --indexes wikipedia

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
    --id <id>
    --type <type>
    --params <params>
    --config <config>
```

*Options*

`--index` ID of the target index.    
`--id` ID of the source.    
`--type` Type of the source. Available types are: `file` and `kafka`.    
`--params` Parameters for the source formatted as a JSON object passed inline or via a file. Parameters are source-specific. Please, refer to the source's documentation for more details.    
`--config` Quickwit config file.    
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
