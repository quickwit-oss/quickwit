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

Specifies the path to the (quickwit config)[quickwit-config.md]. The `config` is required by every command, via a environment variable it can be set once for all.

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
`






