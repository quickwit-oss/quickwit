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



## run
Runs quickwit services. By default, `indexer` and `searcher` are started.

## index
Create your index, ingest data, search, describe... every command you need to manage indexes.

### index list

List indexes.  
`quickwit index list [args]`

*Synopsis*

```bash
quickwit index list
    --config <config>
    [--metastore-uri <metastore-uri>]
```

*Options*

`--config`     
`--metastore-uri`     

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

Creates an index of ID `index` at `index-uri` configured by a [YAML config file](index-config.md) located at `index-config`.
The index config lets you define the mapping of your document on the index and how each field is stored and indexed.
If `index-uri` is omitted, `index-uri` will be set to `{default_index_root_uri}/{index}`, more info on [Quickwit config docs](config.md).
The command fails if an index already exists unless `overwrite` is passed.
When `overwrite` is enabled, the command deletes all the files stored at `index-uri` before creating a new index.
  
`quickwit index create [args]`

*Synopsis*

```bash
quickwit index create
    --config <config>
    --index-config <index-config>
    [--data-dir <data-dir>]
    [--overwrite]
```

*Options*

`--config`     
`--index-config`     
`--data-dir`     
`--overwrite`     

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
    --config <config>
    --index <index>
    [--data-dir <data-dir>]
    [--input-path <input-path>]
    [--overwrite]
    [--keep-cache]
```

*Options*

`--config`     
`--index`     
`--data-dir`     
`--input-path`     
`--overwrite`     
`--keep-cache`     

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
    --config <config>
    --index <index>
    [--data-dir <data-dir>]
```

*Options*

`--config`     
`--index`     
`--data-dir`     

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
    --config <config>
    --index <index>
    [--data-dir <data-dir>]
    --query <query>
    [--aggregation <aggregation>]
    [--max-hits <max-hits>]
    [--start-offset <start-offset>]
    [--search-fields <search-fields>]
    [--start-timestamp <start-timestamp>]
    [--end-timestamp <end-timestamp>]
```

*Options*

`--config`     
`--index`     
`--data-dir`     
`--query`     
`--aggregation`     
`--max-hits`  (Default: 20)    
`--start-offset`  (Default: 0)    
`--search-fields`     
`--start-timestamp`     
`--end-timestamp`     

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
    --config <config>
    --index <index>
    [--data-dir <data-dir>]
    [--grace-period <grace-period>]
    [--dry-run]
```

*Options*

`--config`     
`--index`     
`--data-dir`     
`--grace-period`  (Default: 1h)    
`--dry-run`     
### index delete

Delete an index.  
`quickwit index delete [args]`

*Synopsis*

```bash
quickwit index delete
    --config <config>
    --index <index>
    [--data-dir <data-dir>]
    [--dry-run]
```

*Options*

`--config`     
`--index`     
`--data-dir`     
`--dry-run`     

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
    --config <config>
    --index <index>
    --source-config <source-config>
```

*Options*

`--config`     
`--index`     
`--source-config`     
### source delete

Deletes a source from an index.  
`quickwit source delete [args]`

*Synopsis*

```bash
quickwit source delete
    --config <config>
    --index <index>
    --source <source>
```

*Options*

`--config`     
`--index`     
`--source`     

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
    --config <config>
    --index <index>
    --source <source>
```

*Options*

`--config`     
`--index`     
`--source`     

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
    --config <config>
    --index <index>
```

*Options*

`--config`     
`--index`     

*Examples*

*List `wikipedia` index sources*
```bash
quickwit source list --index wikipedia --config ./config/quickwit.yaml

```

## split
Operations (list, add, delete, describe...) on splits.

### split list

List the splits of an index.  
`quickwit split list [args]`

*Synopsis*

```bash
quickwit split list
    --config <config>
    --index <index>
    [--data-dir <data-dir>]
    [--tags <tags>]
    [--states <states>]
    [--start-date <start-date>]
    [--end-date <end-date>]
```

*Options*

`--config`     
`--index`     
`--data-dir`     
`--tags`     
`--states`     
`--start-date`     
`--end-date`     



{/* End of auto generated CLI docs. */}


## Environment Variables

### QW_CONFIG

Specifies the path to the [quickwit config](quickwit-config.md). Every command requires the `config`, which you can set once for all with `QW_CONFIG` environment variable.

*Example*

`export QW_CONFIG=./config/quickwit.yaml`


### QW_DISABLE_TELEMETRY

Disables [telemetry](telemetry.md) when set to any non-empty value.
