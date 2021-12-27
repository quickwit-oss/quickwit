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

`quickwit <command name>` displays the documentation for the command and a usage example.

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

* `command`:index, split, service, source. 



# index

*Description*
Index operation commands such as `create`, `index`, or `search`...

### index create

`quickwit index create [args]`

*Synopsis*

```bash
quickwit index create
    --index <index>
    --index-config <index-config>
    [--index-uri <index-uri>]
    --config <config>
    [--data-dir <data-dir>]
    [--overwrite]
```

*Options*

`--index` ID of the target index.
`--index-config` Location of the index config file.
`--index-uri` Index data (or splits) storage URI.
`--config` Quickwit config file.
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.
`--overwrite` Overwrites pre-existing index.
### index ingest

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
### index describe

`quickwit index describe [args]`

*Synopsis*

```bash
quickwit index describe
    --index <index>
    --config <config>
    [--data-dir <data-dir>]
```

*Options*

`--index` ID of the target index.
`--config` Quickwit config file.
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.
### index search

`quickwit index search [args]`

*Synopsis*

```bash
quickwit index search
    --index <index>
    --config <config>
    [--data-dir <data-dir>]
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
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.
`--query` Query expressed in Tantivy syntax.
`--max-hits` Maximum number of hits returned.
`--start-offset` Offset in the global result set of the first hit returned.
`--search-fields` Searches only in those fields.
`--start-timestamp` Filters out documents before that timestamp (time-series indexes only).
`--end-timestamp` Filters out documents after that timestamp (time-series indexes only).
### index merge

`quickwit index merge [args]`

*Synopsis*

```bash
quickwit index merge
    --index <index>
    --config <config>
    [--data-dir <data-dir>]
```

*Options*

`--index` INDEX.
`--config` Quickwit config file.
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.
### index gc

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
`--grace-period` Threshold period after which stale staged splits are garbage collected.
`--dry-run` Executes the command in dry run mode and only displays the list of splits candidate for garbage collection.
### index delete

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
# split

*Description*
Operations (list, add, delete, describe...) on splits.

### split list

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
# service

*Description*
Launches services.

### service run

`quickwit service run [args]`

*Synopsis*

```bash
quickwit service run
```

*Options*

# source

*Description*
Manages sources.

### source describe

`quickwit source describe [args]`

*Synopsis*

```bash
quickwit source describe
    --index <index>
    --source <source>
    --config <config>
    [--data-dir <data-dir>]
```

*Options*

`--index` ID of the target index.
`--source` ID of the target source.
`--config` Quickwit config file.
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.
### source list

`quickwit source list [args]`

*Synopsis*

```bash
quickwit source list
    --index <index>
    --config <config>
    [--data-dir <data-dir>]
```

*Options*

`--index` ID of the target index.
`--config` Quickwit config file.
`--data-dir` Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.





## Environment Variables

### QW_ENV

Specifies the nature of the current working environment. Currently, this environment variable is used exclusively for testing purposes, and `LOCAL` is the only supported value.

### QW_DISABLE_TELEMETRY

Disables [telemetry](telemetry.md) when set to any non-empty value.
