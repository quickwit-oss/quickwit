---
title: CLI Reference
sidebar_position: 1
---

Quickwit is a single binary that makes it easy to index and search structured or unstructured data from the command line. It consumes datasets consisting of newline-delimited JSON objects with arbitrary keys and produces indexes that can be stored locally or remotely on an object storage such as Amazon S3 and queried with subsecond latency.

This page documents all the available commands, related options and environment variables.


## Commands

[Command-line synopsis syntax](https://developers.google.com/style/code-syntax)

### Help

`quickwit help` displays the list of available commands.

`quickwit help <command name>` displays the documentation for the command and a usage example.

### Version

`quickwit version` displays the version. Useful for reporting bugs.

### New

*Description*

Creates an index at `index-uri` with a doc mapper configured by a json file located at `doc-mapper-config-path`. The command fails if an index already exists at `index-uri` unless `overwrite` is passed. When `overwrite` is enabled, the command deletes all the files stored at `index-uri` before creating a new index. The doc mapper defines how a document and fields it contains, are stored and indexed, see the [doc mapper documentation](reference/doc-mapper.md).

*Synopsis*

```bash
quickwit new
    --index-uri <uri>
    --doc-mapper-config-path <path>
    [--overwrite]
```

*Options*

`--index-uri` (string) Defines the index location.<br />
`--doc-mapper-config-path` (string) Defines the doc mapper config path.<br />
`--overwrite` (boolean) Overwrites existing index.

*Examples*

*Creating a new index on local file system*<br />
`quickwit new --index-uri file:///quickwit-indexes/catalog --doc-mapper-config-path ~/quickwit-mapper/doc_mapper.json`

*Creating a new index on Amazon S3*<br />
`quickwit new --index-uri s3://quickwit-indexes/catalog --doc-mapper-config-path ~/quickwit-mapper/doc_mapper.json`

*Replacing an existing index*<br />
`quickwit new --index-uri s3://quickwit-indexes/catalog --doc-mapper-config-path ~/quickwit-mapper/doc_mapper.json --overwrite`

### Index

*Description*

Indexes a dataset consisting of newline-delimited JSON objects located at `input-path` or read from *stdin*. The data is appended to the target index specified by `index-uri` unless `overwrite` is passed. `input-path` can be a file or another command output piped into stdin. Currently, only local datasets are supported. By default, the process uses 4 threads and 1 GiB of memory per thread. The `num-threads` and `heap-size` options customize those settings.

*Synopsis*

```bash
quickwit index
    --index-uri <uri>
    [--input-path <path>]
    [--overwrite]
    [--num-thread <num threads>]
    [--heap-size <num bytes>]
    [--temp-dir]
```

*Options*

`--index-uri` (string) Location of the target index.<br />
`--input-path` (string) Location of the source dataset.<br />
`--overwrite` (boolean) Overwrites existing data.<br />
`--num-thread` (integer) Number of allocated threads for the process.<br />
`--heap-size` (integer) Amount of allocated memory for the process.<br />
`--temp-dir` (string) Path of temporary directory for building the index (defaults to `/tmp`)

*Examples*

*Indexing a local dataset*<br />
`quickwit index --index-uri s3://quickwit-indexes/nginx --input-path nginx.json`

*Indexing a dataset from stdin*<br />
`cat nginx.json | quickwit index --index-uri s3://quickwit-indexes/nginx`<br />
`quickwit index --index-uri s3://quickwit-indexes/nginx < nginx.json`

*Reindexing a dataset*<br />
`quickwit index --index-uri s3://quickwit-indexes/nginx --input-path nginx.json --overwrite`

*Customizing the resources allocated to the program*<br />
`quickwit index --index-uri s3://quickwit-indexes/nginx --input-path nginx.json --num-threads 8 --heap-size 16GiB`

### Search

*Description*

Searches the index stored at `index-uri` and returns the documents matching the query specified with `query`. The offset of the first hit returned and the number of hits returned can be set with the `start-offset` and `max-hits` options.

TODO: complete this description when API is stabilized (target fields, start/end datetime, datetime format)

*Synopsis*

```bash
quickwit search
    --index-uri <uri>
    --query <query>
    [--max-hits <n>]
    [--start-offset <offset>]
    [--search-fields <comma-separated list of fields>]
    [--start-timestamp <i64>]
    [--end-timestamp <i64>]
```

*Options*

`--index-uri` (string) Location of the target index.<br />
`--query` (string) Query expressed in Tantivy syntax.<br />
`--max-hits` (integer) Maximum number of hits returned (defaults to `20`).<br />
`--start-offset` (integer) Skips the first `start-offset` hits (defaults to `0`).<br />
`--search-fields` (string) Search only on this comma-separated list of field names.<br />
`--start-timestamp` (string) Inclusive lower bound.<br />
`--end-timestamp` (string) Exclusive upper bound.<br />

*Examples*

*Searching a local index*<br />
`quickwit search --index-uri file:///path-to-my-indexes/wikipedia --query "Barack Obama"`

*Searching a remote index*<br />
`quickwit search --index-uri s3://quickwit-indexes/wikipedia --query "Barack Obama"`

*Limiting the result set to 50 hits*<br />
`quickwit search --index-uri s3://quickwit-indexes/wikipedia --query "Barack Obama" --max-hits 50`

*Skipping the first 20 hits*<br />
`quickwit search --index-uri s3://quickwit-indexes/wikipedia --query "Barack Obama" --start-offset 20`

*Looking for matches in the title and url fields only*<br />
`quickwit search --index-uri s3://quickwit-indexes/wikipedia --query "Barack Obama" --search-fields title,url`

### Serve

*Description*

Starts a rest server at address `host`:`port` and makes searchable indexes located at `index-uri` and returns the documents matching the query specified with `query`. Optionally connects to peers listed at `peer-seeds` using SWIM membership protocol to allow search workload distribution.

*Synopsis*

```bash
quickwit serve
    --index-uri <list of uris>
    --host <hostname>
    --port <port>
    --peer-seeds <list of seeds>
```

*Options*

`--index-uri` (string) List of location of target indexes.<br />
`--host` (string) Hostname the rest server should bind to.<br />
`--port` (string) Port the REST API server should bind to.<br />
`--peer-seeds` (string) List of peer socket address (e.g. 192.1.1.3:12001) to connect to form a cluster..<br />

*Examples*

*Start a local server and for a local index*<br />
`quickwit serve --index-uri file:///path-to-my-indexes/wikipedia`


### Delete

*Description*

Deletes the index at `index-uri`.

*Synopsis*

```bash
quickwit delete
    --index-uri <uri>
    [--dry-run]
```

*Options*

`--index-uri` (string) Location of the target index.
`--dry-run` (boolean) Executes the command in dry run mode and displays the list of files subject to be deleted.

*Examples*

*Deleting an index*<br /> `quickwit delete --index-uri s3://quickwit-indexes/catalog`

*Executing in dry run mode*<br />
`quickwit delete --index-uri s3://quickwit-indexes/catalog --dry-run`
