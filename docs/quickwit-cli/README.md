# Quickwit CLI

Quickwit CLI is a tool that makes it easy to index and search structured or unstructured data from the command line. It consumes datasets consisting of newline-delimited JSON objects with arbitrary keys and produces indexes that can be stored locally or remotely on an object storage such as Amazon S3 and queried with subsecond latency.

Key features include the following:
- schemaless
- local and remote indexes
- native support for time-series datasets

Current limitations
- no support for file formats other than JSON
- no support for object storages not compatible with Amazon S3
- no faceted search
- no deletions (append mode only)

Influence our roadmap by voting in our [GitHub issues](https://github.com/quickwit-inc/quickwit/issues) for the features that you need the most!

### Warning

Cloud providers charge for data transfers in and out of their networks. In addition, querying an index from a remote machine adds some extra latency. For those reasons, we recommend that you test and use the Quickwit CLI from an instance located within your cloud provider's network.

## Getting started
Coming soon!

## Commands

[Command-line synopsis syntax](https://developers.google.com/style/code-syntax)

### Help

`quickwit help` displays the list of available commands.

`quickwit help <command name>` displays the documentation for the command and a usage example.

### Version

`quickwit version` displays the version of the CLI. Useful for reporting bugs.

### New

*Description*

Creates an index at `index-path`. The command fails if an index already exists at `index-path` unless `overwrite` is passed. When `overwrite` is enabled, the command deletes all the files stored at `index-path` before creating a new index. The command features two mutually exclusive options for creating a time-series index or a non-time-series index. Specifying the name of the field that holds a [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time) (in seconds, milliseconds, microseconds, or nanoseconds) in each record of the dataset with the `--timestamp-field` option creates a time-series index whereas passing the `--no-timestamp-field` option creates a non-time-series-index.

*Synopsis*

```bash
quickwit new
    --index-path <path>
    {--timestamp-field <field name> | --no-timestamp-field}
    [--overwrite]
```

*Options*

`--index-path`  (string) Defines the index location.
`--timestamp-field`  (string) Creates a time-series index with the specified timestamp field.
`--no-timestamp-field` (boolean) Creates a non-time-series index.
`--overwrite` (boolean) Overwrites existing index.

The `timestamp-field` and `no-timestamp-field` options are mutually exclusive.

*Examples*

*Creating a new index on local file system*
`quickwit new --index-path ~/quickwit-indexes/catalog --no-timestamp-field`

*Creating a new index on Amazon S3*
`quickwit new --index-path s3://quickwit-indexes/catalog --no-timestamp-field`

*Replacing an existing index*
`quickwit new --index-path s3://quickwit-indexes/catalog --no-timestamp-field --overwrite`

*Creating a new time-series index*
`quickwit new --index-path s3://quickwit-indexes/nginx --timestamp-field ts`

### Index

*Description*

Indexes a dataset consisting of newline-delimited JSON objects located at `input-path` or read from *stdin*. The data is appended to the target index specified by `index-path` unless `overwrite` is passed. `input-path` can be a file or a directory. In the latter case, the directory is traversed recursively. Local and remote datasets are supported. By default, the process uses 4 threads and 1 GiB of memory per thread. The `num-threads` and `heap-size` options customize those settings.

*Synopsis*

```bash
quickwit index
    --index-path <path>
    [--input-path <path>]
    [--overwrite]
    [--num-thread <num threads>]
    [--heap-size <num bytes>]
    [--temp-dir]
```

*Options*

`--index-path` (string) Location of the target index.
`--input-path` (string) Location of the source dataset.
`--overwrite` (boolean) Overwrites existing data.
`--num-thread` (integer) Number of allocated threads for the process.
`--heap-size` (integer) Amount of allocated memory for the process.
`--temp-dir` (string) Path of temporary directory for building the index (defaults to `/tmp`)

*Examples*

*Indexing a local dataset*
`quickwit index --index-path s3://quickwit-indexes/nginx --input-path nginx.json`

*Indexing a dataset from stdin*
`cat nginx.json | quickwit index --index-path s3://quickwit-indexes/nginx`

`quickwit index --index-path s3://quickwit-indexes/nginx < nginx.json`

*Reindexing a dataset*
`quickwit index --index-path s3://quickwit-indexes/nginx --input-path nginx.json --overwrite`

*Customizing the resources allocated to the program*
`quickwit index --index-path s3://quickwit-indexes/nginx --input-path nginx.json --num-threads 8 --heap-size 16GiB`

**Search**

*Description*

Searches the index stored at `index-path` and returns the documents matching the query `query`. The offset of the first hit returned and the number of hits returned can be set with the `start-offset` and `max-hits` options.

TODO: complete this description when API is stabilized (target fields, start/end datetime, datetime format)

*Synopsis*

```bash
quickwit search
    --index-path <path>
    --query <query>
    [--max-hits <n>]
    [--start-offset <offset>]
    [--start-datetime <datetime>]
    [--end-datetime <datetime>]
    [--datetime-format <format>]
    [--target-fields <comma-separated list of fields>]
```

*Options*

`--index-path` (string) Location of the target index.
`--query` (string) query TODO: QL syntax?.
`--max-hits` (integer) Maximum number of hits returned (defaults to `20`).
`--start-offset` (integer) Skips the first `start-offset` hits (defaults to `0`).
`--start-datetime` (string) Inclusive lower bound.
`--end-datetime` (string) Exclusive upper bound.
`--datetime-format` (string) ? (defaults to `YYYY-MM-DDThh:mm:ss`).
`--target-fields` (string) ?.

*Examples*

*Searching a local index*
`quickwit search --index-path ~/indexes/wikipedia --query "Barack Obama"`

*Searching a remote index*
`quickwit search --index-path s3://quickwit-indexes/wikipedia --query "Barack Obama"`

*Limiting the result set to 50 hits*
`quickwit search --index-path s3://quickwit-indexes/wikipedia --query "Barack Obama" --max-hits 50`

*Skipping the first 20 hits*
`quickwit search --index-path s3://quickwit-indexes/wikipedia --query "Barack Obama" --start-offset 20`

*Looking for matches in the title and url fields only*
`quickwit search --index-path s3://quickwit-indexes/wikipedia --query "Barack Obama" --target-fields title,url`

### Delete

*Description*

Deletes the index at`index-path`.

*Synopsis*

```bash
quickwit delete
    --index-path <path>
    [--dry-run]
```

*Options*

`--index-path` (string) Location of the target index.
`--dry-run` (boolean) Executes the command in dry run mode and displays the list of deleted files

*Examples*

*Deleting an index*
`quickwit delete --index-path s3://quickwit-indexes/catalog`

*Executing in dry run mode*

`quickwit delete --index-path s3://quickwit-indexes/catalog --dry-run`
