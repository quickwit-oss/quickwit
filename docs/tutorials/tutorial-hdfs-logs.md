---
title: Index a logging dataset
sidebar_position: 2
---


In this guide, we will index some 40 million log entries (13 GB decompressed) on a local machine. If you want
to start a server with indexes on AWS S3, check out the [tutorial for distributed search](tutorial-hdfs-logs-distributed-search-aws-s3.md).


Here is an example of a log entry:
```json
{
  "timestamp": 1460530013,
  "severity_text": "INFO",
  "body": "PacketResponder: BP-108841162-10.10.34.11-1440074360971:blk_1074072698_331874, type=HAS_DOWNSTREAM_IN_PIPELINE terminating",
  "resource": {
    "service": "datanode/01"
  },
  "attributes": {
    "class": "org.apache.hadoop.hdfs.server.datanode.DataNode"
  }
}
```


## Install

Let's download and install Quickwit.

```bash
curl -L https://install.quickwit.io | sh
```


## Create your index

Let's create an index configured to receive these logs.

```bash
# First, download the hdfs logs config from Quickwit repository.
curl -o hdfslogs_index_config.json https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/index_configs/hdfslogs_index_config.json
```

The index config defines 5 fields: `timestamp`, `severity_text`, `body`, and two object fields
for the nested values `resource` and `attributes`. 
It also sets the [default search field](../reference/index-config.md) and a timestamp field. 
This timestamp field will be used by Quickwit for sorting documents (descending order) and for [splits pruning](../overview/architecture.md) at query time to boost search speed. Check out the [index config docs](../reference/index-config.md) for details.


```json title="hdfslogs_index_config.json"
{
    "store_source": false, // The document source (=json) will not be stored.
    "default_search_fields": ["body", "severity_text"],
    "timestamp_field": "timestamp",
    "field_mappings": [
        {
            "name": "timestamp",
            "type": "i64",
            "fast": true // Fast field must be present when this is the timestamp field.
        },
        {
            "name": "severity_text",
            "type": "text",
            "tokenizer": "raw" // No tokeninization.
        },
        {
            "name": "body",
            "type": "text",
            "tokenizer": "default",
            "record": "position" // Record position will enable phrase query on body field.
        },
        {
            "name": "resource",
            "type": "object",
            "field_mappings": [
                {
                    "name": "service",
                    "type": "text",
                    "tokenizer": "raw"
                }
            ]
        },
        {
            "name": "attributes",
            "type": "object",
            "field_mappings": [
                {
                    "name": "class",
                    "type": "text",
                    "tokenizer": "raw"
                }
            ]
        }
    ]
}
```

Now we can create the index with the new command (assuming you create it in your current directory):

```bash
quickwit new --index-uri file://$(pwd)/hdfs_logs --index-config-path ./hdfslogs_index_config.json
```

You're now ready to fill the index.

## Index logs
The dataset is a compressed [ndjson file](https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz). Instead of downloading it and then indexing the data, we will use pipes to directly send a decompressed stream to Quickwit.
This can take up to 10 min on a modern machine, the perfect time for a coffee break.

```bash
curl https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz | gunzip | quickwit index --index-uri file://$(pwd)/hdfs_logs
```

You can check it's working by using `search` command and look for `ERROR` in `serverity_text` field:
```bash
quickwit search --index-uri file:///$(pwd)/hdfs_logs --query "severity_text:ERROR"
```


:::note

The `index` command generates [splits](../overview/architecture.md) of 5 millions documents. Each split is a small piece of index represented by a directory in which index files are saved. For this dataset, you will end with 9 splits, the last
one will be very small.

:::


## Start your server

The command `serve` starts an http server which provides a [REST API](../reference/search-api.md).

```bash
quickwit serve --index-uri file:///$(pwd)/hdfs_logs
```

Let's execute the same query on field `severity_text` but with `cURL`:

```bash
curl -v "http://0.0.0.0:8080/api/v1/hdfs_logs/search?query=severity_text:ERROR"
```

which returns the json

```json
{
  "numHits": 364,
  "hits": [
    {
      "attributes.class": [
        "org.apache.hadoop.hdfs.server.datanode.DataNode"
      ],
      "body": [
        "RECEIVED SIGNAL 15: SIGTERM"
      ],
      "resource.service": [
        "datanode/02"
      ],
      "severity_text": [
        "ERROR"
      ],
      "timestamp": [
        1442629246
      ]
    }
    ...
  ],
  "numMicrosecs": 505923
}
```

The index config shows that we can use the timestamp field parameters `startTimestamp` and `endTimestamp` and benefit from time pruning. Behind the scenes, Quickwit will only query [splits](../overview/architecture.md) that have logs in this time range.

Let's use these parameters with the following query:

```bash
curl -v 'http://0.0.0.0:8080/api/v1/hdfs_logs/search?query=severity_text:ERROR&startTimestamp=1442834249&endTimestamp=1442900000'
```

It should return 6 hits faster as Quickwit will query fewer splits.

## Clean

Let's do some cleanup by deleting the index:

```bash
quickwit delete --index-uri file:///$(pwd)/hdfs_logs
```


Congratz! You finished this tutorial! 


To continue your Quickwit journey, check out the [tutorial for distributed search](tutorial-hdfs-logs-distributed-search-aws-s3.md) or dig into the [search REST API](../reference/search-api.md) or [query language](../reference/query-language.md).

