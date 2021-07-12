---
title: Index a logging dataset
sidebar_position: 2
---


In this guide, we will index some logs (40 million log entries, 13 GB decompressed) on a local machine. If you want
to start a server with indexes on AWS S3, checkout the [tutorial for distributed search](tutorial-hdfs-logs-distributed-search-aws-s3.md).

Example of a log entry:
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

```bash
curl -L https://install.quickwit.io | sh
```


## Create your index

```bash
# First download the index config from quickwit repository
curl -o hdfslogs_index_config.json https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/index_configs/hdfslogs_index_config.json
quickwit new --index-uri file://$(pwd)/hdfs_logs --index-config-path ./hdfslogs_index_config.json
```

## Index logs
To index the dataset, we will download [data](https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz) with curl, decompress it and pipe it to Quickwit (can take 5 minutes on an apple M1).

```bash
curl https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz | gunzip | quickwit index --index-uri file://$(pwd)/hdfs_logs
```

## Start your server

```bash
quickwit serve --index-uri s3://path-to-your-bucket/hdfs_logs
```

## Time to search

Let's execute a simple query that returns only `ERROR` entries on field `severity_text`:

```bash
curl -v 'http://your-load-balancer/api/v1/hdfs_logs/search?query=severity_text:ERROR
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

You can see that this query has only 364 hits and that the server responds in 0.5 second.

We can also add `startTimestamp` and `endTimestamp` to have a more accurate query and benefit from time pruning on timestamp field. Time pruning means that Quickwit will only query splits concerned by this time range. 
This can have an important impact on speed.


```bash
curl -v 'http://your-load-balancer/api/v1/hdfs_logs/search?query=severity_text:ERROR&startTimestamp=1442834249&endTimestamp=1442900000'
```

returns 6 hits in 0.36 seconds.


Congratz! You finished this tutorial! To continue your Quickwit journey, checkout the [search REST API reference](../reference/search-api.md) or the [query language reference](../reference/query-language.md).

