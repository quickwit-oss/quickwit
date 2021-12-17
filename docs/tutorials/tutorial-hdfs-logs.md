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
curl -o hdfslogs_index_config.yaml https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/index_configs/hdfslogs_index_config.yaml
```

The index config defines 5 fields: `timestamp`, `severity_text`, `body`, and two object fields
for the nested values `resource` and `attributes`. 
It also sets the [default search field](../reference/index-config.md) and a timestamp field. 
This timestamp field will be used by Quickwit for sorting documents (descending order) and for [splits pruning](../overview/architecture.md) at query time to boost search speed. Check out the [index config docs](../reference/index-config.md) for details.


```yaml title="hdfslogs_index_config.yaml"
version: 0
index_id: hdfs_logs
index_uri: file://$(pwd)/indexes/hdfs_logs # Adjust to your own s3 bucket

doc_mapping:
  field_mappings:
  - name: timestamp
    type: i64
    fast: true # Fast field must be present when this is the timestamp field.
  - name: severity_text
    type: text
    tokenizer: raw # No tokeninization.
  - name: body
    type: text
    tokenizer: default
    record: position # Record position will enable phrase query on body field.
  - name: resource
    type: object
    field_mappings:
    - name: service 
      type: text
      tokenizer: raw # Text field referenced as tag must have the `raw` tokenier.
  - name: attributes
    type: object
    field_mappings:
    - name: class
      type: text
      tokenizer: raw
  tag_fields: []

indexing_settings:
  store_source: false # The document source (=json) will not be stored.
  timestamp_field: timestamp
  resources:
    heap_size: 50MB
    
search_settings:
  default_search_fields:
  - body
  - severity_text
```

Now we can create the index with the `create` subcommand (assuming you create it in your current directory):

```bash
quickwit index create --metastore-uri file://$(pwd)/indexes --index-config-uri ./hdfslogs_index_config.json
```

You're now ready to fill the index.

## Index logs
The dataset is a compressed [ndjson file](https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz). Instead of downloading it and then indexing the data, we will use pipes to directly send a decompressed stream to Quickwit.
This can take up to 10 min on a modern machine, the perfect time for a coffee break.

```bash
curl https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz | gunzip | quickwit index ingest --index-id hdfs_logs --metastore-uri file://$(pwd)/indexes
```

You can check it's working by using `search` subcommand and look for `ERROR` in `serverity_text` field:
```bash
quickwit index search --index-id hdfs_logs --metastore-uri file://$(pwd)/indexes  --query "severity_text:ERROR"
```


:::note

The `ingest` subcommand generates [splits](../overview/architecture.md) of 5 millions documents. Each split is a small piece of index represented by a file in which index files and metadata files are saved. For this dataset, you will end with 9 splits, the last one being be very small.

:::


## Start your server

The command `service run searcher` starts an http server which provides a [REST API](../reference/search-api.md).

We first need to create a server configuration file. 

```bash
echo "version: 0
metastore_uri: file://$(pwd)/indexes
searcher:
  data_dir_path: file://$(pwd)/datadir
  host_key_path: file://$(pwd)/datadir" > server_config.yaml
```

```bash
./quickwit service run searcher --server-config-uri ./server_config.yaml
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
quickwit index delete --index-id hdfs_logs --metastore-uri file:///$(pwd)/indexes
```


Congratz! You finished this tutorial! 


To continue your Quickwit journey, check out the [tutorial for distributed search](tutorial-hdfs-logs-distributed-search-aws-s3.md) or dig into the [search REST API](../reference/search-api.md) or [query language](../reference/query-language.md).

