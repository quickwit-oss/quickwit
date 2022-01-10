---
title: Index a logging dataset
sidebar_position: 2
---


In this guide, we will index about 40 million log entries (13 GB decompressed) and start a three-node cluster on a local machine. If you want to start a server with indexes on AWS S3, check out the [tutorial for distributed search](tutorial-hdfs-logs-distributed-search-aws-s3.md).


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
cd quickwit-v*/
```

## Create your index

Let's create an index configured to receive these logs.

```bash
# First, download the hdfs logs config from Quickwit repository.
curl -o hdfslogs_index_config.yaml https://raw.githubusercontent.com/quickwit-inc/quickwit/main/config/tutorials/hdfs-logs/index-config.yaml
```

The index config defines four fields: `timestamp`, `severity_text`, `body`, and one object field
for the nested values `resource.service` . It also sets the `default_search_fields`, the `tag_fields`, and the `timestamp_field`.The `timestamp_field` and `tag_fields` are used by Quickwit for [splits pruning](../overview/architecture.md) at query time to boost search speed. Check out the [index config docs](../reference/index-config.md) for more details.

```yaml title="hdfslogs_index_config.yaml"
version: 0

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
  tag_fields: [resource.service]
  store_source: true

indexing_settings:
  timestamp_field: timestamp

search_settings:
  default_search_fields: [severity_text, body]
```

Now let's create the index with the `create` subcommand (assuming you are inside quickwit install directory):

Let's make use of the default configuration file provided in quickwit installation throughout this guide by setting the `QW_CONFIG` environment variable.

```bash
# Quickwit commands need a config file.
# Setting it as an environment variable helps avoid repeating it as a flag.
export QW_CONFIG=./config/quickwit.yaml
```

```bash
./quickwit index create --index-config hdfslogs_index_config.yaml
```

You're now ready to fill the index.

## Index logs
The dataset is a compressed [ndjson file](https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz). Instead of downloading it and then indexing the data, we will use pipes to directly send a decompressed stream to Quickwit.
This can take up to 10 min on a modern machine, the perfect time for a coffee break.

```bash
curl https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz | gunzip | ./quickwit index ingest --index hdfslogs
```

You can check it's working by using `search` subcommand and look for `ERROR` in `serverity_text` field:
```bash
./quickwit index search --index hdfslogs  --query "severity_text:ERROR"
```

:::note

The `ingest` subcommand generates [splits](../overview/architecture.md) of 5 millions documents. Each split is a small piece of index represented by a file in which index files and metadata files are saved.

:::


## Start your server

The command `service run searcher` starts an http server which provides a [REST API](../reference/search-api.md).


```bash
./quickwit service run searcher
```

Let's execute the same query on field `severity_text` but with `cURL`:

```bash
curl -v "http://127.0.0.1:7280/api/v1/hdfslogs/search?query=severity_text:ERROR"
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
curl -v 'http://127.0.0.1:7280/api/v1/hdfslogs/search?query=severity_text:ERROR&startTimestamp=1442834249&endTimestamp=1442900000'
```

It should return 6 hits faster as Quickwit will query fewer splits.

## Distributed search

Now that we have indexed our dataset and can do a local search, let's show how easy it is to start a distributed search cluster with Quickwit. We will be launching a three-node cluster.
First, let's download the Searcher's configuration files:

```bash
for i in {1..3}; do 
curl -o searcher-$i.yaml https://raw.githubusercontent.com/quickwit-inc/quickwit/main/config/tutorials/hdfs-logs/searcher-$i.yaml
done
```

Once the configuration files are downloaded, start a searcher node for each of the respective configuration files in a different terminal window.

```bash
# run this in the first terminal window.
./quickwit service run searcher --config ./searcher-1.yaml
```

```bash
# run this in the second terminal window.
./quickwit service run searcher --config ./searcher-2.yaml
```

```bash
# run this in the third terminal window.
./quickwit service run searcher --config ./searcher-3.yaml
```

You will see in your terminal the confirmation that the instance has created or joined a cluster. Example of such a log:

```
INFO quickwit_cluster::cluster: Create new cluster. node_id="searcher-1" listen_addr=127.0.0.1:7280
```

:::note

Quickwit will use the configured `rest_listen_port` for serving the HTTP rest API via TCP as well as maintaining the cluster formation via UDP. Also, it will `{rest_listen_port} + 1` for gRPC communication between instances.

:::

Let's execute a simple query that returns only `ERROR` entries on field `severity_text` on one of our searcher node:

```bash
curl -v 'http://127.0.0.1:7280/api/v1/hdfs_logs/search?query=severity_text:ERROR
```

## Clean

Let's do some cleanup by deleting the index:

```bash
./quickwit index delete --index hdfslogs
```


Congratz! You finished this tutorial! 


To continue your Quickwit journey, check out the [tutorial for distributed search](tutorial-hdfs-logs-distributed-search-aws-s3.md) or dig into the [search REST API](../reference/search-api.md) or [query language](../reference/query-language.md).

