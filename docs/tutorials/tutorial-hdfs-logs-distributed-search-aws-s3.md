---
title: Distributed search on AWS S3
sidebar_position: 1
---

In this guide, we will index about 40 million log entries (13 GB decompressed) on AWS S3 and start a distributed search server.

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

:::caution

Before using Quickwit with an object storage, check out our [advice](../administration/cloud-env.md) for deploying on AWS S3 to avoid some bad surprises at the end of the month.

:::


## Prerequisite
- [Configure your environment](configure-aws-env.md) to let Quickwit access your S3 buckets.

All the following steps can be executed on any instance.

## Install

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
for the nested values `resource.service` . It also sets the `default_search_fields`, the `tag_fields`, and the `timestamp_field`. The `timestamp_field` and `tag_fields` are used by Quickwit for [splits pruning](../overview/architecture.md) at query time to boost search speed. Check out the [index config docs](../reference/index-config.md) for more details.

```yaml title="hdfslogs_index_config.yaml"
version: 0

doc_mapping:
  field_mappings:
    - name: timestamp
      type: i64
      fast: true
    - name: severity_text
      type: text
      tokenizer: raw
    - name: body
      type: text
      tokenizer: default
      record: position
    - name: resource
      type: object
      field_mappings:
        - name: service
          type: text
          tokenizer: raw
  tag_fields: [tenant_id]
  store_source: true

indexing_settings:
  timestamp_field: timestamp

search_settings:
  default_search_fields: [severity_text, body]
```

To index our dataset and serve it on a Quickwit cluster, we first need to create a few search nodes configuration files. Please, define your S3 bucket path now to ease the config files creation process.

```bash
export S3_PATH=s3://path/to/bucket/indexes
```

```bash
# configuration for our first node
echo "version: 0
node_id: searcher-1
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
searcher:
  rest_listen_address: 127.0.0.1
  rest_listen_port: 7280
  peer_seeds:
    - 127.0.0.1:7290 # searcher-2
    - 127.0.0.1:7300 # searcher-3
" > searcher-1.yaml
```

```bash
# configuration for our second node
echo "version: 0
node_id: searcher-2
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
searcher:
  rest_listen_address: 127.0.0.1
  rest_listen_port: 7290
  peer_seeds:
    - 127.0.0.1:7280 # searcher-1
    - 127.0.0.1:7300 # searcher-3
" > searcher-2.yaml
```

```bash
# configuration for our third node
echo "version: 0
node_id: searcher-3
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
searcher:
  rest_listen_address: 127.0.0.1
  rest_listen_port: 7300
  peer_seeds:
    - 127.0.0.1:7280 # searcher-1
    - 127.0.0.1:7290 # searcher-2
" > searcher-3.yaml
```

Quickwit's default configuration is so great that we can directly use one of the searcher node configuration to perform our indexing. Let's use the file `./searcher-1.yaml` as the configuration file.

Now let's can create the index with the `create` subcommand directly on S3:

```bash
./quickwit index create --index hdfslogs --index-config ./hdfslogs_index_config.yaml --config ./searcher-1.yaml
```

:::note

This step can also be executed on your local machine. The `create` command creates the index locally and then uploads a json file `quickwit.json` to your bucket at `s3://path-to-your-bucket/hdfslogs/quickwit.json`. 

:::

## Index logs
The dataset is a compressed [ndjson file](https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz). Instead of downloading and indexing the data in separate steps, we will use pipes to send a decompressed stream to Quickwit directly.

```bash
curl https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz | gunzip | ./quickwit index ingest --index hdfslogs --config ./searcher-1.yaml
```

:::note

4GB of RAM is enough to index this dataset; an instance like `t4g.medium` with 4GB and 2 vCPU indexed this dataset in 20 minutes.   

This step can also be done on your local machine. The `ingest` subcommand generates locally [splits](../overview/architecture.md) of 10 million documents and will upload them on your bucket. Concretely, each split is bundle of index files and metadata files.

:::


You can check it's working by using `search` subcommand and look for `ERROR` in `serverity_text` field:
```bash
./quickwit index search --index hdfslogs --config ./searcher-1.yaml --query "severity_text:ERROR"
```


## Start your servers

Quickwit provides two services that can be started through the `run <service-name>` subcommand.
Specifying the service name as `searcher` starts an HTTP server which provides a [REST API](../reference/search-api.md).

Now is the time to start the searcher nodes one at a time. 
Run the following command on each one of your instances by specifying the corresponding node configuration.

```bash
# Then start the http server search service.
./quickwit service run searcher --config ./searcher-1.yaml
```

You will see in your terminal the confirmation that the instance has created a new cluster. Example of such a log:

```
INFO quickwit_cluster::cluster: Create new cluster. node_id="searcher-1" listen_addr=127.0.0.1:7280
```

:::note

Quickwit will use the configured `rest_listen_port` for serving the HTTP rest API via TCP as well as maintaining the cluster formation via UDP. Also, it will `{rest_listen_port} + 1` for gRPC communication between instances.

In AWS, you can create a security group to group these inbound rules. Check out the [network section](configure-aws-env.md) of our AWS setup guide.

:::


## Load balancing incoming requests

Now that you have a search cluster, ideally, you will want to load balance external requests. This can quickly be done by adding an AWS load balancer to listen to incoming HTTP or HTTPS traffic and forward it to a target group.
You can now play with your cluster, kill processes randomly, add/remove new instances, and keep calm.


## Use time pruning

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

You can see that this query has only 364 hits and that the server responds in 0.5 seconds.

The index config shows that we can use the timestamp field parameters `startTimestamp` and `endTimestamp` and benefit from time pruning. Behind the scenes, Quickwit will only query [splits](../overview/architecture.md) that have logs in this time range. This can have a significant impact on speed.


```bash
curl -v 'http://your-load-balancer/api/v1/hdfs_logs/search?query=severity_text:ERROR&startTimestamp=1442834249&endTimestamp=1442900000'
```

Returns 6 hits in 0.36 seconds.


## Clean

Let's do some cleanup by deleting the index:

```bash
./quickwit index delete --index hdfslogs  --config ./searcher-1.yaml
```

Congratz! You finished this tutorial! 

To continue your Quickwit journey, check out the [search REST API reference](../reference/search-api.md) or the [query language reference](../reference/query-language.md).
