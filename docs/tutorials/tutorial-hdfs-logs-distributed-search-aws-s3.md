---
title: Distributed search on AWS S3
sidebar_position: 1
---

In this guide, we will index some logs (40 million log entries, 13 GB decompressed) and start a distributed search server on this index.

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

Before using Quickwit with an object storage, checkout our [advice](../administration/cloud-env.md) for deploying on AWS S3 to avoid some bad surprises at the end of the month.

:::


## Prerequisite
- [Configure your environment](configure-aws-env.md) to let Quickwit access your S3 buckets.

All the following steps can be execucted on any instance.

## Install

```bash
curl -L https://install.quickwit.io | sh
```


## Create your index

```bash
# First download the index config from quickwit repository
curl -o hdfslogs_index_config.json https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/index_configs/hdfslogs_index_config.json
quickwit new --index-uri s3://path-to-your-bucket/hdfs_logs --index-config-path ./hdfslogs_index_config.json
```

:::note

This step can be executed on your local machine. The `new` command create the index locally and will then only lupload a single `json file` to your bucket at `s3://path-to-your-bucket/hdfs_logs/quickwit.json`. 

:::

## Index logs
To index the dataset, we will download [data](https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz) with curl, decompress it and pipe it to Quickwit (can take 5 minutes on an apple M1).

```bash
curl https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz | gunzip | quickwit index --index-uri s3://path-to-your-bucket/hdfs_logs
```

:::note

4GB of RAM are enough to index this dataset, an instance like `t4g.medium` with 4GB and 2 vCPU indexed this dataset in 20 minutes.   

This step can be also done on your local machine. The `index` command generates locally [splits](../overview/architecture.md) of 5 millions documents and will upload them on your bucket. Concretely, each split is represented by a directory in which split index files are saved, uploading a split is equivalent of uploading 9 files at `s3://path-to-your-bucket/hdfs_logs/{split_id}/`.

:::

## Start instances

Run on each of your instance the following command:

```bash
quickwit serve --index-uri s3://path-to-your-bucket/hdfs_logs --peer-seed=ip1,ip2,ip3
```

You will see in your terminal the confirmation that the instance has joined the cluster. Example of such a log:
```
INFO quickwit_cluster::cluster: Joined. host_key=019e4c0e-165a-430d-8ef6-7b7a035decac remote_host=Some(172.31.66.143:8081)
```

:::note

You need to allow UDP trafic for cluster formation and allow TCP for gRPC communication between instannces.
In AWS, you can create a security group to group these inbound rules. Checkout the [network section](configure-aws-env.md) of our aws set up guide.

:::

:::caution

Avoid putting the instance ip in the peer seed list, this generates a [bug](https://github.com/quickwit-inc/quickwit/issues/267) which is known and hopefully it will be fixed very soon. Please apologize for the trouble.

:::

## Load balancing incoming requests

Now that you have a search cluster, ideally you will want to load balance external requests. This can easily be done
by adding a AWS load balancer which will listen to incoming HTTP or HTTPS trafic and forward to a target group.
You can now play with your cluster, just kill processes randomly, add/remove new instance and keep cool.


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

You can see that this query has only 364 hits and that the server responds in 0.5 second.

We can also add `startTimestamp` and `endTimestamp` to have a more accurate query and benefit from time pruning on timestamp field. Time pruning means that Quickwit will only query splits concerned by this time range. 
This can have an important impact on speed.


```bash
curl -v 'http://your-load-balancer/api/v1/hdfs_logs/search?query=severity_text:ERROR&startTimestamp=1442834249&endTimestamp=1442900000'
```

returns 6 hits in 0.36 seconds.

Congratz! You finished this tutorial! To continue your Quickwit journey, checkout the [search REST API reference](../reference/search-api.md) or the [query language reference](../reference/query-language.md).
