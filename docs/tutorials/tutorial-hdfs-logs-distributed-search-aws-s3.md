---
title: Tutorial - Distributed search on AWS S3 with hdfs logs
sidebar_position: 1
---

In this guide, we will index some logs (40 million log entries, 13 GB decompressed) and start a distributed search server on this index.

Example of a log entry:
```
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

## Prerequisite
- [Install](installation.md) Quickwit on your instances
- The [HDFS log dataset](https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz)
- [Configure your environment](configure-aws-env.md) to let Quickwit access your S3 buckets.

All the following steps can be execucted on any instance.

## Install
```
curl -L https://install.quickwit.io | sh
```


## Create your index

```
# First download the hdfs index from quickwit repository
curl -o hdfslogs_index_config.json https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/index_configs/hdfslogs_index_config.json
quickwit new s3://path-to-your-bucket/hdfs_logs --index-config-path ./hdfslogs_index_config.json
```

## Index logs
To index the dataset, we will download it with curl, decompress it and pipe it to Quickwit (could take between 5 and 10 minutes).

```
curl https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz | gunzip | quickwit index --index-uri s3://your-bucket/hdfs-index
```

## Start instances

TODO