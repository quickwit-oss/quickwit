---
title: Tutorial - Distributed search on AWS S3 with hdfs logs
sidebar_position: 3
---

## Prerequisite
- [Install](installation.md) `quickwit-cli`
- Download the [HDFS log dataset]()
- Choose/Create the AWS S3 bucket you will used to store the index, see [AWS doc](link to aws).

## Create your index

```
# First download the hdfs mapper from quickwit repository
curl https://path-to-hdfs-mapper/hdfs_doc_mapper.json
quickwit-cli new s3://path-to-your-bucket/hdfs --doc-mapper-config-path ./hdfs_doc_mapper.json
```

## Index logs

```
quickwit-cli index --index-uri s3://path-to-your-bucket/hdfs --input-path wikipedia.json
```

## Start instances

TODO