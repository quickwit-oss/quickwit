---
title: Set up your AWS S3 environment 
sidebar_position: 3
---

To let Quickwit access your AWS S3 buckets and form a cluster, you need two things: first setup your credentials 
and region and then setup network rules so that instances can communicate between them.

## Credentials and region
To let Quickwit stores your indexes on AWS S3, you need to define three environment variables:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION` or `AWS_DEFAULT_REGION`. If variables are malformed, it will fallback to us-east-1 region.

You can also have these variables defined in a `~/.aws/credentials` and `~/.aws/config` files.


## Network
Cluster membership and search workload distribution need UDP and TCP communication between instances. You need to authorize UDP and TCP on relevant ports between them to make it work: by default, TCP port 8080 is used by the web server, TCP and UDP 8081 ports (8080 + 1) are used by the cluster membership protocol and TCP port 8082 (8080 + 2) is used for gRPC communication.


## Common errors
If you put the wrong credentials, you will see this error message with `Unauthorized` in your terminal:

```bash
Command failed: Another error occured. `Metastore error`. Cause: `StorageError(kind=Unauthorized, source=Failed to fetch object: s3://quickwit-dev/my-hdfs/quickwit.json)`
```

If you put the wrong region, you will see this one:

```bash
Command failed: Another error occured. `Metastore error`. Cause: `StorageError(kind=InternalError, source=Failed to fetch object: s3://your-bucket/your-index/quickwit.json)`.
```

:::note

AWS will try different options to find the credentials, this resolution may take a few seconds before failing expecially when aws is not configured. 

:::

