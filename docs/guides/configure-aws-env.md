---
title: Set up your AWS S3 environment
sidebar_position: 3
---

To let Quickwit access your AWS S3 buckets and form a cluster, you need two things: first setup your credentials
and region and then set up network rules so that instances can communicate between them.

## Credentials
To let Quickwit store your indexes on AWS S3, you need to define three environment variables:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION` or `AWS_DEFAULT_REGION`. If variables are malformed, it will fall back to us-east-1 region.

You can also have these variables defined in a `~/.aws/credentials` and `~/.aws/config` files.

# Region

Quickwit will attempt to different method to sniff the Amazon S3 Region that it should use.
In order of prority, it will attempt to read it from:
- `AWS_DEFAULT_REGION` environment variable
- `AWS_REGION` environment variable
- ec2 instance metadata
- a config file defined in the `AWS_CONFIG_FILE` env variable
- a config file located at `~/.aws/config`
- fallback to us-east-1.

## Network
Cluster membership and search workload distribution need UDP and TCP communication between instances. You need to authorize UDP and TCP on relevant ports between them to make it work.
[More details](../configuration/ports-config.md).


## Common errors
If you put the wrong credentials, you will see this error message with `Unauthorized` in your terminal:

```bash
Command failed: Another error occured. `Metastore error`. Cause: `StorageError(kind=Unauthorized, source=Failed to fetch object: s3://quickwit-dev/my-hdfs/metastore.json)`
```

If you put the wrong region, you will see this one:

```bash
Command failed: Another error occured. `Metastore error`. Cause: `StorageError(kind=InternalError, source=Failed to fetch object: s3://your-bucket/your-index/metastore.json)`.
```

:::note

AWS will try different options to find the credentials; this resolution may take up to 5 seconds before failing, especially when AWS is not configured.

:::

