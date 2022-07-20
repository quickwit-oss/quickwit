---
title: AWS cluster setup
sidebar_position: 3
---

Setting up a Quickwit cluster on AWS requires the configuration of three elements:
- AWS credentials
- AWS Region
- Network configuration

## AWS credentials

When starting a node, Quickwit attempts to find AWS credentials using the credential provider chain implemented by [rusoto_core::ChainProvider](https://docs.rs/rusoto_credential/latest/rusoto_credential/struct.ChainProvider.html) and looks for credentials in this order:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`(optional)).

2. Credential profiles file, typically located at `~/.aws/credentials` or otherwise specified by the `AWS_SHARED_CREDENTIALS_FILE` and `AWS_PROFILE` environment variables if set and not empty.

3. Amazon ECS container credentials, loaded from the Amazon ECS container if the environment variable `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` is set.

4. Instance profile credentials, used on Amazon EC2 instances, and delivered through the Amazon EC2 metadata service.

If all these possibilities are exhausted, an error is returned.


## AWS Region

Quickwit attempts to find an AWS Region in multiple locations and with the following order of precedence:

1. Environment variables (`AWS_REGION` then `AWS_DEFAULT_REGION`)

2. Config file, typically located at `~/.aws/config` or otherwise specified by the `AWS_CONFIG_FILE` environment variable if set and not empty.

3. Amazon EC2 instance metadata service determining the region of the currently running Amazon EC2 instance.

4. Default value: `us-east-1`


:::note

AWS credentials or Region resolution may take a few seconds, especially if the Amazon EC2 instance metadata service is slow or unavailable.

:::


## Network configuration

### Security groups

In order to communicate with each other, nodes must reside in security groups that allow inbound and outbound traffic on one UDP port and two TCP ports. Please, refer to the [ports configuration](../configuration/ports-config.md) page for more details.


## Common errors

If you set the wrong credentials, you will see this error message with `Unauthorized` in your terminal:

```bash
Command failed: Another error occurred. `Metastore error`. Cause: `StorageError(kind=Unauthorized, source=Failed to fetch object: s3://quickwit-dev/my-hdfs/metastore.json)`
```

If you put the wrong region, you will see this one:

```bash
Command failed: Another error occurred. `Metastore error`. Cause: `StorageError(kind=InternalError, source=Failed to fetch object: s3://your-bucket/your-index/metastore.json)`.
```
