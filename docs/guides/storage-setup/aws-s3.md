---
title: AWS S3
sidebar_position: 1
---

In this guide, you will learn how to configure a Quickwit [storage](/docs/reference/storage-uri) for AWS S3.

## Set your AWS credentials

A simple way to do it is to declare the environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`. For more details, read our guide on [AWS setup](../aws-setup).

## Set the Metastore URI and default index URI

Here is an example of how to set up your [node config file](/docs/configuration/node-config) with S3:

```yaml
metastore_uri: s3://{my-bucket}/indexes
default_index_uri: s3://{my-bucket}/indexes
```

## Set the Index URI

Here is an example of how to setup your index URI in the [index config file](/docs/configuration/index-config): 
```yaml
index_uri: s3://{my-bucket}/indexes/{my-index-id}
```
