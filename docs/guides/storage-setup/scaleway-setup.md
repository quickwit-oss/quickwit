---
title: Scaleway Storage
sidebar_position: 4
---

In this guide, you will learn how to configure a Quickwit [storage](/docs/reference/storage-uri) for Scaleway.

## Get access and secret keys from Scaleway 

In your Scaleway console, generate a new API key and note the credentials. 

Once you have the keys:

1. Declare the environment variables used by Quickwit to configure the storage:
```bash
export AWS_ACCESS_KEY_ID=****
export AWS_SECRET_ACCESS_KEY=****
```
   
2. Declare the endpoint URI: 
```bash
export QW_S3_ENDPOINT=https://s3.{your-region}.scw.cloud
```

3. Declare the region: 
```bash
export AWS_REGION={your-region}
```

Now you're ready to have your metadata or index data on Scaleway.


## Set the Metastore URI

Here is an example of how to set up your [node config file](/docs/configuration/node-config) with Scaleway:

```yaml
metastore_uri: s3://{my-bucket}/indexes
default_index_uri: s3://{my-bucket}/indexes
```

## Set the Index URI

Here is an example of how to setup your index URI in the [index config file](/docs/configuration/index-config):
```yaml
index_uri: s3://{my-bucket}/indexes/{my-index-id}
```

:::note
Note that the URI scheme has still the name `s3` but Quickwit is actually sending HTTP requests to `https://s3.{your-region}.scw.cloud`.
:::
