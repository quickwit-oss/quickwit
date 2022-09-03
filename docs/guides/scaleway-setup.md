---
title: Object Storage (Scaleway) setup
sidebar_position: 6
---

In this guide, you will learn how to configure a Quickwit [storage](/docs/reference/storage-uri) for Scaleway.

## Get the access and secret keys from Scaleway 

In your Scaleway console, generate a new API key and note the credentials. 

Once you have the keys, you can follow these steps:

1. Declare the environment variables used by Quickwit to configure the storage:
```bash
export AWS_ACCESS_KEY_ID=****
export AWS_SECRET_ACCESS_KEY=****
```
   
2. Set the endpoint URI: 
```bash
export QW_S3_ENDPOINT=https://s3.fr-par.scw.cloud
```

3. Set the region: 
```bash
export AWS_REGION={your-region}
```

Now you're ready to have your metadata or index data on Scaleway.


## Examples

### Set the Metastore URI

In your [node config file](/docs/configuration/node-config), use `metastore_uri: s3://{your-bucket}/{your-indexes}`.

### Set the Index URI

In your [index config file](/docs/configuration/index-config), use `index_uri: s3://{your-bucket}/{your-indexes}`.

:::note
Note that the URI scheme has still the name `s3` but Quickwit is actually sending HTTP requests to `https://s3.{your-region}.scw.cloud`.
:::
