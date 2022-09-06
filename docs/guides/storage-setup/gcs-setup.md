---
title: Google Cloud Storage
sidebar_position: 3
---

In this guide, you will learn how to configure a Quickwit [storage](/docs/reference/storage-uri) for GCS.

## Get access and secret keys from Google Console Cloud 

As GCS is S3-Compatible, you can go to the [interoperability settings](https://console.cloud.google.com/storage/settings;tab=interoperability) in the Google Cloud Console to get the access & secret keys for the environment. 
   
Once you have the keys, you can follow these steps:

1. Declare the environment variables used by Quickwit to configure the storage:
```bash
export AWS_ACCESS_KEY_ID=****
export AWS_SECRET_ACCESS_KEY=****
```
   
2. Set the endpoint URI: 
```bash
export QW_S3_ENDPOINT=https://storage.googleapis.com
```

Now you're ready to have your metadata or index data on GCS.


## Set the Metastore URI and default index URI

Here is an example of how to set up your [node config file](/docs/configuration/node-config) with GCS:

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
Note that the URI scheme has still the name `s3` but Quickwit is actually sending HTTP requests to `https://storage.googleapis.com`.
:::
