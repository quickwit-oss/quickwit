---
title: Azure Blob Storage
sidebar_position: 2
---

In this guide, you will learn how to configure a Quickwit [storage](/docs/reference/storage-uri) for Azure Blob Storage.

## Get the access key

The access key can be accessed in your `Storage Account` inside the `Access keys` directory. 

Declare the environment variable used by Quickwit to configure the storage:
```bash
export QW_AZURE_ACCESS_KEY=****
```

## Set the Metastore URI and default index URI

Here is an example of how to set up your [node config file](/docs/configuration/node-config) with GCS:

```yaml
metastore_uri: azure://{my-storage-account}/indexes
default_index_uri: azure://{my-storage-account}/indexes
```

## Set the Index URI

Here is an example of how to setup your index URI in the [index config file](/docs/configuration/index-config):
```yaml
index_uri: azure://{my-storage-account}/indexes/{my-index-id}
```
