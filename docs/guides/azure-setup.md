---
title: Azure storage setup
sidebar_position: 4
---

In this guide, you will learn how to configure a Quickwit [storage](/docs/reference/storage-uri) for Azure.

## Get the access key

The access key can be accessed in your `Storage Account` inside the `Access keys` directory. There are 2 keys available and either can be used.

Once you have the key, you can follow this step:

Declare the environment variable used by Quickwit to configure the storage:
```bash
export QW_AZURE_ACCESS_KEY=****
```

## Examples

### Set the Metastore URI

In your [node config file](/docs/configuration/node-config), use `metastore_uri: azure://{your-storage-account}/{your-container}`.

### Set the Index URI

In your [index config file](/docs/configuration/index-config), use `index_uri: azure://{your-storage-account}/{your-container}`.
