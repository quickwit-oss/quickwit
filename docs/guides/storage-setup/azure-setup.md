---
title: Azure Blob Storage
sidebar_position: 2
---

In this guide, you will learn how to configure a Quickwit [storage](/docs/reference/storage-uri) for Azure Blob Storage.

## Configure storage account and access key

To set up your storage account and access key, you can either use the environment variables `QW_AZURE_STORAGE_ACCOUNT` and `QW_AZURE_STORAGE_ACCESS_KEY`:

```bash
export QW_AZURE_STORAGE_ACCOUNT= your-azure-account-name
export QW_AZURE_STORAGE_ACCESS_KEY= your-azure-access-key
```

Or, you can configure them through the storage section of the node config file. Here is an example of a storage configuration for Azure in YAML format:

```yaml
storage:
  azure:
    account: your-azure-account-name
    access_key: your-azure-access-key
```

## Set the metastore URI and default index URI

Quickwit expects Azure URIs to match the following format `azure://{container}/{prefix}` where:
- `container` is the container name (or "bucket" in S3 terminology).
- `prefix` is optional and can be any blob prefix.

Here is an example of how to set up a [node config file](/docs/configuration/node-config) for Azure:

```yaml
metastore_uri: azure://my-container/my-indexes  # Applies only for file-backed metastores
default_index_uri: azure://my-container/my-indexes
```

## Set the index URI

Here is an example of how to setup an index URI in the [index config file](/docs/configuration/index-config):
```yaml
index_uri: azure://my-container/my-indexes/my-index
```
