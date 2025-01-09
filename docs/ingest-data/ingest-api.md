---
title: Ingest API
description: A short tutorial describing how to send data in Quickwit using the ingest API
tags: [ingest-api, integration]
icon_url: /img/tutorials/quickwit-logo.svg
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

In this tutorial, we will describe how to send data to Quickwit using the ingest API.

You will need a [local Quickwit instance](../get-started/installation) up and running to follow this tutorial.

To start it, run `./quickwit run` in a terminal.

## Create an index

First, let's create a schemaless index.

```bash
# Create the index config file.
cat << EOF > stackoverflow-schemaless-config.yaml
version: 0.7
index_id: stackoverflow-schemaless
doc_mapping:
  mode: dynamic
indexing_settings:
  commit_timeout_secs: 30
EOF
# Use the CLI to create the index...
./quickwit index create --index-config stackoverflow-schemaless-config.yaml
# Or with cURL.
curl -XPOST -H 'Content-Type: application/yaml' 'http://localhost:7280/api/v1/indexes' --data-binary @stackoverflow-schemaless-config.yaml
```

## Ingest data

Let's first download a sample of the [StackOverflow dataset](https://www.kaggle.com/stackoverflow/stacksample).

```bash
# Download the first 10_000 Stackoverflow posts articles.
curl -O https://quickwit-datasets-public.s3.amazonaws.com/stackoverflow.posts.transformed-10000.json
```

You can ingest data either with the CLI or with cURL. The CLI is more convenient for ingesting several GB as Quickwit may return `429` responses if the ingest queue is full. Quickwit CLI will automatically retry ingestion in this case.

```bash
# Ingest the first 10_000 Stackoverflow posts articles with the CLI...
./quickwit index ingest --index stackoverflow-schemaless --input-path stackoverflow.posts.transformed-10000.json --force

# OR with cURL.
curl -XPOST -H 'Content-Type: application/json' 'http://localhost:7280/api/v1/stackoverflow-schemaless/ingest?commit=force' --data-binary @stackoverflow.posts.transformed-10000.json
```

## Execute search queries

You can now search the index.

```bash
curl 'http://localhost:7280/api/v1/stackoverflow-schemaless/search?query=body:python'
```

## Tear down resources (optional)

```bash
curl -XDELETE 'http://localhost:7280/api/v1/indexes/stackoverflow-schemaless'
```

This concludes the tutorial. You can now move on to the [next tutorial](/docs/ingest-data/kafka.md) to learn how to ingest data from Kafka.

## Ingest API versions

In 0.9, Quickwit introduced a new version of the ingest API that enables distributing the indexing in the cluster regardless of the node that received the ingest request. This new ingestion service is often referred to as "Ingest V2" compared to the legacy ingestion (V1). In upcoming versions the new ingest API will also be capable of replicating the write ahead log in order to achieve higher durability.

By default, both ingestion services are enabled and ingest V2 is used. You can toggle this behavior with the following environment variables:

| Variable              | Description   | Default value |
| --------------------- | --------------|-------------- |
| `QW_ENABLE_INGEST_V2` | Start the V2 ingest service and use it by default. | true | 
| `QW_DISABLE_INGEST_V1`| V1 ingest will be used by the APIs only if V2 is disabled. Running V1 along V2 can be useful to finish indexing existing V1 logs. | false |

:::note

These configuration drive the ingest service used both by the `api/v1/<index-id>/ingest` endpoint and the [bulk API](../reference/es_compatible_api.md#_bulk--batch-ingestion-endpoint).

:::
