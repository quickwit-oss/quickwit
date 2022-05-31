---
title: Ingesting data from Amazon Kinesis
description: A short tutorial describing how to set up Quickwit to ingest data from Kinesis in a few minutes
---

In this tutorial, we will describe how to set up Quickwit to ingest data from Kinesis in a few minutes. First, we will create an index and configure a Kinesis source. Then, we will create a Kinesis stream and load some events from the [GH Archive](https://www.gharchive.org/) into it. Finally, we will execute some search and aggregation queries to explore the freshly ingested data.

:::caution
You will incur some charges for using the Amazon Kinesis service during this tutorial.
:::

## Prerequisites

You will need the following to complete this tutorial:
- The AWS CLI version 2 (see [Getting started with the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-prereqs.html) for prerequisites and installation)
- A local Quickwit [installation](/docs/get-started/installation)
- [jq](https://stedolan.github.io/jq/download/)
- [GNU parallel](https://www.gnu.org/software/parallel/)

:::note
`jq` is required to transform on the fly some fields of the GH Archive events whose types are not (yet) supported by Quickwit. The [boolean](https://github.com/quickwit-oss/quickwit/issues/1483) and [datetime](https://github.com/quickwit-oss/quickwit/issues/1328) fields are expected to land in [Quickwit 0.4](https://github.com/quickwit-oss/quickwit/projects/5).
:::

### Create index

First, let's create a new index. Here is the index config and doc mapping corresponding to the schema of the GH Archive events:

```yaml title="index-config.yaml"
#
# Index config file for gh-archive dataset.
#
version: 0

index_id: gh-archive

doc_mapping:
  field_mappings:
    - name: id
      type: text
      tokenizer: raw
    - name: type
      type: text
      fast: true
      tokenizer: raw
    - name: public
      type: u64
      fast: true
    - name: payload
      type: json
      tokenizer: default
    - name: org
      type: json
      tokenizer: default
    - name: repo
      type: json
      tokenizer: default
    - name: actor
      type: json
      tokenizer: default
    - name: other
      type: json
      tokenizer: default
    - name: created_at
      type: i64
      fast: true

indexing_settings:
  timestamp_field: created_at

search_settings:
  default_search_fields: []
```

Execute these Bash commands to download the index config and create the `gh-archive` index.

```bash
# Download GH Archive index config.
wget -O gh-archive.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/gh-archive/index-config.yaml

# Create index.
./quickwit index create --index-config gh-archive.yaml
```

## Create Kinesis source

```yaml title="kinesis-source.yaml"
#
# Kinesis source config file.
#
source_id: kinesis-source
source_type: kinesis
params:
  stream_name: gh-archive
```

Run these commands to download the source config file and create the source.

```bash
# Download Kinesis soure config.
wget https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/gh-archive/kinesis-source.yaml

# Create source.
./quickwit source create --index gh-archive --source-config kinesis-source.yaml
```

## Create and populate Kinesis stream

Now, let's create a Kinesis stream and load some events into it.

:::tip
This step may be fairly slow depending on how much bandwidth is available. You can reduce the volume of data to ingest by downloading a single file from the GH Archive and/or place a `| head -n <number of records> \ ` after the `gunzip` command. You can also speed things up by increasing the number of shards and/or the number of jobs launched by `parallel` (`-j` option).
:::

```bash
# Create a stream named `gh-archive` with 3 shards.
aws kinesis create-stream --stream-name gh-archive --shard-count 8

# Download a few GH Archive files.
wget https://data.gharchive.org/2022-05-12-{10..12}.json.gz

# Load the events into Kinesis stream
gunzip -c 2022-05-12*.json.gz | \
jq -c '.created_at = (.created_at | fromdate) | .public = if .public then 1 else 0 end' | \
parallel --gnu -j8 -N 500 --pipe \
'jq --slurp -c "{\"Records\": [.[] | {\"Data\": (. | tostring), \"PartitionKey\": .id }], \"StreamName\": \"gh-archive\"}" > records-{%}.json && \
aws kinesis put-records --cli-input-json file://records-{%}.json >> out.log'
```

## Launch indexing and search services

Finally, execute this command to start Quickwit in server mode.

```bash
# Launch Quickwit services.
./quickwit run
```

Under the hood, this command spawns an indexer and a searcher. On startup, the indexer will connect to the Kinesis stream specified by the source and start streaming and indexing events from the shards composing the stream. With the default commit timeout value (see [indexing settings](/docs/configuration/index-config#indexing-settings)), the indexer should publish the first split after approximately 60 seconds.

You can run this command (in another shell) to inspect the properties of the index and check the current number of published splits:

```bash
# Display some general information about the index.
./quickwit index describe --index gh-archive
```

Once the first split is published, you can start running search queries. For instance, we can find all the events for the Kubernetes [repository](https://github.com/kubernetes/kubernetes):

```bash
curl 'http://localhost:7280/api/v1/gh-archive/search?query=org.login:kubernetes%20AND%20repo.name:kubernetes'
```

We can also group these events by type and count them:

```
curl -XPOST -H 'Content-Type: application/json' 'http://localhost:7280/api/v1/gh-archive/search' -d '
{
  "query":"org.login:kubernetes AND repo.name:kubernetes",
  "max_hits":0,
  "aggs":{
    "count_by_event_type":{
      "terms":{
        "field":"type"
      }
    }
  }
}'
```

## Tear down resources (optional)

Let's delete the files and resources created for the purpose of this tutorial.

```bash
# Delete Kinesis stream.
aws kinesis delete-stream --stream-name gh-archive

# Delete index.
./quickwit index delete --index gh-archive

# Delete source config.
rm kinesis-source.yaml
```

This concludes the tutorial. If you have any questions regarding Quickwit or encounter any issues, don't hesitate to ask a [question](https://github.com/quickwit-oss/quickwit/discussions) or open an [issue](https://github.com/quickwit-oss/quickwit/issues) on [GitHub](https://github.com/quickwit-oss/quickwit) or contact us directly on [Discord](https://discord.com/invite/MT27AG5EVE).
