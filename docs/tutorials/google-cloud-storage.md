---
title: Index a logging dataset on Google Cloud Storage 
description: Learn how to set up Google Cloud Storage, create an index, and start the Quickwit server.
tags: [GCS, integration]
icon_url: /img/tutorials/gcs.png
---

This tutorial will show you how to index approximately 40 million log entries (13 GB decompressed) on Google Cloud Storage and start the Quickwit server.

Some references for this tutorial include [Storage URI](/docs/reference/storage-uri) and [Node configuration](/docs/configuration/node-config).

## Install & Configure
Let's first install the Quickwit binary, configure it and provide Quickwit access to the buckets.
```bash
curl -L https://install.quickwit.io | sh
cd quickwit-v*/
```
:::note
All the steps can be done either on the virtual machine or locally.
:::

Now let's set up the endpoint url for Google Cloud Storage:
```bash
export QW_S3_ENDPOINT=https://storage.googleapis.com
```
Lastly, get the secret & the access key from the [Google Cloud Console settings](https://console.cloud.google.com/storage/settings;tab=interoperability) and modify the environment:

```bash
export AWS_SECRET_ACCESS_KEY=***
export AWS_ACCESS_KEY_ID=***
```
## Create your index

```bash
# First, download the hdfs logs config from Quickwit repository.
curl -o hdfs_logs_index_config.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/hdfs-logs/index-config.yaml
```

The index config defines five fields: `timestamp`, `tenant_id`, `severity_text`, `body`, and one object field for the nested values `resource.service` . It also sets the `default_search_fields`, the `tag_fields`, and the `timestamp_field`. The `timestamp_field` and `tag_fields` are used by Quickwit for [splits pruning](/docs/concepts/architecture) at query time to boost search speed. Check out the [index config docs](/docs/configuration/index-config) for more details.

```yaml title="hdfs_logs_index_config.yaml"
version: 0

doc_mapping:
  field_mappings:
    - name: severity_text
      type: text
      tokenizer: raw
    - name: tenant_id
      type: u64
      fast: true
    - name: body
      type: text
      tokenizer: default
      record: position
    - name: resource
      type: object
      field_mappings:
        - name: service
          type: text
          tokenizer: raw
  tag_fields: [tenant_id]

indexing_settings:
  timestamp_field: timestamp

search_settings:
  default_search_fields: [severity_text, body]
```
Provide the bucket path:
```bash
export S3_PATH=s3://{path/to/bucket}/indexes
```
Create a config file:
```yaml title="config.yaml"
version: 0
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
```
Now we can create an index with the `create` command:
```bash
./quickwit index create --index-config hdfs_logs_index_config.yaml
```

:::note

If executed locally, the `create` command creates the index locally and then uploads a json file `metastore.json` to your bucket at `s3://path-to-your-bucket/hdfs-logs/metastore.json`.

:::


## Index logs

The dataset is a compressed [NDJSON file](https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz). Instead of downloading and indexing the data in separate steps, we will use pipes to send a decompressed stream to Quickwit directly.

```bash
curl https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz | gunzip | ./quickwit index ingest --index hdfs-logs
```

:::note

4GB of RAM is enough to index this dataset; an instance like `t4g.medium` with 4GB and 2 vCPU indexed this dataset in 20 minutes.

This step can also be done on your local machine. The `ingest` subcommand generates locally [splits](/docs/concepts/architecture) of 10 million documents and will upload them on your bucket. Concretely, each split is a bundle of index files and metadata files.

:::

If you are in a hurry, use the sample dataset that contains 10 000 documents, we will use this dataset for the example queries:

```bash
curl https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants-10000.json | ./quickwit index ingest --index hdfs-logs
```




You can check it's working by using `search` subcommand and look for `INFO` in `severity_text` field:
```bash
./quickwit index search --index hdfs-logs  --query "severity_text:INFO"
```

Now that we have indexed the logs and can search them from one instance, it's time to start the Quickwit server.

## Start Quickwit server

Let's start a Quickwit server. 

```bash
# Start the http server search service.
./quickwit run --service searcher
```
Once started, you would see the following success result:

```bash
INFO quickwit_serve::rest: Searcher ready to accept requests at http://127.0.0.1:7280/
```

Now we can query the instance directly by issuing http requests to the rest API endpoint.

```bash
curl -v "http://127.0.0.1:7280/api/v1/hdfs-logs/search?query=severity_text:INFO"
```

## Clean

Let's do some cleanup by deleting the index:

```bash
./quickwit index delete --index hdfs-logs
```

Congratulations! You finished the tutorial!

To continue your Quickwit journey, check out the [search REST API reference](https://quickwit.io/docs/reference/rest-api) or the [query language reference](https://quickwit.io/docs/reference/query-language).