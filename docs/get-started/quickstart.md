---
title: Quickstart
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

In this quick start guide, we will install Quickwit, create an index, add documents and finally execute search queries. All the Quickwit commands used in this guide are documented [in the CLI reference documentation](/docs/reference/cli.md).

## Install Quickwit using Quickwit installer

The Quickwit installer automatically picks the correct binary archive for your environment and then downloads and unpacks it in your working directory.
This method works only for [some OS/architectures](installation.md#download), and you will also need to install some [external dependencies](installation.md#note-on-external-dependencies).

```bash
curl -L https://install.quickwit.io | sh
```

```bash
cd ./quickwit-v*/
./quickwit --version
```

You can now move this executable directory wherever sensible for your environment and possibly add it to your `PATH` environment.

## Use Quickwit's Docker image

You can also pull and run the Quickwit binary in an isolated Docker container.

```bash
# Create first the data directory.
mkdir qwdata
docker run --rm quickwit/quickwit --version
```

If you are using Apple silicon based macOS system you might need to specify the platform. You can also safely ignore jemalloc warnings.

```bash
docker run --rm --platform linux/amd64 quickwit/quickwit --version
```

## Start Quickwit server

<Tabs>

<TabItem value="cli" label="CLI">

```bash
./quickwit run
```

</TabItem>

<TabItem value="docker" label="Docker">

```bash
docker run --rm -v $(pwd)/qwdata:/quickwit/qwdata -p 127.0.0.1:7280:7280 quickwit/quickwit run
```

</TabItem>

</Tabs>

Check it's working by browsing the [UI at http://localhost:7280](http://localhost:7280) or do a simple GET with cURL:

```bash
curl http://localhost:7280/api/v1/version
```

## Create your first index

Before adding documents to Quickwit, you need to create an index configured with a YAML config file. This config file notably lets you define how to map your input documents to your index fields and whether these fields should be stored and indexed. See the [index config documentation](/docs/configuration/index-config.md).

Let's create an index configured to receive Stackoverflow posts (questions and answers).

```bash
# First, download the stackoverflow dataset config from Quickwit repository.
curl -o stackoverflow-index-config.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/stackoverflow/index-config.yaml
```

The index config defines nine text fields. Among them there are five text fields: `user`, `tags`, `title`, `type` and `body`. Two of these fields, `body` and `title` are [indexed and tokenized](../configuration/index-config.md#text-type) and they are also used as default search fields, which means they will be used for search if you do not target a specific field in your query. The `tags` field is configured to accept multiple text values. The rest of the text fields are not tokenized and configured as [fast](/docs/configuration/index-config.md#text-type). There are three numeric fields `questionId`, `answerId` and `acceptedAnswerId`. And there is the `creationDate` field that serves as the timestamp for each record.

And here is the complete config:

```yaml title="stackoverflow-index-config.yaml"
#
# Index config file for stackoverflow dataset.
#
version: 0.5

index_id: stackoverflow

doc_mapping:
  field_mappings:
    - name: user
      type: text
      fast: true
      tokenizer: raw
    - name: tags
      type: array<text>
      fast: true
      tokenizer: raw
    - name: type
      type: text
      fast: true
      tokenizer: raw
    - name: title
      type: text
      tokenizer: default
      record: position
      stored: true
    - name: body
      type: text
      tokenizer: default
      record: position
      stored: true
    - name: questionId
      type: u64
    - name: answerId
      type: u64
    - name: acceptedAnswerId
      type: u64
    - name: creationDate
      type: datetime
      fast: true
      input_formats:
        - rfc3339
      precision: seconds
  timestamp_field: creationDate

search_settings:
  default_search_fields: [title, body]

indexing_settings:
  commit_timeout_secs: 5
```

Now we can create the index with the command:

<Tabs>

<TabItem value="cli" label="CLI">

```bash
./quickwit index create --index-config ./stackoverflow-index-config.yaml
```

</TabItem>

<TabItem value="curl" label="CURL">

```bash
curl -XPOST http://127.0.0.1:7280/api/v1/indexes --header "content-type: application/yaml" --data-binary @./stackoverflow-index-config.yaml
```

</TabItem>

</Tabs>

Check that a directory `./qwdata/indexes/stackoverflow` has been created, Quickwit will write index files here and a `metastore.json` which contains the [index metadata](../overview/architecture.md#index).
You're now ready to fill the index.


## Let's add some documents

Quickwit can index data from many [sources](/docs/configuration/source-config.md). We will use a new line delimited json [ndjson](http://ndjson.org/) datasets as our data source.
Let's download [a bunch of stackoverflow posts (10 000)](https://quickwit-datasets-public.s3.amazonaws.com/stackoverflow.posts.transformed-10000.json) in [ndjson](http://ndjson.org/) format and index it.

```bash
# Download the first 10_000 Stackoverflow posts articles.
curl -O https://quickwit-datasets-public.s3.amazonaws.com/stackoverflow.posts.transformed-10000.json
```

<Tabs>

<TabItem value="cli" label="CLI">

```bash
# Index our 10k documents.
./quickwit index ingest --index stackoverflow --input-path stackoverflow.posts.transformed-10000.json
```

</TabItem>

<TabItem value="curl" label="CURL">

```bash
# Index our 10k documents.
curl -XPOST http://127.0.0.1:7280/api/v1/stackoverflow/ingest --data-binary @stackoverflow.posts.transformed-10000.json
```

</TabItem>

</Tabs>

Wait around 10 seconds and check if it worked by using `search` command:

<Tabs>

<TabItem value="cli" label="CLI">

```bash
./quickwit index search --index stackoverflow --query "search AND engine"
```

</TabItem>

<TabItem value="curl" label="CURL">

```bash
curl "http://127.0.0.1:7280/api/v1/stackoverflow/search?query=search+AND+engine"
```

</TabItem>

</Tabs>

It should return 10 hits. Now you're ready to play with the search API.


## Execute search queries


Let's start with a query on the field `title`: `title:search AND engine`:
```bash
curl "http://127.0.0.1:7280/api/v1/stackoverflow/search?query=title:search+AND+engine"
```

The same request can be expressed as a JSON query:
```bash
curl -XPOST "http://localhost:7280/api/v1/stackoverflow/search" -H 'Content-Type: application/json' -d '{
    "query": "title:search AND engine"
}'
```

This format is more verbose but it allows you to use more advanced features such as aggregations. The following query finds most popular tags used on the questions in this dataset:
```bash
curl -XPOST "http://localhost:7280/api/v1/stackoverflow/search" -H 'Content-Type: application/json' -d '{
    "query": "type:question",
    "max_hits": 0,
    "aggs": {
        "foo": {
            "terms":{
                "field":"tags",
                "size": 10
            }
        }
    }
}'
```

As you are experimenting with different queries check out the server logs to see what's happening.

:::note

Don't forget to encode correctly the query params to avoid bad request (status 400).

:::



## Clean

Let's do some cleanup by deleting the index:

<Tabs>

<TabItem value="cli" label="CLI">

```bash
./quickwit index delete --index stackoverflow
```

</TabItem>

<TabItem value="rest" label="REST">

```bash
curl -XDELETE http://127.0.0.1:7280/api/v1/indexes/stackoverflow
```

</TabItem>

</Tabs>

Congrats! You can level up with the following tutorials to discover all Quickwit features.


## TLDR

Run the following command from within Quickwit's installation directory.

```bash
curl -o stackoverflow-index-config.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/stackoverflow/index-config.yaml
./quickwit index create --index-config ./stackoverflow-index-config.yaml
curl -O https://quickwit-datasets-public.s3.amazonaws.com/stackoverflow.posts.transformed-10000.json
./quickwit index ingest --index stackoverflow --input-path ./stackoverflow.posts.transformed-10000.json
./quickwit index search --index stackoverflow --query "search AND engine"
./quickwit index delete --index stackoverflow
```


## Next tutorials

- [Search on logs with timestamp pruning](/docs/get-started/tutorials/tutorial-hdfs-logs)
- [Setup a distributed search on AWS S3](/docs/get-started/tutorials/tutorial-hdfs-logs-distributed-search-aws-s3)
