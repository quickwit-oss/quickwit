---
title: Quickstart
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


Before running Quickwit search instances on your servers, you will need to create indexes, add documents, and finally launch the server. In this quick start guide, we will install Quickwit and go through these steps one by one. All the Quickwit commands used in this guide are documented [in the CLI reference documentation](../reference/cli.md).

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
docker run quickwit/quickwit --version
```


## Create your first index

Before adding documents to Quickwit, you need to create an index configured with a JSON `config file`. This config file notably lets you define how to map your input documents to your index fields and whether these fields should be stored and indexed. See the [index config documentation](../configuration/index-config.md).

Let's create an index configured to receive Wikipedia articles.

```bash
# First, download the Wikipedia config from Quickwit repository.
curl -o wikipedia-index-config.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/wikipedia/index-config.yaml
```

The index config defines three text fields: `title`, `body` and `url`. It also sets two default search fields `body` and `title`. These fields will be used for search if you do not target a specific field in your query. Please note that by default, text fields are [indexed and tokenized](../configuration/index-config.md).

And here is the complete config:

```yaml title="wikipedia_index_config.yaml"
version: 0
index_id: wikipedia
doc_mapping:
  field_mappings:
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
    - name: url
      type: text
      tokenizer: raw

search_settings:
  # If you do not specify fields in your query, those fields will be used.
  default_search_fields: [title, body]
```

Now we can create the index with the command:

<Tabs>

<TabItem value="cli" label="CLI">

```bash
./quickwit index create --index-config ./wikipedia-index-config.yaml
```

</TabItem>

<TabItem value="docker" label="Docker">

```bash
# Create first the data directory.
mkdir qwdata
docker run -v $(pwd)/qwdata:/quickwit/qwdata -v $(pwd)/wikipedia-index-config.yaml:/quickwit/index-config.yaml quickwit/quickwit index create --index-config index-config.yaml
```

</TabItem>

</Tabs>

Check that a directory `./qwdata/indexes/wikipedia` has been created, Quickwit will write index files here and a `quickwit.json` which contains the [index metadata](../concepts/architecture.md#index).
You're now ready to fill the index.


## Let's add some documents

Quickwit can index data from many [sources](../configuration/source-config.md). We will use a new line delimited json [ndjson](http://ndjson.org/) datasets as our data source.
Let's download [a bunch of wikipedia articles (10 000)](https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json) in [ndjson](http://ndjson.org/) format and index it.

```bash
# Download the first 10_000 Wikipedia articles.
curl -o wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json
```

<Tabs>

<TabItem value="cli" label="CLI">

```bash
# Index our 10k documents.
./quickwit index ingest --index wikipedia --input-path wiki-articles-10000.json
```

</TabItem>

<TabItem value="docker" label="Docker">

```bash
docker run -v $(pwd)/qwdata:/quickwit/qwdata -v $(pwd)/wiki-articles-10000.json:/quickwit/docs.json quickwit/quickwit index ingest --index wikipedia --input-path docs.json
```

</TabItem>

</Tabs>

Wait one second or two and check if it worked by using `search` command:

<Tabs>

<TabItem value="cli" label="CLI">

```bash
./quickwit index search --index wikipedia --query "barack AND obama"
```

</TabItem>

<TabItem value="docker" label="Docker">

```bash
docker run -v $(pwd)/qwdata:/quickwit/qwdata quickwit/quickwit index search --index wikipedia --query "barack AND obama"
```

</TabItem>

</Tabs>

It should return 10 hits. Now you're ready to serve our search API.


## Start the search service

Quickwit provides a search [REST API](../reference/rest-api.md) that can be started using the `service` subcommand.

<Tabs>

<TabItem value="cli" label="CLI">

```bash
./quickwit run --service searcher
```

</TabItem>

<TabItem value="docker" label="Docker">

```bash
docker run -v $(pwd)/qwdata:/quickwit/qwdata -p 127.0.0.1:7280:7280 quickwit/quickwit run --service searcher
```

</TabItem>

</Tabs>


Check it's working by browsing the [UI at http://localhost:7280](http://localhost:7280) or do a simple GET with cURL:
```bash
curl "http://127.0.0.1:7280/api/v1/wikipedia/search?query=barack+AND+obama"
```

You can also specify the search field with `body:barack AND obama`:
```bash
curl "http://127.0.0.1:7280/api/v1/wikipedia/search?query=body:barack+AND+obama"
```

Check out the server logs to see what's happening.


:::note

Don't forget to encode correctly the query params to avoid bad request (status 400).

:::



## Clean

Let's do some cleanup by deleting the index:

<Tabs>

<TabItem value="cli" label="CLI">

```bash
./quickwit index delete --index wikipedia
```

</TabItem>

<TabItem value="docker" label="Docker">

```bash
docker run -v $(pwd)/qwdata:/quickwit/qwdata quickwit/quickwit index delete --index wikipedia
```

</TabItem>

</Tabs>

Congrats! You can level up with the following tutorials to discover all Quickwit features.


## TLDR

Run the following command from within Quickwit's installation directory.

```bash
curl -o wikipedia-index-config.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/wikipedia/index-config.yaml
./quickwit index create --index-config ./wikipedia-index-config.yaml
curl -o wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json
./quickwit index ingest --index wikipedia --input-path ./wiki-articles-10000.json
./quickwit index search --index wikipedia --query "barack AND obama"
./quickwit index delete --index wikipedia
```


## Next tutorials

- [Search on logs with timestamp pruning](../guides/tutorial-hdfs-logs.md)
- [Setup a distributed search on AWS S3](../guides/tutorial-hdfs-logs-distributed-search-aws-s3.md)
