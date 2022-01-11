---
title: Quickstart
sidebar_position: 1
---

Before running Quickwit search instances on your servers, you will need to create indexes, add documents, and finally launch the server. In this quick start guide, we will install Quickwit and pass through these steps one by one. All Quickwit commands used in this guide are documented [in the CLI reference documentation](../reference/cli.md).

## Install Quickwit

Let's download and install Quickwit.

```bash
curl -L https://install.quickwit.io | sh
```

This script will automatically pick the correct binary archive for your environment and then download and unpack it in your working directory.
Once it is done, let's check if it is working!

```bash
cd ./quickwit-v*/
./quickwit --version
```

You can now move this executable directory wherever sensible for your environment and possibly add it to your `PATH` environement.
You can also install it via [other means](installation.md).

## Use Quickwit's docker image

You can also pull and run the Quickwit binary in an isolated docker container.

```bash
docker run quickwit/quickwit --version
```

## Create your first index

Before adding documents to Quickwit, you need to create an index configured with a JSON `config file`. This config file notably lets you define how to map your input documents to your index fields and whether these fields should be stored and indexed. See the [index config documentation](../reference/index-config.md).

Let's create an index configured to receive Wikipedia articles.

```bash
# First, download the Wikipedia config from Quickwit repository.
curl -o wikipedia_index_config.yaml https://raw.githubusercontent.com/quickwit-inc/quickwit/main/config/tutorials/wikipedia/index-config.yaml
```

The index config defines three text fields: `title`, `body` and `url`. It also sets two default search fields `body` and `title`. These fields will be used for search if you do not target a specific field in your query. Please note that by default, text fields are [indexed and tokenized](../reference/index-config.md).

And here is the complete config:

```yaml title="wikipedia_index_config.yaml"
version: 0

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

Let's make use of the default configuration file provided in quickwit installation throughout this guide by setting the `QW_CONFIG` environment variable.

```bash
export QW_CONFIG=./config/quickwit.yaml
```

Now we can create the index with the command:

```bash
./quickwit index create --index-config ./wikipedia_index_config.yaml
```

Check that a directory `./qwdata/wikipedia` has been created, Quickwit will write index files here and a `quickwit.json` which contains the [index metadata](../design/architecture.md#index).
You're now ready to fill the index.


## Let's add some documents

Quickwit can index data from many [sources](../reference/source-config.md). We will use a new line delimited json [ndjson](http://ndjson.org/) datasets as our data source.
Let's download [a bunch of wikipedia articles (10 000)](https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json) in [ndjson](http://ndjson.org/) format and index it.

```bash
# Download the first 10_000 Wikipedia articles.
curl -o wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json

# Index our 10k documents.
./quickwit index ingest --index wikipedia --input-path wiki-articles-10000.json
```

Wait one second or two and check if it worked by using `search` command:

```bash
./quickwit index search --index wikipedia --query "barack AND obama"
```

It should return 10 hits. Now you're ready to serve our search API.


## Start the search service

Quickwit provides a search [REST API](../reference/rest-api.md) that can be started using the `service` subcommand.

```bash
./quickwit service run searcher 
```

Check it's working with a simple GET request in the browser or via cURL:
```bash
curl "http://127.0.0.1:7280/api/v1/wikipedia/search?query=barack+AND+obama"
```

You can also specify the search field with `body:barack AND obbama`:
```bash
curl "http://127.0.0.1:7280/api/v1/wikipedia/search?query=body:barack+AND+obama"
```

Check out the server logs to see what's happening.


:::note

Don't forget to encode correctly the query params to avoid bad request (status 400).

:::


## Clean

Let's do some cleanup by deleting the index:

```bash
./quickwit index delete --index wikipedia
```

Congrats! You can level up with the following tutorials to discover all Quickwit features.


## TLDR

Run the following command from within Quickwit's installation directory.

```bash
curl -o wikipedia_index_config.yaml https://raw.githubusercontent.com/quickwit-inc/quickwit/main/config/tutorials/wikipedia/index-config.yaml
export QW_CONFIG=./config/quickwit.yaml
./quickwit index create --index-config ./wikipedia_index_config.yaml
curl -o wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json
./quickwit index ingest --index wikipedia --input-path ./wiki-articles-10000.json
./quickwit index search --index wikipedia --query "barack AND obama"
./quickwit index delete --index wikipedia
```


## Next tutorials

- [Search on logs with timestamp pruning](../guides/tutorial-hdfs-logs.md)
- [Setup a distributed search on AWS S3](../guides/tutorial-hdfs-logs-distributed-search-aws-s3.md)


