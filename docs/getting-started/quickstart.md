---
title: Quickstart
sidebar_position: 1
---

Before running Quickwit search instances on your servers, you will need to create indexes, add documents finally launch the server. In this quick start guide, we will install Quickwit and pass through these steps one by one. All Quickwit commands used in this guide are documented [in the CLI reference documentation](../reference/cli.md).

## Install Quickwit

Let's download and install Quickwit.

```bash
curl -L https://install.quickwit.io | sh
```

This script will automatically pick the correct binary for your environment and then download and unpack it in your working directory.
Once it is here, let's check if it is working!

```bash
./quickwit --version
```

You can now move this executable wherever sensible for your environment. (e.g. `/usr/local/bin`)
You can also install it via [other means](installation.md).

## Use Quickwit's docker image

You can also pull and run the Quickwit binary in an isolated docker container.

```bash
docker run quickwit/quickwit-cli --version
```

## Create your first index

Before adding documents to Quickwit, you need to create an index configured with a JSON `config file`. This config file notably lets you define how to map your input documents to your index fields and whether these fields should be stored and indexed. See the [index config documentation](../reference/index-config.md).

Let's create an index configured to receive Wikipedia articles.

```bash
# First, download the Wikipedia config from Quickwit repository.
curl -o wikipedia_index_config.json https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/index_configs/wikipedia_index_config.json
```

The index config defines three text fields: `title`, `body` and `url`. It also sets two default search fields `body` and `title`. These fields will be used for search if you do not target a specific field in your query. Please note that by default, text fields are [indexed and tokenized](../reference/index-config.md).

And here is the complete config:

```json title="wikipedia_index_config.json"
{
    "default_search_fields": ["body", "title"], // If you do not specify fields in your query, those fields will be used. 
    "field_mappings": [
        {
            "name": "body",
            "type": "text"
        },
        {
            "name": "title",
            "type": "text"
        },
        {
            "name": "url",
            "type": "text",
            "indexed": false, // Field not indexed, you will not be able to search on this field.
            "stored": false  // Field not stored. 
        }
    ]
}
```

Now we can create the index with the command:

```bash

./quickwit new --index-uri file:///your-path-to-your-index/wikipedia --index-config-path ./wikipedia_index_config.json --index wikipedia --metastore-uri file:///path-to-your-metastore
```

The metastore is like a directory of indexes that has it's own storage and some meta information about the indexes.

To simplify our command, we now assume that the index and metastore are to be created in the current directory and use the `pwd` command to specify the index uri:

```bash
./quickwit new --index-uri file://$(pwd)/wikipedia --index-config-path ./wikipedia_index_config.json --index wikipedia --metastore-uri file://$(pwd)/metastore/
```

Check that an empty directory `$(pwd)/wikipedia` has been created, Quickwit will write index files here and a `quickwit.json` which contains the [index metadata](../overview/architecture.md#index-metadata).
You're now ready to fill the index.

:::note

The `index id` will be useful when using the search REST API.

:::


## Let's add some documents

Currently Quickwit can index new line delimited json [ndjson](http://ndjson.org/) datasets.
Let's download [a bunch of wikipedia articles (10 000)](https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json) in [ndjson](http://ndjson.org/) format and index it.

```bash
# Download the first 10_000 Wikipedia articles.
curl -o wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json

# Index our 10k documents.
./quickwit index --index wikipedia --data-dir-path ./wikipedia --metastore-uri file://$(pwd)/metastore --input-path wiki-articles-10000.json
```

Wait one second or two and check if it worked by using `search` command:

```bash
./quickwit search --index wikipedia --metastore-uri file://$(pwd)/metastore --query "barack AND obama"
```

It should return 10 hits. Now you're ready to serve our search API.


## Start server

The command `serve` starts an http server which provides a [REST API](../reference/search-api.md).

```bash
./quickwit serve --index-uri file:///$(pwd)/wikipedia
quickwit serve --metastore-uri file://$(pwd)/metastore
```

Check it's working with a simple GET request in the browser or via cURL:
```bash
curl "http://0.0.0.0:8080/api/v1/wikipedia/search?query=barack+AND+obama"
```

You can also specify the search field with `body:barack AND obbama`:
```bash
curl "http://0.0.0.0:8080/api/v1/wikipedia/search?query=body:barack+AND+obama"
```

Check out the server logs to see what's happening.


:::note

The REST API use `index name` which is defined by the last segment of your `index uri`.

Don't forget to encode correctly the query params to avoid bad request (status 400).

:::


## Clean

Let's do some cleanup by deleting the index:

```bash
./quickwit delete --index wikipedia --metastore-uri file:///$(pwd)/metastore
```

Congrats! You can level up with the following tutorials to discover all Quickwit features.


## TLDR

```bash
curl -o wikipedia_index_config.json https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/index_configs/wikipedia_index_config.json
./quickwit new --index-uri file://$(pwd)/wikipedia --index-config-path ./wikipedia_index_config.json --index wikipedia --metastore-uri file://$(pwd)/metastore/
curl -o wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json
./quickwit index --index wikipedia --data-dir-path ./wikipedia --metastore-uri file://$(pwd)/metastore --input-path wiki-articles-10000.json
./quickwit search --index wikipedia --metastore-uri file://$(pwd)/metastore --query "barack AND obama"
./quickwit delete --index wikipedia --metastore-uri file:///$(pwd)/metastore
```


## Next tutorials

- [Search on logs with timestamp pruning](../tutorials/tutorial-hdfs-logs.md)
- [Setup a distributed search on AWS S3](../tutorials/tutorial-hdfs-logs-distributed-search-aws-s3.md)


