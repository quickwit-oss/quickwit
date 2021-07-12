---
title: Quickstart
sidebar_position: 1
---

Before running quickwit search instances on your servers, you will need to create indexes, add documents finally launch the server. In this quick start guide, we will install Quickwit and pass through these steps one by one. All Quickwit commands used in this guide are documented [in the CLI reference documentation](../reference/cli.md).

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

## Create your first index

Before adding documents to Quickwit, you need to create an index configured with a JSON `config file`. This config file notably lets you define how to map your input documents to your index fields and whether these fields should be stored and indexed. See the [index config documentation](../reference/index-config.md).

Let's create an index configured to receive Wikipedia articles.

```bash
# First, download the Wikipedia config from Quickwit repository.
curl -o wikipedia_index_config.json https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/index_configs/wikipedia_index_config.json
```

The index config defines three text fields: `title`, `body` and `url`. It also sets two default search fields `body` and `title`. These fields will be used for search if you do not target a specific field in your query. Please note that by default, text fields are indexed and tokenized.

And here is the complete config:

```json title="wikipedia_index_config.json"
{
    "default_search_fields": ["body", "title"],
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
            "indexing": false
        }
    ]
}
```

Now we can create the index with the command:

```bash
quickwit new --index-uri file:///your-path-to-your-index/wikipedia --index-config-path ./wikipedia_index_config.json
```

To simplify our command, we now assume that the index is to be created in the current directory and use `pwd` variable to specify the index uri:

```bash
quickwit new --index-uri file:///$(pwd)/wikipedia --index-config-path ./wikipedia_index_config.json
```

Check that an empty directory `/$(pwd)/wikipedia` has been created, Quickwit will write index files here and a `quickwit.json` which contains the [index metadata](../overview/architecture.md#index-metadata).
You're now ready to fill the index.


:::note

Behind the scenes, Quickwit will parse the index-uri and use the last part to infer your `index name`. In the example above, our index name is Wikipedia. This `index name` will be useful when using the search REST API.

:::


## Let's add some documents

Currently Quickwit can index new line delimited json [ndjson](http://ndjson.org/) datasets.
Let's download [a bunch of wikipedia articles (10 000)](https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json) in [ndjson](http://ndjson.org/) format and index it.

```bash
# Download the first 10_000 Wikipedia articles.
curl -o wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json

# Index our 10k documents.
quickwit index --index-uri file:///$(pwd)/wikipedia --input-path wiki-articles-10000.json
```

Wait one second or two and check if it worked by using `search` command:

```bash
quickwit search --index-uri file:///$(pwd)/wikipedia --query "barack AND obama"
```

It should return 10 hits. Now you're ready to serve our search API.


## Start server

The command `serve` starts an http server which provides a [REST API](../reference/search-api.md).

```bash
quickwit serve --index-uri file:///$(pwd)/wikipedia
```

Check it's working with a simple GET request in the browser or via cURL:
```bash
curl http://0.0.0.0:8080/api/v1/wikipedia/search?query=barack+AND+obama
```

:::note

The REST API use `index name` which is defined by the last segment of your `index uri`.

:::



## Clean

Let's do some cleanup by deleting the index:

```bash
quickwit delete --index-uri file:///$(pwd)/wikipedia
```

Congrats! You can level up with the following tutorials to discover all Quickwit features.

## TLDR

```bash
curl -o wikipedia_index_config.json https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/index_configs/wikipedia_index_config.json
quickwit new --index-uri file:///$(pwd)/wikipedia --index-config-path ./wikipedia_index_config.json
curl -o wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json
quickwit index --index-uri file:///$(pwd)/wikipedia --input-path wiki-articles-10000.json
quickwit search --index-uri file:///$(pwd)/wikipedia --query "barack AND obama"
quickwit delete --index-uri file:///$(pwd)/wikipedia
```


## Next tutorials

- [Setup a distributed search on AWS S3](tutorial-distributed-search-aws-s3.md)
- [Search on logs with timestamp pruning](tutorial-hdfs-logs.md)


