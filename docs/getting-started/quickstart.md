---
title: Quickstart
sidebar_position: 1
---

Before running quickwit search instances on your servers, you will need to create indexes, add documents or even delete some data and finally launch the server. To ease these actions, you just need to download `Quickwit` binary, for full commands documentation, see the [Quickwit CLI page](../quickwit-cli.md).


## Install Quickwit

Let's download and install the Quiwkit.

```
curl -L https://install.quickwit.io | sh
```

Once installed, check it's working.

```
quickwit --version
```

You can also install it via [other means](installation.md).

## Create your first index

Before adding documents to Quickwit, you need to create an index along with the `index config` which notably defines how a document and fields it contains, are stored and indexed.

Let's create an index with configured for wikipedia articles on you local machine.

```
# First download the wikipedia config from quickwit repository
curl -o wikipedia_index_config.json https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/index_configs/wikipedia_index_config.json
```

The index config defines three text fields: `title`, `body` and `url` and set two default search fields `body` and `title`. Thes fields will be used for search if you do not specify fields in your query. Please note that by default, text fields are indexed and tokenized. See the [index config documentation](../reference/doc-mapper.md).

And here is the complete config:

```
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

```
quickwit new --index-uri file:///your-path-to-your-index/wikipedia --index-config-path ./wikipedia_index_config.json
```

or the following one if you can use `pwd` to create it in your current directory:

```
quickwit new --index-uri file:///$(pwd)/wikipedia --index-config-path ./wikipedia_index_config.json
```

Check that an empty directory `/your-path-to-your-index/wikipedia` has been created, Quickwit will write index files here and a `quickwit.json` which contains the [index metadata](../overview/architecture.md#index-metadata).
You're now ready to fill the index.


## Let's add some documents

Currently Quickwit can index new line delimited json [ndjson](http://ndjson.org/) datasets.
Let's download [a bunch of wikipedia articles (10 000)](https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json) in [ndjson](http://ndjson.org/) format and index it.

```
# Download the first 10000 wikipedia articles in ndjson format.
curl -o -L wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json
quickwit index --index-uri file:///$(pwd)/wikipedia --input-path wiki-articles-10000.json
```

Wait a few seconds and check if it worked by using `search` command:

```
quickwit search --index-uri file:///$(pwd)/wikipedia --query "barack AND obama"
```

It should return 10 hits. Now you're ready to serve.


## Start server

The command `serve` starts an http server which provides a [REST API](../reference/search-api.md). You can also start several instances and provide peer socket addresses to form a cluster on which search workload will be distributed.

```
quickwit serve --index-uris file:///$(pwd)/wikipedia
```

Check it's working with a simple GET request in the browser or via cURL:
```
http://0.0.0.0:8080/api/v1/wikipedia/search?query=barack+AND+obama
```


## Clean

Let's do some cleanup by deleting the index:

```
quickwit delete --index-uri file:///$(pwd)/wikipedia
```

Congrats! You can level up with some nice tutorials to discover all Quickwit features. 

## TLDR

```
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


