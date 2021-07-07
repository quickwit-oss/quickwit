---
title: Quickstart
sidebar_position: 1
---

Before running quickwit search instances on your servers, you will need to create indexes, add documents or even delete some data and finally launch the server. To ease these actions, we provide a [CLI](../quickwit-cli.md) and that's 
all you need to quick start.


## Install Quickwit CLI

Let's download and install the Quiwkit CLI.

```
curl https://install-cli.quickwit.io | sh
```

Once installed, check it's working.

```
quickwit-cli --version
```

You can also install the CLI via [other means](installation.md).

## Create your first index

Before adding documents to Quickwit, you need to create an index along with a `doc mapper` which defines how a document and fields it contains, are stored and indexed.

Let's create an index with a mapper for wikipedia articles on you local machine.

```
# First download the wikipedia mapper from quickwit repository
curl -o wikipedia_doc_mapper.json https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/doc_mappers/wikipedia_doc_mapper.json
```

The doc mapper defines three text fields: `title`, `body` and `url` and set two default search fields `body` and `title`, it means that a text search will by default search into these two fields. Please note that by default text field are indexed and tokenized. See the [doc mapper documentation](../reference/doc-mapper.md).

And here is the complete doc mapper config:

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
            "type": "text"
        }
    ]
}
```

Now we can create the index:

```
quickwit-cli new --index-uri file:///your-path-to-your-index/wikipedia --doc-mapper-config-path ./wikipedia_doc_mapper.json
```


The following commands assume bash, which will replace pwd with the current directory. If you use another shell you need to adjust the replacement (remove $ in fish shell) or replace the uri with an absolute path.

E.g. to create it in the current directory with bash, you can use
```
quickwit-cli new --index-uri file://$(pwd)/wikipedia --doc-mapper-config-path ./wikipedia_doc_mapper.json
```

Check that an empty directory `/your-path-to-your-index/wikipedia` has been created, Quickwit will write index files here and a `quickwit.json` which contains the [index metadata](../overview/architecture.md#index-metadata).
You're now ready to fill the index.

## Let's add some documents

Currently `quickwit-cli` can index new line delimited json [ndjson](http://ndjson.org/) datasets.
Let's download [a bunch (10 000) of wikipedia articles](http://fulmicoton.com/tantivy-files/wiki-articles-1000.json) in [ndjson](http://ndjson.org/) format and index it.

```
# Download the first 1000 wikipedia articles in ndjson format.
curl -o -L wiki-articles-1000.json http://fulmicoton.com/tantivy-files/wiki-articles-1000.json
quickwit-cli index --index-uri file:///$(pwd)/wikipedia --input-path wiki-articles-1000.json
```

Wait a few seconds and check if it worked by using `search` command:

```
quickwit-cli search --index-uri file:///$(pwd)/wikipedia --query "barack AND obama"
```

It should return 10 hits. Now you're ready to serve.


## Start server

The command `serve` starts an http server which provides a [REST API](../reference/search-api.md). You can start several instances and provide peer socket addresses to form a cluster on which search workload will be distributed.

```
quickwit-cli serve --index-uris file:///$(pwd)/wikipedia
```

Check it's working with a simple GET request in the browser:
```
http://0.0.0.0:8080/api/v1/wikipedia/search?query=barack+AND+obama
```


## Clean

Let's do some cleanup by deleting the index:

```
quickwit-cli delete --index-uri file:///$(pwd)/wikipedia
```

Congrats! You can level up with some nice tutorials to discover all Quickwit features. 

## TLDR

```
curl -o wikipedia_doc_mapper.json https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/doc_mappers/wikipedia_doc_mapper.json
quickwit-cli new --index-uri file://$(pwd)/wikipedia --doc-mapper-config-path ./wikipedia_doc_mapper.json
curl -o -L wiki-articles-1000.json http://fulmicoton.com/tantivy-files/wiki-articles-1000.json
quickwit-cli index --index-uri file:///$(pwd)/wikipedia --input-path wiki-articles-1000.json
quickwit-cli search --index-uri file:///$(pwd)/wikipedia --query "barack AND obama"
```


## Next tutorials

- [Setup a distributed search on AWS S3](tutorial-distributed-search-aws-s3.md)
- [Search on logs with timestamp pruning](tutorial-hdfs-logs.md)


