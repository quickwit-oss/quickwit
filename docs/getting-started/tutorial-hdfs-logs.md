---
title: Tutorial - Index a logging dataset
sidebar_position: 4
---

Before running quickwit search instances on your servers, you will need to create indexes, add documents or even delete some data and finally launch the server. To ease these actions, we provide a [CLI](../quickwit-cli/README.md) and that's 
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

Before adding documents to Quickwit, you need to create an index along with a `mapper` which will define how a document and fields it contains, are stored and indexed.

Let's create an index with a mapper for wikipedia pages on you local machine. The mapper defines three text fields: `title`, `body` and `url`. 

```
# First download the wikipedia mapper from quickwit repository
curl https://path-to-wikipedia-mapper
quickwit-cli new file://./my-indexes/wikipedia --doc-mapper-config-path ./wikipedia_doc_mapper.json
```

Check that an empty directory `my-indexes/wikipedia` is created, Quickwit will write all files related to the index here.
You're now ready to fill the index.

## Let's add some documents

`quickwit-cli` can currently index [ndjson](http://ndjson.org/) datasets.
Let's download [a bunch of wikipedia articles]() and index it.

```
# Download the first 1000 wikipedia articles in ndjson format.
curl https://path-to-wikipedia-ndjson
quickwit-cli index --index-uri file://./my-indexes/wikipedia --input-path wikipedia.json
```

Wait a few seconds and you're ready to search these documents.


## Start server and search

You can search directly with the `search` command of the CLI but for this tutorial, we will start directly a server.

```
# You need to indcate the index-uri to start the server
quickwit-cli serve --index-uri file://./my-indexes/wikipedia
```


## Next steps

- [Search on AWS S3]()
- Index the full wikipedia index
- 


