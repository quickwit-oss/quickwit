---
title: Features
sidebar_position: 1
---

Quickwit is still a young search engine but it already offers several features, here are the main ones:


- Full text search with phrase query and configurable tokenizers
- Natural query language: `(michael AND jackson) OR "king of pop")`
- Native support for time partitionning 
- Local and remote (S3-like storage) indexes
- Distributed search with stateless instances
- Fast search on object storage or any storage which provides bytes range queries
- Fast startup time, indexing and search
- Mapping feature to define your schema and easily convert a json into a document to index 
- Support data types text, i64, f64, date and composite types object and array.


You can have a look at [tantivy features](https://github.com/tantivy-search/tantivy/) to dig into the indexing engine.


## Current limitations
- no support for file formats other than [ndjson](http://ndjson.org/)
- no support for object storages not compatible with Amazon S3
- no faceted search
- no deletions (append mode only)


Influence our roadmap by voting in our [GitHub issues](https://github.com/quickwit-inc/quickwit/issues) for the features that you need the most!
