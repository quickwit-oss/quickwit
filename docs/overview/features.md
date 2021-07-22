---
title: Features
sidebar_position: 1
---

Quickwit ships with the following features:

- simple CLI to manage your index and setup a distributed search cluster;
- local and remote (S3-like storage) indexes;
- stateless instances, add or remove instances without moving your data;
- subsecond search on an object storage or any storage providing byte range queries;
- full text search, including phrase query;
- native support for time partitionning;
- boolean queries: `(michael AND jackson) OR "king of pop")`;
- mapping feature to define your schema and easily convert JSON records into indexable documents;
- support data types text, i64, f64, date, bytes and composite types object and array.


## Quickwit v0.1 limitations

Quickwit v0.1 comes with the following limitations:

- no support for file formats other than [NDJSON](http://ndjson.org/);
- no support for object storages not compatible with Amazon S3;
- no aggregations nor faceted search;
- no deletions (append mode only). One can only delete splits;
- no split merging;
- no indexing server;
- no concurrent indexing on the same index, as the object storage metastore would not allow it.

Influence our roadmap by voting in our [GitHub issues](https://github.com/quickwit-inc/quickwit/issues) for the features that you need the most!
