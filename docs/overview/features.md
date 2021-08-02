---
title: Features
sidebar_position: 1
---

Here is Quickwit featureset:

- Simple CLI to manage your index and setup a distributed search cluster
- Local and remote (S3-like storage) indexes
- Stateless instances, add or remove instances without moving your data
- Subsecond search on an object storage or any storage which provides bytes range queries
- Full text search, including phrase query
- Native support for time partitionning
- Boolean queries: `(michael AND jackson) OR "king of pop"`
- Mapping feature to define your schema and easily convert a json into a document to index
- Support data types text, i64, f64, date, bytes and composite types object and array.


## Quickwit v0.1 limitations
- no support for file formats other than [ndjson](http://ndjson.org/)
- no support for object storages not compatible with Amazon S3
- no aggregations nor faceted search
- no deletions (append mode only). One can only delete splits.
- no split merging.
- no indexing server
- no concurrent indexing on the same index, as the object storage metastore would not allow it.

Influence our roadmap by voting in our [GitHub issues](https://github.com/quickwit-inc/quickwit/issues) for the features that you need the most!
