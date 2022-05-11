---
title: Indexing
sidebar_position: 2
---

## Supported data formats

Quickwit ingests JSON records and refers to them as "documents" or "docs". Each document must be a JSON object. When ingesting files, documents must be separated by a newline.

As of version 0.2, Quickwit does not support file formats such as `Avro` or `CSV`. Compression formats such as `bzip2` or `gzip` are also not supported.

## Data model

As of version 0.2, Quickwit only supports indexes with a fixed schema. The "document mapping" of an index, also commonly called "doc mapping", is a list of field names and types that declares the schema of an index. Additionally, a doc mapping specifies how documents are indexed (tokenizers) and stored (column- vs. row-oriented).


## Merge process and merge policy

An index is broken into immutable splits. The size of a split is defined by the number of documents it carries. A split is considered "mature" when its size reaches a threshold defined in the index config as `split_num_docs_target`.

An indexer buffers incoming documents and produces a new split when the size of the buffer reaches `split_num_docs_target` or `commit_timeout_secs` seconds have passed since the first document has been enqueued, depending on which event occurs first. In the latter case, the indexer generates immature splits. The merge process designates the iterative procedure that groups and merges immature splits together to produce mature splits.

The merge policy controls the merge algorithm, which is mainly driven by the two parameters `split_num_docs_target` and `merge_factor`. Each time a new split is published, the merge policy examines the list of immature splits and attempts to merge `merge_factor` splits together in order to produce larger splits. The merge policy may also decide to merge fewer or more splits together if deemed necessary. Finally, the merge algorithm never merges more than `max_merge_factor` splits together.

### Split store

The split store is a cache that keeps recently published and immature splits on disk to speed up the merge process. After a successful merge phase, the split store evicts dangling splits.

The disk space allocated to the split store is controlled by the config parameters `split_store_max_num_splits` and `split_store_max_num_bytes`.

## Data sources

A data source designates the location and set of parameters that allow to connect to and ingest data from an external data store, which can be a file, a stream, or a database. Often, Quickwit simply refers to data sources as "sources". The indexing engine supports file-based and stream-based sources. Finally, Quickwit can insert data into an index from one or multiple sources, defined in the index config.


### File sources

File sources are sources that read data from a file stored on the local file system.

### Streaming sources

Streaming sources are sources that read data from a streaming service such as Apache Kafka. As of version 0.2, Quickwit only supports Apache Kafka. Future versions of Quickwit will support additional streaming services such as Amazon Kinesis.

## Checkpoint

Quickwit achieves exactly-once processing using checkpoints. For each source, a "source checkpoint" records up to which point documents have been processed in the target file or stream. Checkpoints are stored in the metastore and updated atomically each time a new split is published. When an indexing error occurs, the indexing process is resumed right after the last successfully published checkpoint. Internally, a source checkpoint is represented as an object mapping from absolute paths or partition IDs to offsets or sequence numbers.
