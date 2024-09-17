# Queue sources: high level design

## Exactly once

Besides the usual failures that can happen during indexing, most queues are also subject to duplicates. To ensure that all object files are indexed exactly once, we track the progress of their indexing using the shard table:
- each file object is tracked as a shard, the file URI is the shard ID
- progress made on the indexing of a given shard is committed in the shard table in a common transaction with the split publishing
- after some time (called deduplication window) shards are garbage collected to keep the size of the shard table small

## Visibility extension task

To keep messages invisible to other pipelines while they are being processed, each received message spawns a visibility extension task. This task is responsible of extending the visibility timeout each time the visibility deadlines approaches. When the last batch is read for the message and sent to the indexing pipeline:
- a last visibility extension is requested to give time for the indexing to complete (typically twice the commit timeout) 
- the visibility extension task stopped

## Cleanup of old shards

Garbage collection is owned by the queue based sources. Each pipeline with a queue source will spawn a garbage collection task. To avoid having an increased load on the metastore as the number of pipeline scales, garbage collection calls are debounced by the control plane.
