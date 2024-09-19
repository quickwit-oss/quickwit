# Queue source design

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

## Onboarding new queues

This module is meant to be generic enough to:
- use other queue implementations, such as GCP Pub/Sub
- source the data from other sources than object storage, e.g directly from the message

Note that because every single messages is tracked by the metastore, this design will not behave well with high message rates. For instance it is not meant to be efficient with a data stream where every message contains a single event. As a rule of thumb, to protect the metastore, it is discouraged to try processing more than 50 messages per second with this design. This means that high throughput can only be achieved with larger contents for each message (e.g larger files when the using the file source with queue notifications).

## Implementation

The `QueueCoordinator` is a concrete implementation of the machinery necessary to consume data from a queue, from the message reception to its acknowledgment after indexing. The `QueueCoordinator` interacts with 3 main components.

### The `Queue`

The `Queue` is an abstract interface that can represent any queue implementation (AWS SQS, Google Pub/Sub...). It is sufficient that the queue guaranties at least one delivery of its messages. The abstraction reduces the actual queue's API surface to 3 main functions:
- receive messages that are ready to be processed
- extend their visibility timeout, i.e delay the time at which a message is visible again to other consumers
- acknowledge messages, i.e delete them definitively from the queue after successful indexing

### The `QueueLocalState`

The local state is an in memory data structure that keeps track of the knowledge that the current source has of recently received messages. It manages the transitions of messages between 4 states:
- ready for read
- read in progress
- awaiting commit
- completed


### The `QueueSharedState`

The shared state is a client of the Shard API, a metastore API that was mainly designed to serve ingest V2. The Shard API improves on the previous checkpoint API which was stored as a blob in one of the fields of the index model. The flow is the following one:

The queue source opens a shard, using an ID that uniquely identifies the content of the message as shard ID. For the file source, the shard ID is the file URI. Each source has a unique publish token that is provided in the `OpenShards` metastore request. The response of the `OpenShards` requests returns the token of the caller that called the API first. Either:
- The returned token matches the current pipeline's token. This means that we have the ownership of this message content and can proceed with its indexing
- The returned token does not match the current pipeline's token. This means that another pipeline has the ownership. In that case, we look at the content of the shard:
  - if it's already completely processed (EOF), we acknowledge the message drop it
  - if its last update timestamp is old (e.g twice the commit timeout), we assume the processing of the content to be stale (e.g the owning pipeline failed). We perform an `AcquireShards` call to update the shard's token in the metastore with the local one. This indicates subsequent attempts to process the shard that this pipeline now has its ownership. Note though that this is subject to a race conditions: 2 pipelines might acquire the shard concurrently. In that case both pipelines will assume that it owns the shard, and one of them will fail at commit time.
  - if its last update timestamp is recent, we assume that the processing of the content is still in progress in another pipeline. We just drop the message (without any acknowledgment) and let it be re-processed once its visibility timeout expires.

The `QueueSharedState` also owns the background task that will periodically initiate a call to `PruneShards` to garbage collect old shards.
