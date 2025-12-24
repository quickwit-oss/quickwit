## Replication

### Sync replication
For each shard, leaders replicate the state of their local mrecordlog queues and associated metadata (positions) by sending replication requests to their followers. Then, they wait for followers to acknowledge the replication requests before returning success or failure responses to routers.

### Replication stream
Two gRPC streams back the independent streams of requests and responses between leader-follower pairs called the SYN replication stream and the ACK replication stream. gRPC streams guarantee that the streamed messages are delivered in the order they are sent. However, gRPC bidirectional streaming does not guarantee that requests and responses match. Most of the logic implemented in `replication.rs` aims to "zip" the two streams together to fix this issue.

### Life of a happy persist request
1. Leader receives a persist request pre-assigned to a shard from a router.

1. Leader forwards replicate request to follower of the shard via the SYN replication stream.

1. Follower receives the replicate request, writes the data to its replica queue, and records the new position of the queue called `replica_position`.

1. Follower returns replicate response to leader via the ACK replication stream.

1. Leader records the new position of the replica queue.

1. Leader writes the data to its local mrecordlog queue and records the new position of the queue called `primary_position`.  It should match the `replica_position`.

1. Leader return success persist response to router.

### Replication stream errors

- When a replication request fails, the leader and follower close the shard(s) targeted by the request.

- When a replication stream fails (transport error, timeout), the leader and follower close the shard(s) targeted by the stream. Then, the leader reopens a new stream if necessary.

## Ingester Status

Each Quickwit node, regardless of the enabled services on the node, has the notion liveness and readiness. Liveness is defined by chitchat's failure detector. Readiness is defined as live + gRPC server is up, REST server is up, metastore connectivity test succeeds, and ingester status is "ready" if the ingester service is enabled on the node (In other word if the node is an indexer). Once all those requirements are met, a node gossip it's readiness via chitchat and that's how other nodes in the cluster know that can route traffic to it. Finally, a node that is ready is also sometimes reffered to as "available".

For a node that carries the ingester role, being available is required but not sufficient, and that's why it has a dedicated status. On startup the ingester status is "initializating: while it's loading it's WAL. If it fails to do so, its status will changed to "failed" and the node will never become ready and eventually will fail its readiness probe, causing k8s to restart the pod (and most likely the node will go in a crash loop, because the WAL load failing most likely comes from a WAL corruption which cannot be recover without manual intervention). Once the ingester status is "ready", the node also becomes ready and the readiness probe should succeed leading k8s to route traffic to the pod and the ingester will start serving read and write requests.

## Decommissioning

An ingester starts the decommissioning routine upon receiving a decommission gRPC request or a SIGINT signal. Then, the ingester sets its ingester status to "Decommissioning" and closes all its shards. It stops serviring write requests (open shard and persist requests) but can still service read requests (fetch stream request). 
