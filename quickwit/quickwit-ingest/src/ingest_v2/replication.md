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

- When a replication request fails, the leader and follower close the shard(s) targetted by the request.

- When a replication stream fails (transport error, timeout), the leader and follower close the shard(s) targetted by the stream. Then, the leader reopens a new stream if necessary.
