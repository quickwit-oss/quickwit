# Quickwit gRPC clients

Crate that implements the basics for Quickwit gRPC clients:
- A balance channel that updates its endpoints from cluster member changes.
- (soon) A client pool that updates its clients from cluster member changes.
- The gRPC Control Plane client. If we implement this client directly in the control plane crate, we would have the cyclic dependency `indexing-service -> metastore (uses the Control Plane Client) -> control-plane (uses the indexing gRPC client) -> indexing-service (implements the indexing gRPC client)`.
