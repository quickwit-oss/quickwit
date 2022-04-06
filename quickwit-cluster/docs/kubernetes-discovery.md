# Bootstrapping Quickwit clusters in a Kubernetes environment

- Author: Xavier Vello (@xvello)
- Date: 2022/04/06
- Pull request: https://github.com/quickwit-oss/quickwit/pull/1260/files

## Context and current state

Quickwit v0.3 is introducing a new cluster membership system, backed by the
[Scuttlebutt](https://github.com/quickwit-oss/scuttlebutt) library
(see [show and tell recording](https://www.youtube.com/watch?v=oN2zGosNsdE)).
In contrast to other clustering algorithm used to build a decision quorum, it uses a
[conflict-free replicated data type](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)
to propagate node status and metadata. The cluster state is then an input for:

- coordinating queries across searcher nodes
- providing an administration interface

For new nodes to join the cluster, they need to discover the others. They will do so by contacting at least
one "seed" node. That seed will share the current cluster state and advertise the new node to the other members.
In v0.3, the only way to specify seeds is through a list of fixed IP addresses (the `peer_seeds` config option).

Containerized environments such as Kubernetes do not guarantee IP address stability ; as containers can be automatically
moved across nodes, scaled up and down, or restarted for updates. During these operations, it is common for
container IP addresses to change. Operating in this context will require changes to the `quickwit-cluster`
and `scuttlebutt` crates, discussed in this document.

## Target and constraints

We want to enable users to easily and reliably setup one or several Quickwit clusters in a Kubernetes environment:

- Configuring and applying a [Helm](https://helm.sh) chart should result in a healthy cluster, without additional user
intervention
- Scaling up and down the indexer and searcher pools should be safe and reliable, with new nodes joining the
clusters and old nodes gracefully leaving it
- A mass-restart of the cluster (due to a software bug or a human error) should result in the cluster
converging back to a healthy state, without any thundering herd problems impacting the cluster or external services

There are risks we need to take into account and mitigate in our solution:

### Risk 1: Split brain

A split brain situation happens when a single pool is split into two (or more) different clusters operating
independently. This situation can happen if the discovery mechanism does not expose enough seeds to ensure
that all members are able to discover the full pool.

A pathological case of split brain would be a discovery mechanism only exposing a single random seed. If a node
happens to be given itself as a seed, it would operate in isolation and fail to join the other nodes. This scenario
can happen if the configured seed is a simple DNS or network-level load-balancer (regular Kubernetes service,
or network load-balancer for example).

### Risk 2: Unintended cluster merges on IP reuse

We want to enable users to partition their system by running several Quickwit clusters in the same Kubernetes cluster.
Use cases are multiple:

- reduce the blast radius of incidents caused by load spikes / updates / configuration changes
- use Quickwit for several services / teams while ensuring operational isolation
- mitigate scaling issues in the cluster membership system, if any were to come up in the future

When Kubernetes pods are restarted, they are assigned a new IP and their old IP is reused by future pods.
If an IP used by a member of `clusterA` were to be reused by a member of `clusterB`, the cluster state of
both clusters could merge, leading to an incident.

### Risk 3: Unbounded cluster state size leading to degraded performance

Operating large clusters will lead to a significant churn of cluster members over time:

- any configuration change or image update will result in every pod being progressively deleted and re-created,
potentially changing their `node_id` and IP
- both the indexer and searcher processes are good candidates for auto-scaling based on load / traffic. Auto-scaling
would also generate a lot of member churn every day

The Scuttlebutt crate does not currently remove old nodes from the cluster state. Without a garbage-collection
mechanism, this could degrade the performance of the cluster membership system.

## Proposed solution

The following action items are ordered by priority, and could be independently implemented in several releases:

**[TODO]**

## Alternative solutions

**[TODO]**

## Relevant links:

- [Github issue #1238](https://github.com/quickwit-oss/quickwit/issues/1238)
- [Quickwit architecture overview](https://quickwit.io/docs/current/design/architecture/)