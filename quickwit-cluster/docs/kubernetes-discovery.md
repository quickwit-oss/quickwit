# Bootstrapping Quickwit clusters in a Kubernetes environment

- Author: Xavier Vello (@xvello)
- Date: 2022/04/06 - 2022/04/08
- Pull request: https://github.com/quickwit-oss/quickwit/pull/1260/files

## Context and current state

Quickwit v0.3 is introducing a new cluster membership system, backed by the
[scuttlebutt](https://github.com/quickwit-oss/scuttlebutt) crate
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

The `scuttlebutt` crate [handles this risk](https://github.com/quickwit-oss/scuttlebutt/pull/33) by garbage-collecting
node entries if they have not been seen for 24 hours. That 24h value cloud be made configurable for a faster
garbage collection, to support very high churn scenarios.

## Illustrative Quickwit example release topology

In [Helm terminology](https://helm.sh/docs/glossary/), a chart defines templates that the user will
configure and render into a release (a set of k8s entity manifests) and apply into one k8s cluster.
Applying several releases of the chart with different options, users can deploy several decoupled Quickwit clusters.
Resource names are usually prefixed with the configurable release name to avoid conflicts.

Defining the topology of the official Quickwit chart is out of scope for this document (especially choosing between
deployments and statefulsets), but we will consider this potential topology (inspired from Bitnami's
[Cassandra chart](https://github.com/bitnami/charts/tree/master/bitnami/cassandra/templates) and my experience)
as an illustration for discussions on the proposed solution:

- a `$release-indexing` deployment for an elastic pool of indexer nodes, that can be quickly auto-scaled based on volume
- a `$release-indexing` service, to provide a DNS endpoint for the push API. This service uses a label selector to 
  expose pods of the `$release-indexing` deployment once [they are ready](https://blog.colinbreck.com/kubernetes-liveness-and-readiness-probes-how-to-avoid-shooting-yourself-in-the-foot/)
- a `$release-search` statefulset, for a pool of searcher nodes. Statefulset would allow prioritizing disk caching
  (with persistent disk volumes) over elasticity (they are slower to up/down scale), if the read volume is steady
- a `$release-search` service, to provide a DNS endpoint for the search API. It uses a label selector to
  expose ready pods of the `$release-search` statefulset
- a `$release-headless` service, to be used for discovery (see next section). It will be set:
  - as [headless](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services), to allow
    enumerating all pods at the DNS level
  - with `publishNotReadyAddresses: true` to expose all pods, even before they are ready
  - with a label selector matching both the `$release-indexing` and `$release-search` pods, as a unified cluster

## Proposed solution

The following action items are ordered by priority, and could be independently implemented in several releases.
Alternatives are discussed in the next section, with a pros/cons comparison.

### 1a. Source the sibling pods as seeds via DNS enumeration on startup

A new `peer_seed_service` configuration option is added to Quickwit. It is exclusive to the existing `peer_seeds`,
and setting both will result on a validation error. `quickwit-cluster::start_cluster_service` is updated to check
this option. If it is not empty:

- a DNS enumeration request is issued, that will return the IP addresses of all the live pods in the cluster,
- if the DNS request fails or if the result is empty, Quickwit fails to start. Kubernetes will restart the
  container, allowing to retry the request when more pods are created. In case of repeated failures (misconfiguration,
  or DNS error), the CrashLoopBackoff mechanism will engage, reducing the pressure on the DNS server by keeping
  pods down longer between retries,
- if not empty, the response is used to populate the `seed_nodes` argument passed to `scuttlebutt`.

Due to the eventually consistent nature of the k8s control plane, there will be a delay before all pods are exposed
in the DNS record. We do have guarantees that the responses to the different pods will be consistent. Pods will
restart until one IP is registered, and the same first peer will be returned to all pods, avoiding a split brain.

When adding nodes to the cluster (by scaling up the deployment/statefulset), new nodes will retrieve
[up to 1000](https://kubernetes.io/docs/concepts/services-networking/service/#over-capacity-endpoints)
IPs as seeds by querying the service. Existing nodes will not update their seeds, but discover the new nodes
via the gossip messages.

If an existing node restarts (due to an out-of-memory error for example), it will request up-to-date seeds again
from the DNS servers, discarding its previous state. This is better than running the DNS enumeration in an
[init container](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/), which would not
update the seeds on container restart.

### 2. Gate cluster membership on new a `cluster_name` property

Redis, Cassandra and Elasticsearch all had to solve the IP reuse problem before. They all settled on the simplest and
most resilient solution: adding a `cluster_name` property to the nodes, and forbidding nodes to join if their
advertised cluster name does not match the other peers. This mechanism can be implemented in `scuttlebutt` with
the following changes:

- adding a new required argument `cluster_name` to the `ScuttleServer::spawn` function (String type)
- copying that value as a new  `cluster_name` field to the `Digest` object
- updating `ScuttleServer::process_message` to reject messages if it detects a mismatch between the `Digest`'s
cluster name and its own. A warning will be logged to alert the operators that nodes are misconfigured

Making the cluster name required will simplify the `scuttlebutt` code. To keep single-node deploys easy,
Quickwit can provide a default value if its `cluster_name` configuration option is not set. Cassandra uses
["Test Cluster"](https://cassandra.apache.org/doc/latest/cassandra/configuration/cass_yaml_file.html#cluster_name)
to nudge operators into setting a relevant value, even if they only have one cluster for now.

### 3. Use the pod name as default `node_id`

Pods are assigned a unique name by the system on creation. Reusing this pod name as the `node_id` will make it easier
for cluster operators to navigate between the Quickwit admin and their infrastructure monitoring. This name is available
in the `$POD_NAME` environment variable and can be injected in arbitrarily named environment variables using a 
[fieldRef](https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/).

The best practice for Kubernetes-native services is to allow overriding configuration values with envvar.
We will update `quickwit-config` to lookup the `QUICKWIT_NODE_ID` envvar, and use its value if not empty.

The conventional priority order is environment variables > command line flags > configuration file. If we want
to adhere to this convention, we should investigate existing crates implementing this mechanism, and migrate
`quickwit-config` to use it for all configuration values.

### 4. Introduce a readiness check and set initial cluster sync as a readiness condition

The good practice is for servers to expose liveness and readiness checks to the orchestrator:

- a failed liveness check indicates a non-recoverable invalid state, and will trigger a restart of the container
- a failed readiness check indicates a recoverable invalid state: the container will continue running, but will
  be excluded from the related service as it is not ready to serve requests yet

Defining the conditions for these checks is a delicate process, because incidents can be triggered if they
are too stringent. [This blog post](https://blog.colinbreck.com/kubernetes-liveness-and-readiness-probes-how-to-avoid-shooting-yourself-in-the-foot/)
illustrates some things that can go wrong.

If the cluster is intended to be more than one node, most operators would expect that the readiness check
only succeeds once the node has successfully connected with its peers and is able to correctly optimize
read and write requests.

## Alternative solutions

### 1b. Use the Kubernetes API to list sibling pods

The kube-apiserver component exposes API endpoints that can be used to list resources. We could issue 
[a list pods](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#list-pod-v1-core) request
with a `labelSelector` parameter matching the cluster's release name to retrieve the IPs of all running pods
in the cluster (be them in a deployment of a statefulset). 

The `watch` parameter allows to keep the connection open and subscribe to deltas (additions and removals)
with a low latency. It could be used to remove old nodes from the seeds, but `scuttlebutt` already implements
a node garbage-collection, that would conflict with this mechanism.

I do not recommend this solution as access to these endpoints can be restricted by cluster admins, because:

- the load on the apiservers (and their backing etcd store) is usually the limit for cluster size,
  adding load to these servers when alternative solutions exist can be frowned upon
- these endpoints expose a lot of information about the infrastructure and could be used by intruders for
  lateral movement and privilege escalation

Also, the DNS solution could be used in other containerized environments (Nomad/Consul or Mesos for example),
which we might have to support at some point in the future.

### 1c. Use a stable "seed" pool

I have seen distributed systems use a set of three idle machines as their seed pool. Their single job is to
provide stable seeds for the workers coming in and out of the cluster. Once they are up, workers can reach any
of them via a network-level load-balancer and reach a stable seed.

One topology option would be to run them in a dedicated statefulset, for them to have stable dns entries in the
form `$release-seeds-$ordinal`, that could be statically be set in the `peer_seeds` config option. Another would
be to use a headless service as described above.

I do not see any advantage for Quickwit at this time, but we could keep this solution in the back of our mind
for other environments.

## Relevant links:

- [Github issue #1238](https://github.com/quickwit-oss/quickwit/issues/1238)
- [Quickwit architecture overview](https://quickwit.io/docs/current/design/architecture/)
- [Kubernetes services documentation](https://kubernetes.io/docs/concepts/services-networking/service/)
- [Helm glossary](https://helm.sh/docs/glossary/)