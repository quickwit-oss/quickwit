---
title: Distributed search on AWS S3
description: Index log entries on AWS S3 using an EC2 instance and launch a distributed cluster.
tags: [aws, integration]
icon_url: /img/tutorials/aws-logo.png
sidebar_position: 6
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

In this guide, we will index about 20 million log entries (7 GB decompressed) on AWS S3 using an EC2 instance and launch a three-node distributed search cluster.

Example of a log entry:
```json
{
  "timestamp": 1460530013,
  "severity_text": "INFO",
  "body": "PacketResponder: BP-108841162-10.10.34.11-1440074360971:blk_1074072698_331874, type=HAS_DOWNSTREAM_IN_PIPELINE terminating",
  "resource": {
    "service": "datanode/01"
  },
  "attributes": {
    "class": "org.apache.hadoop.hdfs.server.datanode.DataNode"
  }
}
```

:::caution

Before using Quickwit with an object storage, check out our [advice](../../operating/aws-costs) for deploying on AWS S3 to avoid some bad surprises at the end of the month.

:::

First of all, let's create an EC2 instance, install a Quickwit binary, and [configure it](../../guides/aws-setup) to let Quickwit access your S3 buckets. This instance will be used for indexing our dataset (note that you can also index your dataset from your local machine if it has the rights to read/write on AWS S3).

## Install

```bash
curl -L https://raw.githubusercontent.com/quickwit-oss/quickwit/main/install.sh | sh
cd quickwit-v*/
```

## Configure Quickwit with S3

Let's define the S3 path where we want to store our indexes.

```bash
export S3_PATH=s3://{path/to/bucket}/indexes
```

:::note
Quickwit needs access to the bucket and the objects it contains. On EC2, attach an IAM role through an instance
profile. Quickwit picks up those credentials automatically. The role needs `s3:ListBucket` on the bucket, plus
`s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListMultipartUploadParts`, and `s3:AbortMultipartUpload` on
its objects.

For more information, check out [our AWS setup guide](../../guides/aws-setup).
:::

Now we can create a Quickwit config file.

```bash
# Create Quickwit config file.
echo "version: 0.8
node_id: searcher-1
listen_address: 0.0.0.0
enabled_services:
  - metastore
  - control_plane
  - searcher
  - indexer
  - janitor
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
" > config.yaml
```

> You can also pass environment variables directly:
> ```yaml
> # config.yaml
> node_id: searcher-1
> listen_address: 0.0.0.0
> version: 0.8
> metastore_uri: ${S3_PATH}
> default_index_root_uri: ${S3_PATH}
>```

`enabled_services` controls which services run on each node. Here `searcher-1` runs the five services listed above,
while nodes 2 and 3 run only the searcher.

We are now ready to start Quickwit.

```bash
./quickwit run --config config.yaml
```

:::note

`quickwit run` stays in the foreground. Keep it running and open a second connection to the same instance for the
index and ingest commands that follow. To keep access to the node logs after disconnecting, run it under `tmux` or
redirect its output with `nohup`.

:::

## Create your index

```bash
# First, download the hdfs logs config from Quickwit repository.
curl -o hdfs_logs_index_config.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/v0.9.0/config/tutorials/hdfs-logs/index-config.yaml
```

The index config defines five fields: `timestamp`, `tenant_id`, `severity_text`, `body`, and one JSON field
for the nested values `resource.service`, we could use an object field here and maintain a fixed schema, but for convenience we're going to use a JSON field.
It also sets the `default_search_fields`, the `tag_fields`, and the `timestamp_field`. The `timestamp_field` and `tag_fields` are
used by Quickwit for [splits pruning](../../overview/architecture) at query time to boost search speed. 
Check out the [index config docs](../../configuration/index-config) for more details.

```yaml title="hdfs_logs_index_config.yaml"
version: 0.8

index_id: hdfs-logs

doc_mapping:
  field_mappings:
    - name: timestamp
      type: datetime
      input_formats:
        - unix_timestamp
      output_format: unix_timestamp_secs
      fast_precision: seconds
      fast: true
    - name: tenant_id
      type: u64
    - name: severity_text
      type: text
      tokenizer: raw
    - name: body
      type: text
      tokenizer: default
      record: position
    - name: resource
      type: json
      tokenizer: raw
  tag_fields: [tenant_id]
  timestamp_field: timestamp

search_settings:
  default_search_fields: [severity_text, body]
```

We can now create the index with the `create` subcommand.

```bash
./quickwit index create --index-config hdfs_logs_index_config.yaml
```

:::note

The `create` command sends the index configuration to the running Quickwit node. The node stores the file-backed
metastore at `s3://path-to-your-bucket/indexes/hdfs-logs/metastore.json`, under the prefix set in `S3_PATH`.

To run this command from your local machine, add `--endpoint http://<quickwit-host>:7280`.

:::

## Index logs
The dataset is a compressed [NDJSON file](https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz). 
Instead of downloading and indexing the data in separate steps, we will use pipes to send a decompressed stream to Quickwit directly.

```bash
wget https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz
gunzip -c hdfs-logs-multitenants.json.gz | ./quickwit index ingest --index hdfs-logs --force
```

:::note

8GB of RAM is enough to index this dataset; an instance like `t4g.large` with 8GB and 2 vCPU indexed this dataset in less than 10 minutes 
(provided that you have some CPU credits).

The `ingest` subcommand streams documents to the running Quickwit node through the ingest API. The indexer creates
[splits](../../overview/architecture), uploads them to your bucket, and merges them according to the index merge policy.

To run this command from your local machine, add `--endpoint http://<quickwit-host>:7280`.

:::


:::note

If ingestion fails after committing some documents, retrying appends to those documents, so the search below may
report more than 345 hits. Reset the index before retrying:

```bash
./quickwit index clear --index hdfs-logs
```

This permanently deletes all indexed data for `hdfs-logs`.

:::

You can check it's working by using `search` subcommand and look for `ERROR` in `severity_text` field:

<Tabs>

<TabItem value="cli" label="CLI">

```bash
./quickwit index search --index hdfs-logs --query "severity_text:ERROR"
```

</TabItem>

<TabItem value="curl" label="cURL">

```bash
curl "http://127.0.0.1:7280/api/v1/hdfs-logs/search?query=severity_text:ERROR"
```

</TabItem>

</Tabs>

which returns the json

```json
{
  "num_hits": 345,
  "hits": [
    {
      "attributes": {
        "class": "org.apache.hadoop.hdfs.server.datanode.DataNode"
      },
      "body": "RECEIVED SIGNAL 15: SIGTERM",
      "resource": {
        "service": "datanode/16"
      },
      "severity_text": "ERROR",
      "tenant_id": 51,
      "timestamp": 1469687755
    },
    ...
  ],
  "elapsed_time_micros": 522542
}
```

You can see that this query has 345 hits. In this case for the first run, the server responded in 523 milliseconds.
Subsequent runs use the cached metastore and can be resolved in under 100 milliseconds.

Now that we have indexed the logs and can search from one instance, it's time to configure and start two other instances to form a cluster.

## Start two more instances

Quickwit needs a port `rest.listen_port` for serving the HTTP rest API via TCP as well as maintaining the cluster formation via UDP. 
Also, it needs `{rest.listen_port} + 1` for gRPC communication between instances.

In AWS, you can create a security group to group these inbound rules. Check out the [network section](../../guides/aws-setup) of our AWS setup guide.

Create one security group for the Quickwit cluster. Set the security group itself as the source and allow TCP ports
7280 and 7281, plus UDP port 7280.

Attach this group to all three instances, including the `searcher-1` instance you launched earlier. All three nodes
exchange cluster gossip over UDP port 7280 in both directions, and the searcher nodes communicate with each other
over gRPC on TCP port 7281, also in both directions. Each instance must therefore accept inbound traffic from the
other instances in the security group. Cluster communication can fail if an instance does not have this group
attached.

Next, create two additional EC2 instances using this security group, and note the private IP address of `searcher-1`.

Connect to the second and third EC2 instances, install Quickwit, and
[configure AWS access](../../guides/aws-setup) for the index bucket.

```bash
curl -L https://raw.githubusercontent.com/quickwit-oss/quickwit/main/install.sh | sh
cd quickwit-v*/
```

And configure the environment so instances can form a cluster:

```bash
export S3_PATH=s3://{path/to/bucket}/indexes
export IP_NODE_1={first-ec2-instance-private-ip}
```

:::note

Because all three nodes are in the same VPC, set `peer_seeds` to the private IP address of `searcher-1`. In this setup
Quickwit advertises each node's private address, and traffic sent to a public IP does not match the self-referencing
security group rule above.

:::

```bash
# configuration for our second node
echo "version: 0.8
node_id: searcher-2
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
listen_address: 0.0.0.0
enabled_services:
  - searcher
peer_seeds:
  - ${IP_NODE_1} # searcher-1
" > config.yaml

# Start a Quickwit searcher.
./quickwit run --config config.yaml
```

```bash
# configuration for our third node
echo "version: 0.8
node_id: searcher-3
listen_address: 0.0.0.0
enabled_services:
  - searcher
peer_seeds:
  - ${IP_NODE_1} # searcher-1
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
" > config.yaml

# Start a Quickwit searcher.
./quickwit run --config config.yaml
```


Each searcher logs that it is joining the cluster. This line is emitted before any peer has been contacted, so on its
own it does not prove that the cluster formed. Example of such a log:

```
2023-03-19T16:44:56.918Z  INFO quickwit_cluster::cluster: Joining cluster. cluster_id=quickwit-default-cluster node_id=searcher-2 enabled_services={Searcher} gossip_listen_addr=0.0.0.0:7280 gossip_advertise_addr=172.31.30.168:7280 grpc_advertise_addr=172.31.30.168:7281 peer_seed_addrs=172.31.91.203:7280
```

Now we can query one of our instance directly by issuing http requests to one of the nodes rest API endpoint.

```
curl -v "http://127.0.0.1:7280/api/v1/hdfs-logs/search?query=severity_text:ERROR"
```

To confirm that the cluster actually formed, list its members:

```bash
curl -s http://127.0.0.1:7280/api/v1/cluster
```

The `ready_nodes` array should list `searcher-1`, `searcher-2`, and `searcher-3`.

## Load balancing incoming requests

Now that you have a search cluster, ideally, you will want to load balance external requests. 
This can quickly be done by adding an AWS load balancer to listen to incoming HTTP or HTTPS traffic and forward it to a target group.
You can now add or remove search-only instances and continue querying the cluster. This tutorial runs the metastore and
control plane only on `searcher-1`; keep that node running while testing searcher failures.

## Clean

Let's do some cleanup by deleting the index:

```bash
./quickwit index delete --index hdfs-logs
```

Then terminate all three EC2 instances and delete the cluster security group. Stopping the instances continues to
incur EBS charges, and when you terminate them, delete any EBS volumes that are not set to delete on termination. The
`index delete` command removes only the `hdfs-logs` index, so if the bucket is dedicated to this tutorial, empty and
delete it to stop its storage charges.

Congratulations! You finished this tutorial!

To continue your Quickwit journey, check out the [search REST API reference](../../reference/rest-api) or the [query language reference](../../reference/query-language).
