---
title: Distributed search on AWS S3
sidebar_position: 1
---

In this guide, we will index about 40 million log entries (13 GB decompressed) on AWS S3 using an EC2 instance and launch a three-node distributed search cluster.

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

Before using Quickwit with an object storage, check out our [advice](../administration/cloud-env.md) for deploying on AWS S3 to avoid some bad surprises at the end of the month.

:::


## Install

First of all, let's create an EC2 instance, install a Quickwit binary, and [configure it](configure-aws-env.md) to let Quickwit access your S3 buckets. This instance will be used for indexing our dataset.

```bash
curl -L https://install.quickwit.io | sh
cd quickwit-v*/
```

## Create your index

```bash
# First, download the hdfs logs config from Quickwit repository.
curl -o hdfs_logs_index_config.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/hdfs-logs/index-config.yaml
```

The index config defines five fields: `timestamp`, `tenant_id`, `severity_text`, `body`, and one object field
for the nested values `resource.service` . It also sets the `default_search_fields`, the `tag_fields`, and the `timestamp_field`. The `timestamp_field` and `tag_fields` are used by Quickwit for [splits pruning](../design/architecture.md) at query time to boost search speed. Check out the [index config docs](../reference/index-config.md) for more details.

```yaml title="hdfs_logs_index_config.yaml"
version: 0

doc_mapping:
  field_mappings:
    - name: severity_text
      type: text
      tokenizer: raw
    - name: tenant_id
      type: u64
      fast: true
    - name: body
      type: text
      tokenizer: default
      record: position
    - name: resource
      type: object
      field_mappings:
        - name: service
          type: text
          tokenizer: raw
  tag_fields: [tenant_id]

indexing_settings:
  timestamp_field: timestamp

search_settings:
  default_search_fields: [severity_text, body]
```

Now let the EC2 instance know the S3 bucket path we will be working with. This will make running subsequent  command snippets easier.

```bash
export S3_PATH=s3://{path/to/bucket}/indexes
```

```bash
# Create Quickwit config file.
echo "version: 0
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
" > config.yaml
```

We can now create the index with the `create` subcommand.

```bash
./quickwit index create --index-config hdfs_logs_index_config.yaml --config config.yaml
```

:::note

This step can also be executed on your local machine. The `create` command creates the index locally and then uploads a json file `metastore.json` to your bucket at `s3://path-to-your-bucket/hdfs-logs/metastore.json`.

:::

## Index logs
The dataset is a compressed [ndjson file](https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz). Instead of downloading and indexing the data in separate steps, we will use pipes to send a decompressed stream to Quickwit directly.

```bash
curl https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz | gunzip | ./quickwit index ingest --index hdfs-logs --config ./config.yaml
```

:::note

4GB of RAM is enough to index this dataset; an instance like `t4g.medium` with 4GB and 2 vCPU indexed this dataset in 20 minutes.

This step can also be done on your local machine. The `ingest` subcommand generates locally [splits](../design/architecture.md) of 10 million documents and will upload them on your bucket. Concretely, each split is a bundle of index files and metadata files.

:::


You can check it's working by using `search` subcommand and look for `ERROR` in `serverity_text` field:
```bash
./quickwit index search --index hdfs-logs --config ./config.yaml --query "severity_text:ERROR"
```

Now that we have indexed the logs and can search from one instance, It's time to configure and start a search cluster.

## Start the cluster

Quickwit needs a port `rest_listen_port` for serving the HTTP rest API via TCP as well as maintaining the cluster formation via UDP. Also, it needs `{rest_listen_port} + 1` for gRPC communication between instances.

In AWS, you can create a security group to group these inbound rules. Check out the [network section](configure-aws-env.md) of our AWS setup guide.

To make things easier, let's create a security group that opens the TCP/UDP port range [7200-7300]. Next, create three EC2 instances using the previously created security group. Take note of each instance's public IP address.

Now ssh into the first EC2 instance, install Quickwit, and [configure the environment](configure-aws-env.md) to let Quickwit access the index S3 buckets.

```bash
curl -L https://install.quickwit.io | sh
cd quickwit-v*/
```

Let's create the node configuration file by first running this snippet. Make sure you fill in the correct values before running in the EC2 terminal session.

```bash
export S3_PATH=s3://{path/to/bucket}/indexes
export IP_NODE_1={first-ec2-instance-public-ip}
export IP_NODE_2={second-ec2-instance-public-ip}
export IP_NODE_3={third-ec2-instance-public-ip}
```

```bash
# configuration for our first node
echo "version: 0
node_id: searcher-1
listen_address: 0.0.0.0
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
" > config.yaml
```

Now let's launch a searcher node for this instance.

```bash
# Then start the http server search service.
./quickwit run --service searcher --config ./config.yaml
```

You will see in the terminal the confirmation that the instance has created a new cluster. Example of such a log:

```
INFO quickwit_cluster::cluster: Create new cluster. node_id="searcher-1" listen_addr=0.0.0.1:7200
```

Let's launch the second and third searcher nodes instance by repeating the same previous steps but each time with the respective configuration snippet.

```bash
# configuration for our second node
echo "version: 0
node_id: searcher-2
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
listen_address: 0.0.0.0
peer_seeds:
  - ${IP_NODE_1} # searcher-1
" > config.yaml
```

```bash
# configuration for our third node
echo "version: 0
node_id: searcher-3
listen_address: 0.0.0.0
peer_seeds:
  - ${IP_NODE_1} # searcher-1
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
" > config.yaml
```

You will see in the terminal the confirmation that the instance has joined the existing cluster. Example of such a log:

```
INFO quickwit_cluster::cluster: Joined. node_id="searcher-1" remote_host=Some(18.222.142.42:7100)
```

Now we can query one of our instance directly by issuing http requests to one of the nodes rest API endpoint.

```
curl -v "http://${IP_NODE_2}:7280/api/v1/hdfs-logs/search?query=severity_text:ERROR"
```

## Load balancing incoming requests

Now that you have a search cluster, ideally, you will want to load balance external requests. This can quickly be done by adding an AWS load balancer to listen to incoming HTTP or HTTPS traffic and forward it to a target group.
You can now play with your cluster, kill processes randomly, add/remove new instances, and keep calm.


## Use time pruning

Let's execute a simple query that returns only `ERROR` entries on field `severity_text`:

```bash
curl -v 'http://your-load-balancer/api/v1/hdfs-logs/search?query=severity_text:ERROR
```

which returns the json

```json
{
  "num_hits": 364,
  "hits": [
    {
      "attributes.class": [
        "org.apache.hadoop.hdfs.server.datanode.DataNode"
      ],
      "body": [
        "RECEIVED SIGNAL 15: SIGTERM"
      ],
      "resource.service": [
        "datanode/02"
      ],
      "severity_text": [
        "ERROR"
      ],
      "timestamp": [
        1442629246
      ]
    }
    ...
  ],
  "elapsed_time_micros": 505923
}
```

You can see that this query has only 364 hits and that the server responds in 0.5 seconds.

The index config shows that we can use the timestamp field parameters `startTimestamp` and `endTimestamp` and benefit from time pruning. Behind the scenes, Quickwit will only query [splits](../design/architecture.md) that have logs in this time range. This can have a significant impact on speed.


```bash
curl -v 'http://your-load-balancer/api/v1/hdfs_logs/search?query=severity_text:ERROR&startTimestamp=1442834249&endTimestamp=1442900000'
```

Returns 6 hits in 0.36 seconds.


## Clean

Let's do some cleanup by deleting the index:

```bash
./quickwit index delete --index hdfs-logs  --config ./config.yaml
```

Also remember to remove the security group to protect your EC2 instances. You can just remove the instances if you don't need them.

Congratz! You finished this tutorial!

To continue your Quickwit journey, check out the [search REST API reference](../reference/rest-api.md) or the [query language reference](../reference/query-language.md).
