---
title: Distributed search on AWS S3
description: Index log entries on AWS S3 using an EC2 instance and launch a distributed cluster.
tags: [aws, integration]
icon_url: /img/tutorials/aws-logo.png
sidebar_position: 2
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

Before using Quickwit with an object storage, check out our [advice](../../operating/aws-costs) for deploying on AWS S3 to avoid some bad surprises at the end of the month.

:::

First of all, let's create an EC2 instance, install a Quickwit binary, and [configure it](../../guides/aws-setup) to let Quickwit access your S3 buckets. This instance will be used for indexing our dataset (note that you can also index your dataset from your local machine if it has the rights to read/write on AWS S3).

## Install

```bash
curl -L https://install.quickwit.io | sh
cd quickwit-v*/
```

## Configure Quickwit with S3

Let's define the S3 path where we want to store our indexes.

```bash
export S3_PATH=s3://{path/to/bucket}/indexes
```

:::note
You'll want to include the necessary authorization for the given bucket, this can be done by setting the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
environment variables, or via the AWS credentials file. Usually located at `~/.aws/credentials`.

For more info check out [our AWS setup guide](https://quickwit.io/docs/guides/aws-setup)
:::

Now we can create a Quickwit config file.

```bash
# Create Quickwit config file.
echo "version: 0.5
node_id: searcher-1
listen_address: 0.0.0.0
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
" > config.yaml
```

> You can also pass environment variables directly:
> ```yaml
> # config.yaml
> node_id: searcher-1
> listen_address: 0.0.0.0
> version: 0.5
> metastore_uri: ${S3_PATH}
> default_index_root_uri: ${S3_PATH}
>```

We are now ready to start Quickwit.

```bash
./quickwit run --config config.yaml
```

## Create your index

```bash
# First, download the hdfs logs config from Quickwit repository.
curl -o hdfs_logs_index_config.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/hdfs-logs/index-config.yaml
```

The index config defines five fields: `timestamp`, `tenant_id`, `severity_text`, `body`, and one JSON field
for the nested values `resource.service`, we could use an object field here and maintain a fixed schema, but for convenience we're going to use a JSON field.
It also sets the `default_search_fields`, the `tag_fields`, and the `timestamp_field`. The `timestamp_field` and `tag_fields` are
used by Quickwit for [splits pruning](../../overview/architecture) at query time to boost search speed. 
Check out the [index config docs](../../configuration/index-config) for more details.

```yaml title="hdfs_logs_index_config.yaml"
version: 0.5

index_id: hdfs-logs

doc_mapping:
  field_mappings:
    - name: timestamp
      type: datetime
      input_formats:
        - unix_timestamp
      output_format: unix_timestamp_secs
      precision: seconds
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

This step can also be executed on your local machine. The `create` command creates the index locally and then uploads a 
json file `metastore.json` to your bucket at `s3://path-to-your-bucket/hdfs-logs/metastore.json`.

:::

## Index logs
The dataset is a compressed [NDJSON file](https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz). 
Instead of downloading and indexing the data in separate steps, we will use pipes to send a decompressed stream to Quickwit directly.

```bash
wget https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz
gunzip -c hdfs-logs-multitenants.json.gz | ./quickwit index ingest --index hdfs-logs
```

:::note

4GB of RAM is enough to index this dataset; an instance like `t4g.medium` with 4GB and 2 vCPU indexed this dataset in 20 minutes.

This step can also be done on your local machine. 
The `ingest` subcommand generates locally [splits](../../overview/architecture) of 10 million documents and will upload 
them on your bucket. Concretely, each split is a bundle of index files and metadata files.

:::


You can check it's working by using `search` subcommand and look for `ERROR` in `severity_text` field:
```bash
./quickwit index search --index hdfs-logs --query "severity_text:ERROR"
```

Now that we have indexed the logs and can search from one instance, It's time to configure and start two other instances to form a cluster.

## Start two more instances

Quickwit needs a port `rest_listen_port` for serving the HTTP rest API via TCP as well as maintaining the cluster formation via UDP. 
Also, it needs `{rest_listen_port} + 1` for gRPC communication between instances.

In AWS, you can create a security group to group these inbound rules. Check out the [network section](../../guides/aws-setup) of our AWS setup guide.

To make things easier, let's create a security group that opens the TCP/UDP port range [7200-7300]. 
Next, create three EC2 instances using the previously created security group. Take note of each instance's public IP address.

Now ssh into the first EC2 instance, install Quickwit, and [configure the environment](../../guides/aws-setup) to let Quickwit access the index S3 buckets.

Let's install Quickwit on the second and third EC2 instances.

```bash
curl -L https://install.quickwit.io | sh
cd quickwit-v*/
```

And configure the environment so instances can form a cluster:

```bash
export S3_PATH=s3://{path/to/bucket}/indexes
export IP_NODE_1={first-ec2-instance-public-ip}
```

```bash
# configuration for our second node
echo "version: 0.5
node_id: searcher-2
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
listen_address: 0.0.0.0
peer_seeds:
  - ${IP_NODE_1} # searcher-1
" > config.yaml

# Start a Quickwit searcher.
./quickwit run --service searcher --config config.yaml
```

```bash
# configuration for our third node
echo "version: 0.5
node_id: searcher-3
listen_address: 0.0.0.0
peer_seeds:
  - ${IP_NODE_1} # searcher-1
metastore_uri: ${S3_PATH}
default_index_root_uri: ${S3_PATH}
" > config.yaml

# Start a Quickwit searcher.
./quickwit run --service searcher --config config.yaml
```


You will see in the terminal the confirmation that the instance has joined the existing cluster. Example of such a log:

```
2023-03-19T16:44:56.918Z  INFO quickwit_cluster::cluster: Joining cluster. cluster_id=quickwit-default-cluster node_id=searcher-2 enabled_services={Searcher} gossip_listen_addr=0.0.0.0:7280 gossip_advertise_addr=172.31.30.168:7280 grpc_advertise_addr=172.31.30.168:7281 peer_seed_addrs=172.31.91.203:7280
```

Now we can query one of our instance directly by issuing http requests to one of the nodes rest API endpoint.

```
curl -v "http://0.0.0.0:7280/api/v1/hdfs-logs/search?query=severity_text:ERROR"
```

Check out the logs of all instances and you will see that all nodes are working.

## Load balancing incoming requests

Now that you have a search cluster, ideally, you will want to load balance external requests. 
This can quickly be done by adding an AWS load balancer to listen to incoming HTTP or HTTPS traffic and forward it to a target group.
You can now play with your cluster, kill processes randomly, add/remove new instances, and keep calm.


## Use time pruning

Let's execute a simple query that returns only `ERROR` entries on field `severity_text`:

```bash
curl -v 'http://your-load-balancer/api/v1/hdfs-logs/search?query=severity_text:ERROR
```

which returns the json

```json
{
  "num_hits": 10000,
  "hits": [
    {
      "body": "Receiving BP-108841162-10.10.34.11-1440074360971:blk_1073836032_95208 src: /10.10.34.20:60300 dest: /10.10.34.13:50010",
      "resource": {
        "service": "datanode/03"
      },
      "severity_text": "INFO",
      "tenant_id": 58,
      "timestamp": 1440670490
    }
    ...
  ],
  "elapsed_time_micros": 2502
}
```

You can see that this query has 10 000 hits and that the server responds in 2.5 milliseconds.

The index config shows that we can use the timestamp field parameters `start_timestamp` and `end_timestamp` and benefit from time pruning. 
Behind the scenes, Quickwit will only query [splits](../../overview/architecture) that have logs in this time range. This can have a significant impact on speed.


```bash
curl -v 'http://your-load-balancer/api/v1/hdfs-logs/search?query=severity_text:ERROR&start_timestamp=1442834249&end_timestamp=1442900000'
```

Returns 6 hits in 0.36 seconds.


## Clean

Let's do some cleanup by deleting the index:

```bash
./quickwit index delete --index hdfs-logs
```

Also remember to remove the security group to protect your EC2 instances. You can just remove the instances if you don't need them.

Congratz! You finished this tutorial!

To continue your Quickwit journey, check out the [search REST API reference](/docs/reference/rest-api) or the [query language reference](/docs/reference/query-language).
