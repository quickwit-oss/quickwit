---
title: Search with AWS Lambda
description: Index and search using AWS Lambda on 20 million log entries
tags: [aws, integration]
icon_url: /img/tutorials/aws-logo.png
sidebar_position: 4
---

In this tutorial, we will index and search about 20 million log entries (7 GB decompressed) located on AWS S3 with Quickwit Lambda.

Concretely, we will deploy an AWS CloudFormation stack with the Quickwit Lambdas, and two buckets: one staging for hosting gzipped newline-delimited JSON files to be indexed and one for hosting the index data. The staging bucket is optional as Quickwit indexer can read data from any S3 files it has access to.

![Tutorial stack overview](../../assets/images/quickwit-lambda-service.svg)

## Install

### Install AWS CDK

We will use [AWS CDK](https://aws.amazon.com/cdk/) for our infrastructure automation script. Install it using [npm](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm):
```bash
npm install -g aws-cdk 

You also need AWS credentials to be properly configured in your shell. One way is using the [credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

Finally, clone the Quickwit repository:
```bash
git clone https://github.com/quickwit-oss/tutorials.git
cd tutorials/simple-lambda-stack
```

### Setup python environment

We use python 3.10 to define the AWS CloudFormation stack we need to deploy, and a python CLI to invoke Lambdas.
Let's install those few packages (boto3, aws-cdk-lib, click, pyyaml).

```bash
# Install pipenv if needed.
pip install --user pipenv
pipenv shell
pipenv install
```

### Download Quickwit Lambdas

```bash
mkdir -p cdk.out
wget -P cdk.out https://github.com/quickwit-oss/quickwit/releases/download/aws-lambda-beta-01/quickwit-lambda-indexer-beta-01-x86_64.zip
wget -P cdk.out https://github.com/quickwit-oss/quickwit/releases/download/aws-lambda-beta-01/quickwit-lambda-searcher-beta-01-x86_64.zip
```

### Bootstrap and deploy

Configure the AWS region and [account id](https://docs.aws.amazon.com/IAM/latest/UserGuide/console_account-alias.html) where you want to deploy the stack:

```bash
export CDK_ACCOUNT=123456789
export CDK_REGION=us-east-1
```

If this region/account pair was not bootstrapped by CDK yet, run:
```bash
cdk bootstrap aws://$CDK_ACCOUNT/$CDK_REGION
```

This initializes some basic resources to host artifacts such as Lambda packages.

## Index the HDFS logs dataset

Here is an example of a log entry of the dataset:
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
  },
  "tenant_id": 58
}
```

If you have a few minutes ahead of you, you can index the whole dataset which is available on our public S3 bucket.

```bash
python cli.py index s3://quickwit-datasets-public/hdfs-logs-multitenants.json.gz
```

If not, just index the 10,000 documents dataset:

```bash
python cli.py index s3://quickwit-datasets-public/hdfs-logs-multitenants-10000.json
```

## Execute search queries

Let's start with a query on the field `severity_text` and look for errors: `severity_text:ERROR`:

```bash
python cli.py search '{"query":"severity_text:ERROR"}'
```

It should respond under 1 second and return 10 hits out of 345 if you indexed the whole dataset. If you index the first 10,000 documents, you won't have any hits, try to query `INFO` logs instead.


Let's now run a more advanced query: a date histogram with a term aggregation on the `severity_text`` field:

```bash
python cli.py search '{ "query": "*", "max_hits": 0, "aggs": { "events": { "date_histogram": { "field": "timestamp", "fixed_interval": "30d" }, "aggs": { "log_level": { "terms": { "size": 10, "field": "severity_text", "order": { "_count": "desc" } } } } } } }'
```

It should respond under 2 seconds and return the top log levels per 30 days.


### Cleaning up

First, you have to delete the files created on your S3 buckets.
Once done, you can delete the stack. 

```bash
cdk destroy -a cdk/app.py
rm -rf cdk.out
```

Congratz! You finished this tutorial! You can level up with the following tutorials to discover all Quickwit features.

## Next steps

- [Advanced Lambda tutorial](tutorial-aws-lambda.md) which covers an end-to-end use cases
- [Search REST API](/docs/reference/rest-api)
- [Query language](/docs/reference/query-language)
