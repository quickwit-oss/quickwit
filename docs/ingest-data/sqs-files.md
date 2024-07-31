---
title: S3 with SQS notifications
description: A short tutorial describing how to set up Quickwit to ingest data from S3 files using an SQS notifier
tags: [s3, sqs, integration]
icon_url: /img/tutorials/file-ndjson.svg
sidebar_position: 5
---

In this tutorial, we describe how to set up Quickwit to ingest data from S3
with bucket notification events flowing through SQS. We will first create the
AWS resources (S3 bucket, SQS queue, notifications) using terraform. We will
then configure the Quickwit index and file source. Finally we will send some
data to the source bucket and verify that it gets indexed.

## AWS resources

The complete terraform script can be downloaded [here](../assets/sqs-file-source.tf).

First, create the bucket that will receive the source data files (NDJSON format):

```
resource "aws_s3_bucket" "file_source" {
  bucket_prefix = "qw-tuto-source-bucket"
}
```

Then setup the SQS queue that will carry the notifications when files are added
to the bucket. The queue is configured with a policy that allows the source
bucket to write the S3 notification messages to it. Also create a dead letter
queue (DLQ) to receive the messages that couldn't be processed by the file
source (e.g corrupted files). Messages are moved to the DLQ after 5 indexing
attempts. 

```
locals {
  sqs_notification_queue_name = "qw-tuto-s3-event-notifications"
}

data "aws_iam_policy_document" "sqs_notification" {
  statement {
    effect = "Allow"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions   = ["sqs:SendMessage"]
    resources = ["arn:aws:sqs:*:*:${local.sqs_notification_queue_name}"]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.file_source.arn]
    }
  }
}

resource "aws_sqs_queue" "s3_events_deadletter" {
  name = "${locals.sqs_notification_queue_name}-deadletter"
}

resource "aws_sqs_queue" "s3_events" {
  name   = local.sqs_notification_queue_name
  policy = data.aws_iam_policy_document.sqs_notification.json

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.s3_events_deadletter.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "s3_events_deadletter" {
  queue_url = aws_sqs_queue.s3_events_deadletter.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue",
    sourceQueueArns   = [aws_sqs_queue.s3_events.arn]
  })
}
```

Configure the bucket notification that writes messages to SQS each time a new
file is created in the source bucket:

```
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.file_source.id

  queue {
    queue_arn = aws_sqs_queue.s3_events.arn
    events    = ["s3:ObjectCreated:*"]
  }
}
```

:::note

Only events of type `s3:ObjectCreated:*` are supported. Other types (e.g.
`ObjectRemoved`) are discarded with an error log.

:::

The source needs to have access to both the notification queue and the source
bucket. The following policy document contains the minimum permissions required
by the source:

```
data "aws_iam_policy_document" "quickwit_node" {
  statement {
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueAttributes",
    ]
    resources = [aws_sqs_queue.s3_events.arn]
  }
  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.file_source.arn}/*"]
  }
}
```

Create the IAM user and credentials that will be used to
associate this policy to your local Quickwit instance:

```
resource "aws_iam_user" "quickwit_node" {
  name = "quickwit-filesource-tutorial"
  path = "/system/"
}

resource "aws_iam_user_policy" "quickwit_node" {
  name   = "quickwit-filesource-tutorial"
  user   = aws_iam_user.quickwit_node.name
  policy = data.aws_iam_policy_document.quickwit_node.json
}

resource "aws_iam_access_key" "quickwit_node" {
  user = aws_iam_user.quickwit_node.name
}
```


:::warning

We don't recommend using IAM user credentials for running Quickwit nodes in
production. This is just a simplified setup for the sake of the tutorial. When
running on EC2/ECS, attach the policy document to an IAM roles instead.

:::

Download the [complete terraform script](../assets/sqs-file-source.tf) and
deploy it using `terraform init` and `terraform apply`. After a successful
execution, the outputs required to configure Quickwit will be listed. You can
display the values of the sensitive outputs (key id and secret key) with:


```bash
terraform output quickwit_node_access_key_id
terraform output quickwit_node_secret_access_key
```

## Run Quickwit

[Install Quickwit locally](/docs/get-started/installation), then in your install
directory, run Quickwit with the necessary access rights by replacing the
`<quickwit_node_access_key_id>` and `<quickwit_node_secret_access_key>` with the
matching Terraform output values:

```bash
AWS_ACCESS_KEY_ID=<quickwit_node_access_key_id> \
AWS_SECRET_ACCESS_KEY=<quickwit_node_secret_access_key> \
AWS_REGION=us-east-1 \
./quickwit run
```

## Configure the index and the source

In another terminal, in the Quickwit install directory, create an index:

```bash
cat << EOF > tutorial-sqs-file-index.yaml
version: 0.7
index_id: tutorial-sqs-file
doc_mapping:
  mode: dynamic
indexing_settings:
  commit_timeout_secs: 30
EOF

./quickwit index create --index-config tutorial-sqs-file-index.yaml
```

Replacing `<notification_queue_url>` with the corresponding Terraform output
value, create a file source for that index:

```bash
cat << EOF > tutorial-sqs-file-source.yaml
version: 0.8
source_id: sqs-filesource
source_type: file
num_pipelines: 2
params:
  notifications:
    - type: sqs
      queue_url: <notification_queue_url>
      message_type: s3_notification
EOF

./quickwit source create --index tutorial-sqs-file --source-config tutorial-sqs-file-source.yaml
```

:::tip

The `num_pipeline` configuration controls how many consumers will poll from the queue in parallel. Choose the number according to the indexer compute resources you want to dedicate to this source. As a rule of thumb, configure 1 pipeline for every 2 cores.

:::

## Ingest data

We can now ingest data into Quickwit by uploading files to S3. If you have the
AWS CLI installed, run the following command, replacing `<source_bucket_name>`
with the associated Terraform output:

```bash
curl https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants-10000.json | \
    aws s3 cp - s3://<source_bucket_name>/hdfs-logs-multitenants-10000.json
```

If you prefer not to use the AWS CLI, you can also download the file and upload
it manually to the source bucket using the AWS console.

Wait approximately 1 minute and the data should appear in the index:

```bash
./quickwit index describe --index tutorial-sqs-file
```

## Tear down the resources

The AWS resources instantiated in this tutorial don't incur any fixed costs, but
we still recommend deleting them when you are done. In the directory with the
Terraform script, run `terraform destroy`.
