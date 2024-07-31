---
title: S3 files with SQS notifications
description: A short tutorial describing how to set up Quickwit to ingest data from S3 files using an SQS notifier
tags: [s3, sqs, integration]
icon_url: /img/tutorials/file-ndjson.svg
sidebar_position: 5
---

In this tutorial, we will describe how to set up Quickwit to ingest data from S3
with bucket notification events flowing through SQS. We will first create the
AWS resources (S3 bucket, SQS queue, notifications) using terraform. We will
then configure the Quickwit index and file source. Finally we will send some
data to the source bucket and verify that it gets indexed.

## AWS resources

The complete terraform script can be downloaded [here](/docs/assets/sqs-file-source.tf).

First, create the bucket that will receive the data files (NDJSON format):

```
resource "aws_s3_bucket" "file_source" {
  bucket_prefix = "qw-tuto-source-bucket"
}
```

Then setup the SQS queue that will carry the notifications when files are added
to the bucket. The queue is configured with a policy that allows the source bucket to
write notifications to it. Also create a deadletter queue to receive events that
couldn't be processed by the file source:

```
locals {
  sqs_notification_queue_name = "qw-tuto-s3-event-notifications"
  sqs_notification_deadletter_queue_name = "${sqs_notification_queue_name}-deadletter"
}

data "aws_iam_policy_document" "sqs_notification" {
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
  name = "${local.sqs_notification_deadletter_queue_name}"
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

Finally, configure the notification that writes messages to SQS each time a new
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

The source needs to have access to both the notification queue and the source
bucket. The following creates an IAM user and credentials that can be used to
associate the right permissions with your local Quickwit instance:

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

Download the [complete terraform script](/docs/assets/sqs-file-source.tf) and
run it using `terraform init` and `terraform apply`. After a successful
execution, the outputs required to configure Quickwit will be displayed. You can
display the sensitive key id and secret key by running:


```bash
terraform output quickwit_node_access_key_id
terraform output quickwit_node_secret_access_key
```

## Run Quickwit

[Install Quickwit locally](/docs/get-started/installation), then run Quickwit
with the necessary access rights by replacing the
`<quickwit_node_access_key_id>` and `<quickwit_node_secret_access_key>` with the
matching Terraform output values:

```bash
AWS_ACCESS_KEY_ID=<quickwit_node_access_key_id> \
AWS_SECRET_ACCESS_KEY=<quickwit_node_secret_access_key> \
AWS_REGION=us-east-1 \
./quickwit run
```

## Configure the index and the source

In another terminal, create an index:

```bash
cat << EOF > tutorial-sqs-file-index.yaml
version: 0.7
index_id: tutorial-sqs-file
doc_mapping:
  mode: dynamic
indexing_settings:
  commit_timeout_secs: 5
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

Wait approximately 20 seconds and the data should appear in the index:

```bash
./quickwit index describe --index tutorial-sqs-file
```

## Tear down the resources

The resources instantiated above don't incur any fixed costs, but we still
recommend deleting them after you complete the tutorial. In the directory with
the Terraform script, run `terraform destroy`.
