---
title: Lambda configuration
sidebar_position: 6
---

Quickwit supports offloading leaf search operations to AWS Lambda for horizontal scaling. When the local search queue becomes saturated, overflow splits are automatically sent to Lambda functions for processing.

:::note
Lambda offloading is currently only supported on AWS.
:::

## How it works

Lambda offloading is **only active when a `lambda` configuration section is present** under `searcher` in your node configuration. When configured:

1. Quickwit monitors the local search queue depth
2. When pending searches exceed the `offload_threshold`, new splits are sent to Lambda instead of being queued locally
3. Lambda returns per-split search results that are cached and merged with local results

This allows Quickwit to handle traffic spikes without provisioning additional searcher nodes.

## Startup validation

When a `lambda` configuration is defined, Quickwit performs a **dry run invocation** at startup to verify that:
- The Lambda function exists
- The function version matches the embedded binary
- The invoker has permission to call the function

If this validation fails, **Quickwit will fail to start**. This ensures that Lambda offloading works correctly before the node begins serving traffic.

## Configuration

Add a `lambda` section under `searcher` in your node configuration:

```yaml
searcher:
  lambda:
    offload_threshold: 100
    auto_deploy:
      execution_role_arn: arn:aws:iam::123456789012:role/quickwit-lambda-role
      memory_size: 5 GiB
      invocation_timeout_secs: 15
```

### Lambda configuration options

| Property | Description | Default value |
| --- | --- | --- |
| `function_name` | Name of the AWS Lambda function to invoke. | `quickwit-lambda-search` |
| `max_splits_per_invocation` | Maximum number of splits to send in a single Lambda invocation. Must be at least 1. | `10` |
| `offload_threshold` | Number of pending local searches before offloading to Lambda. A value of `0` offloads everything to Lambda. | `100` |
| `auto_deploy` | Auto-deployment configuration. If set, Quickwit automatically deploys or updates the Lambda function at startup. | (none) |

### Auto-deploy configuration options

| Property | Description | Default value |
| --- | --- | --- |
| `execution_role_arn` | **Required.** IAM role ARN for the Lambda function's execution role. | |
| `memory_size` | Memory allocated to the Lambda function. More memory provides more CPU. | `5 GiB` |
| `invocation_timeout_secs` | Timeout for Lambda invocations in seconds. | `15` |

## Deployment options

### Automatic deployment (recommended)

With `auto_deploy` configured, Quickwit automatically:
1. Creates the Lambda function if it doesn't exist
2. Updates the function code if the embedded binary has changed
3. Publishes a new version with a unique identifier
4. Garbage collects old versions (keeps current + 5 most recent)

This is the recommended approach as it ensures the Lambda function always matches the Quickwit binary version.

### Manual deployment

You can deploy the Lambda function manually without `auto_deploy`:
1. Download the Lambda zip from [GitHub releases](https://github.com/quickwit-oss/quickwit/releases)
2. Create or update the Lambda function using AWS CLI, Terraform, or the AWS Console
3. Publish a version with description format `quickwit:{version}-{sha1}` (e.g., `quickwit:0_8_0-fa752891`)

The description must match the format Quickwit expects, or it won't find the function version.

## IAM permissions

### Permissions for the Quickwit node

The IAM role or user running Quickwit needs the following permissions to invoke Lambda:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:*:*:function:quickwit-lambda-search:*"
    }
  ]
}
```

If using `auto_deploy`, additional permissions are required for deployment:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:CreateFunction",
        "lambda:GetFunction",
        "lambda:UpdateFunctionCode",
        "lambda:PublishVersion",
        "lambda:ListVersionsByFunction",
        "lambda:DeleteFunction"
      ],
      "Resource": "arn:aws:lambda:*:*:function:quickwit-lambda-search"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::*:role/quickwit-lambda-role",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": "lambda.amazonaws.com"
        }
      }
    }
  ]
}
```

### Lambda execution role

The Lambda function requires an execution role with S3 read access to your index data. CloudWatch logging permissions are not required.

Example policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::your-index-bucket/*"
    }
  ]
}
```

The execution role must also have a trust policy allowing Lambda to assume it:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

## Versioning

Quickwit uses content-based versioning for Lambda:
- A SHA1 hash of the Lambda binary is computed at build time
- This hash is embedded in the Lambda function description as `quickwit:{version}-{sha1_short}`
- When Quickwit starts, it searches for a version matching this description
- Different Quickwit builds with the same Lambda binary share the same Lambda version
- Updating the Lambda binary automatically triggers a new deployment

## Example configuration


Minimal configuration (with auto-deployment):

```yaml
searcher:
  lambda:
    auto_deploy:
      execution_role_arn: arn:aws:iam::123456789012:role/quickwit-lambda-role
```


Full configuration (auto-deployment):

```yaml
searcher:
  lambda:
    function_name: quickwit-lambda-search
    max_splits_per_invocation: 10
    offload_threshold: 10
    auto_deploy:
      execution_role_arn: arn:aws:iam::123456789012:role/quickwit-lambda-role
      memory_size: 5 GiB
      invocation_timeout_secs: 15
```

Aggressive offloading (send everything to Lambda):

```yaml
searcher:
  lambda:
    function_name: quickwit-lambda-search
    offload_threshold: 0
    auto_deploy:
      execution_role_arn: arn:aws:iam::123456789012:role/quickwit-lambda-role
```
