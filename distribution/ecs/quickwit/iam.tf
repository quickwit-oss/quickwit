data "aws_iam_policy_document" "quickwit_task_permission" {
  # Reference: https://quickwit.io/docs/guides/aws-setup#amazon-s3
  statement {
    actions = [
      "s3:ListBucket",
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [
      "arn:aws:s3:::${local.quickwit_index_s3_prefix}*",
    ]
  }
}

resource "aws_iam_policy" "quickwit_task_permission" {
  name = "quickwit-task-policy-${local.module_id}"
  path = "/"

  policy = data.aws_iam_policy_document.quickwit_task_permission.json
}

data "aws_iam_policy_document" "quickwit_task_execution_permission" {
  statement {
    actions = [
      "logs:PutLogEvents",
      "logs:CreateLogStream"
    ]

    resources = ["*"]
  }
  statement {
    actions = [
      "ecr:GetDownloadUrlForLayer",
      "ecr:GetAuthorizationToken",
      "ecr:BatchGetImage",
      "ecr:BatchCheckLayerAvailability",
      "ecr:CreateRepository",
      "ecr:BatchImportUpstreamImage"
    ]

    resources = ["*"]
  }

  statement {
    actions = ["ssm:GetParameters"]

    resources = [local.postgres_uri_parameter_arn]
  }

  statement {
    actions = ["secretsmanager:GetSecretValue"]

    resources = ["arn:aws:secretsmanager:*:*:secret:*"]
  }

}

resource "aws_iam_policy" "quickwit_task_execution_permission" {
  name = "quickwit-task-execution-policy-${local.module_id}"
  path = "/"

  policy = data.aws_iam_policy_document.quickwit_task_execution_permission.json
}
