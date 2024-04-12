data "aws_iam_policy_document" "quickwit_task_permission" {
  # Reference: https://quickwit.io/docs/guides/aws-setup#amazon-s3
  statement {
    actions = [
      "s3:ListBucket",
      "s3:ListObjects",
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [
      "arn:aws:s3:::${local.quickwit_index_s3_prefix}*",
    ]
  }

  statement {
    actions = [
      "ecs:Describe*",
      "ecs:List*"
    ]

    resources = ["*"]
  }
}

resource "aws_iam_policy" "quickwit" {
  name = "quickwit-task-policy-${local.module_id}"
  path = "/"

  policy = data.aws_iam_policy_document.quickwit_task_permission.json
}
