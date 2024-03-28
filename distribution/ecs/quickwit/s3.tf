data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "index" {
  count         = var.quickwit_index_s3_prefix == "" ? 1 : 0
  bucket        = "quickwit-ecs-index-${data.aws_caller_identity.current.account_id}-${local.s3_id}"
  force_destroy = true
}
