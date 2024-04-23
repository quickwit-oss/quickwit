locals {
  quickwit_data_dir = "/quickwit/qwdata"

  quickwit_common_environment = [
    {
      name  = "QW_PEER_SEEDS"
      value = join(",", var.quickwit_peer_list)
    },
    {
      name  = "NO_COLOR"
      value = "true"
    },
    {
      name  = "QW_CLUSTER_ID"
      value = "ecs-${var.module_id}"
    },
    {
      name  = "QW_LISTEN_ADDRESS"
      value = "0.0.0.0"
    },
    {
      name  = "QW_DATA_DIR"
      value = local.quickwit_data_dir
    },
    {
      name  = "QW_DEFAULT_INDEX_ROOT_URI"
      value = "s3://${var.quickwit_index_s3_prefix}"
    },
  ]

  nb_extra_policies             = length(var.service_config.extra_task_policy_arns)
  extra_tasks_iam_role_policies = { for i in range(local.nb_extra_policies) : "extra_policy_${i}" => var.service_config.extra_task_policy_arns[i] }
  tasks_iam_role_policies       = merge({ s3_access = var.s3_access_policy_arn }, local.extra_tasks_iam_role_policies)
}
