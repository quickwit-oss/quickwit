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

}
