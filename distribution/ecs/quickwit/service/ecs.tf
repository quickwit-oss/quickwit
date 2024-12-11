module "quickwit_service" {
  source  = "terraform-aws-modules/ecs/aws//modules/service"
  version = "5.9.3"

  name        = "quickwit-${var.service_name}-${var.module_id}"
  cluster_arn = var.cluster_arn

  cpu    = var.service_config.cpu
  memory = var.service_config.memory
  ephemeral_storage = {
    size_in_gib = var.service_config.ephemeral_storage_gib
  }

  container_definitions = merge(var.sidecar_container_definitions, {
    quickwit = {
      cpu    = var.service_config.cpu
      memory = var.service_config.memory

      essential                 = true
      image                     = var.quickwit_image
      enable_cloudwatch_logging = var.enable_cloudwatch_logging

      command = ["run"]

      environment = local.quickwit_common_environment

      secrets = [
        {
          name      = "QW_METASTORE_URI"
          valueFrom = var.postgres_uri_secret_arn
        }
      ]

      port_mappings = [
        {
          name          = "rest"
          containerPort = 7280
          protocol      = "tcp"
        },
        {
          name          = "grpc"
          containerPort = 7281
          protocol      = "tcp"
        },
        {
          name          = "gossip"
          containerPort = 7280
          protocol      = "udp"
        }
      ]

      log_configuration = var.log_configuration

      mount_points = [
        {
          sourceVolume  = "quickwit-data-vol"
          containerPath = local.quickwit_data_dir
        },
        # A volume that can be used to inject secrets as files.
        {
          sourceVolume  = "quickwit-keys"
          containerPath = "/quickwit/keys"
        }
      ]

      stopTimeout = var.stop_timeout

      dependencies = var.sidecar_container_dependencies
    }
  })

  requires_compatibilities = ["FARGATE"]
  runtime_platform = {
    operating_system_family = "LINUX"
    cpu_architecture        = var.quickwit_cpu_architecture
  }

  service_registries = {
    registry_arn   = var.service_discovery_registry_arn
    container_name = "quickwit"
  }

  subnet_ids = var.subnet_ids
  security_group_rules = {
    ingress_internal = {
      type      = "ingress"
      from_port = 7280
      to_port   = 7281
      protocol  = "-1"

      source_security_group_id = var.quickwit_cluster_member_sg_id
    }
    ingress_external = {
      type      = "ingress"
      from_port = 7280
      to_port   = 7281
      protocol  = "-1"

      cidr_blocks = var.ingress_cidr_blocks
    }
    egress_all = {
      type      = "egress"
      from_port = 0
      to_port   = 0
      protocol  = "-1"

      cidr_blocks = ["0.0.0.0/0"]
    }
  }
  security_group_ids = [var.quickwit_cluster_member_sg_id]

  enable_autoscaling = false
  desired_count      = var.service_config.desired_count

  volume = [
    {
      name = "quickwit-data-vol"
    },
    {
      name = "quickwit-keys"
    }
  ]

  tasks_iam_role_policies = local.tasks_iam_role_policies

  task_exec_iam_role_policies = {
    policy = var.task_execution_policy_arn
  }

}
