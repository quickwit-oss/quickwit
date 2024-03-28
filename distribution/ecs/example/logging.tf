# Example configuration for pushing ECS logs to Datadog

locals {
  example_datadog_api_key_arn = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:your-dd-api-key-secret"
  example_log_configuration = {
    logDriver = "awsfirelens"
    options = {
      "Name"       = "datadog"
      "Host"       = "http-intake.logs.datadoghq.eu"
      "TLS"        = "on"
      "dd_service" = "quickwit"
      "dd_source"  = "quickwit"
      "provider"   = "ecs"
    }
    secretOptions = [
      {
        "name"      = "apikey"
        "valueFrom" = local.example_datadog_api_key_arn
      }
    ]
  }
  example_sidecar_container_definitions = {
    log_router = {
      name                      = "log_router"
      image                     = "public.ecr.aws/aws-observability/aws-for-fluent-bit:stable"
      memory_reservation        = 50,
      enable_cloudwatch_logging = true
      firelens_configuration = {
        "type" = "fluentbit",
        "options" = {
          "enable-ecs-log-metadata" = "true"
        }
      },
    }
    datadog_agent = {
      name  = "datadog-agent",
      image = "public.ecr.aws/datadog/agent:latest",
      port_mappings = [
        {
          "containerPort" = 8126,
          "hostPort"      = 8126,
          "protocol"      = "tcp"
        }
      ],
      start_timeout             = 120
      readonly_root_filesystem  = false
      enable_cloudwatch_logging = true
      environment = [
        {
          name  = "ECS_FARGATE",
          value = "true"
        },
        {
          name  = "DD_LOGS_ENABLED",
          value = "true"
        },
        {
          name  = "DD_LOGS_CONFIG_CONTAINER_COLLECT_ALL",
          value = "true"
        },
        {
          name  = "DD_SITE",
          value = "datadoghq.eu"
        }
      ],
      secrets = [
        {
          name      = "DD_API_KEY"
          valueFrom = local.example_datadog_api_key_arn
        }
      ]
      health_check = {
        "command" = [
          "CMD-SHELL",
          "agent health"
        ],
        "interval" = 30,
        "timeout"  = 5,
        "retries"  = 3
      }
    }
  }

  example_sidecar_container_dependencies = [
    {
      condition     = "START"
      containerName = "log_router"
    },
    {
      condition     = "HEALTHY"
      containerName = "datadog-agent"
    }
  ]
}
