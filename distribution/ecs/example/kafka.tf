# Example configuration for injecting SSL keys for securing a Kafka connection
# You can then create a secured Kafka source along these lines:
# 
# version: 0.8
# source_id: kafka-source
# source_type: kafka
# num_pipelines: 2
# params:
#   topic: your-topic
#   client_params:
#     bootstrap.servers: "your-kafka-broker.com"
#     security.protocol: "SSL"
#     ssl.ca.location: "/quickwit/keys/ca.pem"
#     ssl.certificate.location: "/quickwit/keys/service.cert"
#     ssl.key.location: "/quickwit/keys/service.key"


locals {
  ca_pem       = "echo \"$CA_PEM\" > /quickwit/cfg/ca.pem"
  service_cert = "echo \"$SERVICE_CERT\" > /quickwit/cfg/service.cert"
  service_key  = "echo \"$SERVICE_KEY\" > /quickwit/cfg/service.key"
  example_kafka_sidecar_container_definitions = {
    kafka_key_init = {
      name                      = "kafka_key_init"
      essential                 = false
      image                     = "busybox"
      command                   = ["sh", "-c", "${local.ca_pem} && ${local.service_cert} && ${local.service_key}"]
      enable_cloudwatch_logging = true
      mount_points = [
        {
          sourceVolume  = "quickwit-keys"
          containerPath = "/quickwit/keys"
        }
      ]
      secrets = [
        {
          name      = "CA_PEM"
          valueFrom = "arn:aws:secretsmanager:eu-west-1:542709600413:secret:your_kafka_ca_pem"
        },
        {
          name      = "SERVICE_CERT"
          valueFrom = "arn:aws:secretsmanager:eu-west-1:542709600413:secret:your_kafka_service_cert"
        },
        {
          name      = "SERVICE_KEY"
          valueFrom = "arn:aws:secretsmanager:eu-west-1:542709600413:secret:your_kafka_service_key"
        }
      ]
    }
  }

  example_kafka_sidecar_container_dependencies = [
    {
      condition     = "SUCCESS"
      containerName = "kafka_key_init"
    }
  ]
}
