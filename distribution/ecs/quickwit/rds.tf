resource "random_password" "quickwit_db" {
  count   = local.use_external_rds ? 0 : 1
  length  = 64
  special = false
}

module "quickwit_db" {
  count   = local.use_external_rds ? 0 : 1
  source  = "terraform-aws-modules/rds/aws"
  version = "6.5.2"

  identifier = "quickwit-metastore-${local.module_id}"

  engine               = "postgres"
  engine_version       = "16"
  family               = "postgres16" # DB parameter group
  major_engine_version = "16"         # DB option group

  instance_class    = var.rds_config.instance_class
  multi_az          = var.rds_config.multi_az
  allocated_storage = 5

  db_name  = "quickwit"
  username = "quickwit"
  password = random_password.quickwit_db[0].result

  port                                 = "5432"
  publicly_accessible                  = false
  manage_master_user_password          = true
  manage_master_user_password_rotation = false
  iam_database_authentication_enabled  = true
  vpc_security_group_ids               = [aws_security_group.quickwit_db[0].id]
  db_subnet_group_name                 = aws_db_subnet_group.quickwit[0].name

  maintenance_window = "Mon:00:00-Mon:03:00"

  create_monitoring_role = true
  monitoring_interval    = "30"
  monitoring_role_name   = "RDSQuickwitMonitoringRole-${local.module_id}"

  deletion_protection = false
  skip_final_snapshot = true
}

resource "aws_security_group" "quickwit_db" {
  count       = local.use_external_rds ? 0 : 1
  name        = "quickwit-db-${local.module_id}"
  description = "Security group for the Quickwit Metastore DB"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Connection from explicitly allowed resources"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.quickwit_cluster_member_sg.id]
  }
}

resource "aws_db_subnet_group" "quickwit" {
  count       = local.use_external_rds ? 0 : 1
  name        = "quickwit-${local.module_id}"
  description = "Quickwit metastore"
  subnet_ids  = var.subnet_ids
}

resource "aws_ssm_parameter" "postgres_credential" {
  count = local.use_external_rds ? 0 : 1
  name  = "/quickwit/${local.module_id}/postgres"
  type  = "SecureString"
  value = "postgres://${module.quickwit_db[0].db_instance_username}:${random_password.quickwit_db[0].result}@${module.quickwit_db[0].db_instance_address}:${module.quickwit_db[0].db_instance_port}/${module.quickwit_db[0].db_instance_name}"
}
