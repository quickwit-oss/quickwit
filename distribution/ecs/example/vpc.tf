module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.5.3"

  name = "quickwit-ecs"
  cidr = "10.0.0.0/16"

  azs             = ["${data.aws_region.current.name}a", "${data.aws_region.current.name}b"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24"]

  enable_nat_gateway = true
}
