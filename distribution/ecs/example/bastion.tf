variable "bastion_public_key" {
  description = "The public key used to connect to the bastion host. If empty, no bastion is created."
  default     = ""
}

output "bastion_ip" {
  value = var.bastion_public_key != "" ? aws_instance.bastion[0].public_ip : null
}

data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"] # Canonical
}

resource "aws_security_group" "allow_ssh" {
  count       = var.bastion_public_key != "" ? 1 : 0
  name        = "qw_ecs_bastion_allow_ssh"
  description = "Allow SSH inbound traffic from everywhere"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "bastion" {
  count                       = var.bastion_public_key != "" ? 1 : 0
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = "t3.nano"
  key_name                    = aws_key_pair.bastion_key[0].key_name
  subnet_id                   = module.vpc.public_subnets[0]
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.allow_ssh[0].id]

  tags = {
    Name = "quickwit-ecs-bastion"
  }
}

resource "aws_key_pair" "bastion_key" {
  count      = var.bastion_public_key != "" ? 1 : 0
  key_name   = "quickwit-ecs-bastion-key"
  public_key = var.bastion_public_key
}
