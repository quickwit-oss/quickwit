# ECS deployment for quickwit

## Run Quickwit in your infrastructure

Create a Quickwit module using:

```terraform
module "quickwit" {
  source = "github.com/quickwit-oss/quickwit/distribution/ecs/quickwit"

  vpc_id                       =       # VPC in which all resources will be created
  subnet_ids                   = [...] # At least 2 private subnets must be specified
  quickwit_ingress_cidr_blocks = [...] # List of CIDR blocks allowed to access to the Quickwit API
}
```

The Quickwit cluster is running on a private subnet. For ECS to pull the image:
- if using the default Docker Hub image `quickwit/quickwit`, the subnets
specified must be configured with a NAT Gateway (no public IPs are attached to
the tasks)
- if using an image hosted on ECR, a VPC endpoint for ECR can be used instead of
a NAT Gateway


## Module configurations

To get the list of available configurations, check the `./quickwit/variables.tf`
file.

### Tips

Metastore database backups are disabled as restoring one would lead to
inconsistencies with the index store on S3. To ensure high availability, you
should enable `rds_config.multi_az` instead.

Using NAT Gateways for the image registry is quite costly (~$0.05/hour/AZ). If
you are not already using NAT Gateways in the AZs where Quickwit will be
deployed, you should probably push the Quickwit image to ECR and use ECR
interface VPC endpoints instead (~$0.01/hour/AZ).

Sidecar container and custom logging configurations can be configured using the
variables `sidecar_container_definitions`, `sidecar_container_dependencies`,
`log_configuration`, `enable_cloudwatch_logging`. A more concrete example can be
found in the `./example/sidecar.tf` file.

## Running the example stack

We provide an example of self contained deployment with an ad-hoc VPC. 

> [!IMPORTANT] This stack costs ~$150/month to run (Fargate tasks, NAT Gateways
> and RDS)

To make it easy to access your the Quickwit cluster, this stack includes a
bastion instance. Access is secured using an RSA key pair that you need to
provide (e.g generated with `ssh-keygen -t rsa`).

In the `./example` directory create a `terraform.tfvars` file with the public
key of your RSA key pair:

```terraform
bastion_public_key = "ssh-rsa ..."
```

> [!NOTE] You can skip the creation of the bastion by not specifying the
> `bastion_public_key` variable, but that would make it hard to access and
> experiment with the created Quickwit cluster.

In the same directory (`./example`) run:

```bash
terraform init
terraform apply
```

The successful `apply` command should output the IP of the bastion EC2 instance.
You can port forward Quickwit's search UI using:

```bash
ssh -L -N 7280:searcher.quickwit:7280 -i {your-private-key-file} ubuntu@{bastion_ip}
```

To ingest some example dataset, log into the bastion:

```bash
ssh -i {your-private-key-file} ubuntu@{bastion_ip}

# create the log index
wget https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/hdfs-logs/index-config.yaml
curl -X POST \
  -H "content-type: application/yaml" \
  --data-binary @index-config.yaml \
  http://indexer.quickwit:7280/api/v1/indexes

# import some data
wget https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants-10000.json
curl -X POST \
  -H "content-type: application/json" \
  --data-binary @hdfs-logs-multitenants-10000.json \
  http://indexer.quickwit:7280/api/v1/hdfs-logs/ingest
```

If your SSH tunnel to the searcher is still running, you should be able to see
the ingested data in the UI as soon as it is committed (~30 seconds).
