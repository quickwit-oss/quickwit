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
should enable `rds_config.multi_az` instead. To use your own Postgres database
instead of creating a new RDS instance, configure the
`external_postgres_uri_secret_arn` variable (e.g ARN of an SSM parameter with
the value `postgres://user:password@domain:port/db`).

Using NAT Gateways for the image registry is quite costly (approx. $0.05/hour/AZ). If
you are not already using NAT Gateways in the AZs where Quickwit will be
deployed, you should probably push the Quickwit image to ECR and use ECR
interface VPC endpoints instead (approx. ~$0.01/hour/AZ).

When using the default image, you will quickly run into the Docker Hub rate
limiting. We recommend pushing the Quickwit image to ECR and configure that as
`quickwit_image`. Note that the architecture of the image that you push to ECR
must match the `quickwit_cpu_architecture` variable (`ARM64` by default).

Sidecar container and custom logging configurations can be configured using the
variables `sidecar_container_definitions`, `sidecar_container_dependencies`,
`log_configuration`, `enable_cloudwatch_logging`. See [custom log
routing](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_firelens.html).

You can use sidecars to inject additional secrets as files. This can be
useful for configuring sources such as Kafka. See `./example/kafka.tf` for an
example.

To access external AWS services like the Kinesis source, use the
`quickwit_indexer.extra_task_policy_arns` variable to attach the necessary
IAM policies to indexers.

## Running the example stack

We provide an example of self contained deployment with an ad-hoc VPC. 

> [!IMPORTANT]
> This stack costs ~$200/month to run (Fargate tasks, NAT Gateways
> and RDS)

### Deploy the Quickwit module and connect through a bastion

To make it easy to access your Quickwit cluster, the example stack includes
a bastion instance. Access is secured using an SSH key pair that you need to
provide (e.g generated with `ssh-keygen -t ed25519`).

In the `./example` directory, create a `terraform.tfvars` file with the public
key of your RSA key pair:

```terraform
bastion_public_key = "ssh-ed25519 ..."
```

> [!NOTE]
> You can skip the creation of the bastion by not specifying the
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
ssh -N -L 7280:searcher.quickwit:7280 -i {your-private-key-file} ubuntu@{bastion_ip}
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
  http://indexer.quickwit:7280/api/v1/hdfs-logs/ingest?commit=force
```

If your SSH tunnel to the searcher is still running, you should be able to see
the ingested data in the UI.

### Setup an ECR repository to avoid throttling from Docker Hub

By default, the example stack uses Docker Hub to pull the Quickwit image. This
is convenient but it quickly runs into rate limiting. To avoid this, in the
`terraform.tfvars` file, set the `dockerhub_pull_through_creds_secret_arn` to a
AWS Secret with the following content:

```json
{"username":"...","accessToken":"..."}
```

This will:
- provision an ECR repository and a pull through cache rule
- configure the Quickwit module to use that repository
