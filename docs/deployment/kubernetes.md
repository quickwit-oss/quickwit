---
title: Kubernetes (Helm)
sidebar_position: 2
---

[Helm](https://helm.sh) is a package manager for Kubernetes that allows you to configure, install, and upgrade containerized applications in a Kubernetes cluster in a version-controlled and reproducible way.

## Install Quickwit using Helm

Install Quickwit on Kubernetes with the official Quickwit Helm chart. If you encounter any problem with the chart, please, open an issue in our [GitHub repository](https://github.com/quickwit-oss/helm-charts).

## Requirements

To deploy Quickwit on Kubernetes, you will need:

- kubectl, compatible with your cluster (+/- 1 minor release from your cluster) (`kubectl version`)
- Helm v3 (`helm version`)
- A Kubernetes cluster
- An instance of PostgreSQL server

If you want to install PostgreSQL to the same Kubernetes cluster, you can follow the instructions in the [ContainIQ Blog Post](https://www.containiq.com/post/deploy-postgres-on-kubernetes). You will need to replace the `data` portion in the `postgres-config.yaml` file from the post with the following:

```yaml
data:
  POSTGRES_DB: quickwit-metastore
  POSTGRES_USER: quickwit
  POSTGRES_PASSWORD: <my strong password>
```

1. Install `kubectl` and `helm`

To install `kubectl` and `helm` locally, follow the [Kubernetes](https://kubernetes.io/docs/tasks/tools/#install-kubectl) and [Helm](https://helm.sh/docs/intro/install/) documentation pages.

2. Add the Quickwit Helm chart repository to Helm

```bash
helm repo add quickwit https://helm.quickwit.io
```

3. Update the repository

```bash
helm repo update quickwit
```

4. Create and customize your configuration file `values.yaml`

You can inspect the default configuration values of the chart using the following command:

```bash
helm show values quickwit/quickwit
```

Here is an example of a minimal configuration:

```yaml
config:
  default_index_root_uri: s3://<my-bucket>/quickwit-indexes

  postgres:
    host: <postgres_host>
    port: 5432
    database: quickwit-metastore
    username: quickwit
    password: <my strong password> # This password will be stored as a Kubernetes Secret

  s3:
    region: eu-east-1
    # We recommend using IAM roles and permissions to access Amazon S3 resources,
    # but you can specify a pair of access and secret keys if necessary.
    access_key: <my access key>
    secret_key: <my secret key>
  ```

If you installed PostgreSQL using the method in the ContainIQ Blog Post, replace `<postgres_host>` in the configuration about with `postgres`.

5. Deploy Quickwit

```bash
helm install <deployment name> quickwit/quickwit -f values.yaml
```

6. Check that Quickwit is running

It might take some time for the cluster to start. During the startup process individual pods might restart themselves. The command that you typed on previous step should print out instructions on how to set port forwarding to connect to the cluster. While this command is running, you can access the cluster UI by navigating to http://127.0.0.1:8080 in your browser. You can also check the version of Quickwit by running:

```bash
curl http://127.0.0.1:8080/api/v1/version
```

## Uninstall the deployment

Run the following Helm command to uninstall the deployment

```bash
helm uninstall <deployment name>
```
