---
title: Kubernetes with Helm
sidebar_position: 2
---

[Helm](https://helm.sh) is a package manager for Kubernetes that allows you to configure, install, and upgrade containerized applications in a Kubernetes cluster. You can use the [official chart](https://github.com/quickwit-oss/helm-charts) for Quickwit to define your own configuration in a YAML file (`values.yaml`) and use Helm to deploy your applications in a version-controlled and reproducible way.

# Install Quickwit using Helm

1. Install the Helm CLI

Select the appropriate method for your environment on Helm's [install documentation page](https://helm.sh/docs/intro/install/).

2. Add the Quickwit Helm chart repository to Helm:

    helm repo add quickwit https://helm.quickwit.io

3. Update the repository

    helm repo update

4. Create and customize your configuration file `values.yaml`

5. Deploy Quickwit

    helm install <deployment name> quickwit/quickwit


# Uninstall the deployment

1. Run the following Helm command to uninstall the deployment

    helm delete <deployment name>
