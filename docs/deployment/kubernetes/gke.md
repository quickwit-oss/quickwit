---
title: Install Quickwit on Google GKE
sidebar_label: Google GKE
sidebar_position: 2
---

This guide will help you set up a Quickwit cluster on GKE with the correct GCS permissions.


## Set up

Before installing Quickwit with Helm, let's create a namespace for our playground.

```
export NS=quickwit-tutorial
kubectl create ns ${NS}
```

Quickwit stores its index on an object storage. We will use GCS, which is natively supported since the 0.7 version (for versions < 0.7, you should use an S3 interoperability key).

The following steps create a GCP and a GKE service account and bind them together.

```bash
export PROJECT_ID={your-project-id}
export GCP_SERVICE_ACCOUNT=quickwit-tutorial
export GKE_SERVICE_ACCOUNT=quickwit-sa
export BUCKET=your-bucket

kubectl create serviceaccount ${GKE_SERVICE_ACCOUNT} -n ${NS}

gcloud iam service-accounts create ${GCP_SERVICE_ACCOUNT} --project=${PROJECT_ID}

gcloud storage buckets add-iam-policy-binding gs://${BUCKET} \
--member "serviceAccount:${GCP_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com" \
--role "roles/storage.objectAdmin"

# Notice that the member is related to a namespace.
gcloud iam service-accounts add-iam-policy-binding ${GCP_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com \
--role roles/iam.workloadIdentityUser \
--member "serviceAccount:${PROJECT_ID}.svc.id.goog[${NS}/${GKE_SERVICE_ACCOUNT}]"

# Now we can annotate our service account!
kubectl annotate serviceaccount ${GKE_SERVICE_ACCOUNT} \
iam.gke.io/gcp-service-account=${GCP_SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com \
-n ${NS}
```

## Install Quickwit using Helm

We are now ready to install Quickwit on GKE. If you'd like to know more about Helm, consult our [comprehensive guide](./helm.md) for installing Quickwit on Kubernetes.

```bash
helm repo add quickwit https://helm.quickwit.io
helm repo update quickwit
```

Let's set Quickwit `values.yaml`:

```yaml
# We use the edge version here as we recently fixed
# a bug which prevents the metastore from running on GCS.
image:
    repository: quickwit/quickwit
    pullPolicy: Always
    tag: edge

serviceAccount:
  create: false
  name: quickwit-sa

config:
  default_index_root_uri: gs://${BUCKET}/qw-indexes
  metastore_uri: gs://${BUCKET}/qw-indexes

```

We're ready to deploy:

```bash
helm install <deployment name> quickwit/quickwit -f values.yaml
```

## Check that Quickwit is running

It should take a few seconds for the cluster to start. During the startup process, individual pods might restart themselves several times.

To access the UI, you can run the following command and then open your browser at [http://localhost:7280](http://localhost:7280):

```
kubectl port-forward svc/release-name-quickwit-searcher 7280:7280
```

## Uninstall the deployment

Run the following Helm command to uninstall the deployment

```bash
helm uninstall <deployment name>
```

And don't forget to clean your bucket, Quickwit should have stored 3 files in `gs://{BUCKET}/qw-indexes`.
