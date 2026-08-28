---
title: Install Quickwit on AWS EKS
sidebar_label: AWS EKS
sidebar_position: 3
---

This guide will help you set up a Quickwit cluster on EKS with the correct S3 permissions.

## Prerequisites
- Running Elastic Kubernetes cluster (EKS)
- `kubectl`
- Permission to create the IAM role and Policies
- AWS CLI
- `eksctl` if you don't have an IAM OIDC provider for your cluster.

## Set up

Let's use the following environment variables:

```bash
export NAMESPACE=qw-tutorial
export EKS_CLUSTER=qw-cluster
export S3_BUCKET={your-bucket}
export SERVICE_ACCOUNT_NAME=qw-sa
export REGION={your-region}
export CLUSTER_ID={your-cluster-id}
```

Create the namespace for our playground:

```bash
kubectl create ns ${NAMESPACE}
```

And set this namespace as the default one:

```bash
kubectl config set-context --current --namespace=${NAMESPACE}
```


### Create IAM OIDC provider if you don't have one

To check if you have one provider for your EKS cluster, just run:

```bash
aws iam list-open-id-connect-providers
```

If you have one, you will get a response similar to this one:

```json
{
    "OpenIDConnectProviderList": [
        {
            "Arn": "arn:aws:iam::(some-ID):oidc-provider/oidc.eks.{your-region}.amazonaws.com/id/{your-cluster-id}"
        }
    ]
}
```

If you don't, run the following command:

```bash
eksctl utils associate-iam-oidc-provider --cluster ${EKS_CLUSTER} --approve
```

You can run again `aws iam list-open-id-connect-providers` to get the ARN of the provider.

### Create an IAM policy

You need to set the following policy to allow Quickwit to access your S3 bucket.

Then create the policy using the AWS CLI:

```bash
cat > s3-policy.json <<EOF
{
    "Statement": [
        {
          "Action": [
            "s3:ListBucket"
          ],
          "Effect": "Allow",
          "Resource": [
            "arn:aws:s3:::${S3_BUCKET}"
          ]
        },
        {
            "Action": [
                "s3:AbortMultipartUpload",
                "s3:DeleteObject",
                "s3:GetObject",
                "s3:ListMultipartUploadParts",
                "s3:PutObject"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::${S3_BUCKET}/*"
            ]
        }
    ],
    "Version": "2012-10-17"
}
EOF
```

```bash
aws iam create-policy --policy-name qw-s3-policy --policy-document file://s3-policy.json
```

### Create an IAM Role and attach the policy

```bash
cat > s3-role.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::${IAM_ID}:oidc-provider/oidc.eks.${REGION}.amazonaws.com/id/${CLUSTER_ID}"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "oidc.eks.${REGION}.amazonaws.com/id/${CLUSTER_ID}:aud": "sts.amazonaws.com",
          "oidc.eks.${REGION}.amazonaws.com/id/${CLUSTER_ID}:sub": "system:serviceaccount:${NAMESPACE}:${SERVICE_ACCOUNT_NAME}"
        }
      }
    }
  ]
}
EOF
```

```bash
aws iam create-role --role-name s3-role --assume-role-policy-document file://s3-role.json
```

And then attach the policy to the role:

```bash
aws iam attach-role-policy --role-name s3-role --policy-arn=arn:aws:iam::${IAM_ID}:policy/s3-policy
```

## Install Quickwit using Helm

We are now ready to install Quickwit on EKS. If you'd like to know more about Helm, consult our [comprehensive guide](./helm.md) for installing Quickwit on Kubernetes.

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

serviceAccount:
  create: true
  name: ${SERVICE_ACCOUNT_NAME}
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::${ARN_ID}:role/${SERVICE_ACCOUNT_NAME}

config:
  default_index_root_uri: s3://${S3_BUCKET}/qw-indexes
  metastore_uri: s3://${S3_BUCKET}/qw-indexes

```

We're ready to deploy:

```bash
helm install <deployment name> quickwit/quickwit -f values.yaml
```

## Check that Quickwit is running

It should take a few seconds for the cluster to start. During the startup process, individual pods might restart themselves several times.

To access the UI, you can run the following command and then open your browser at [http://localhost:7280](http://localhost:7280):

```
kubectl port-forward svc/quickwit-searcher 7280:7280
```

## Uninstall the deployment

Run the following Helm command to uninstall the deployment

```bash
helm uninstall <deployment name>
```

And don't forget to clean your bucket, Quickwit should have stored 3 files in `s3://${S3_BUCKET}/qw-indexes`.
