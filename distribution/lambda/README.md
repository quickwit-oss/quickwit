
# CDK template for running Quickwit on AWS Lambda

## Prerequisites

- Install AWS CDK Toolkit (cdk command)
  - `npm install -g aws-cdk `
- Install the AWS CLI
  - https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

## AWS Lambda service quotas

For newly created AWS accounts, a conservative quota of 10 concurrent executions
is applied to Lambda in each individual region. If that's the case, CDK won't be
able to apply the reserved concurrency of the indexing Quickwit lambda. You can
increase the quota without charge using the [Service Quotas
console](https://console.aws.amazon.com/servicequotas/home/services/lambda/quotas).

> **Note:** The request can take hours or even days to be processed.

## Python venv

This project is set up like a standard Python project. The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```bash
python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```bash
source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```bash
.venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```


## Deploy and run

The Makefile is a usefull entrypoint to show how the Lambda deployement can used.
```bash
# replace with you AWS account ID and prefered region
export CDK_ACCOUNT=123456789
export CDK_REGION=us-east-1
make init
make deploy
make upload-src-file
make invoke-indexer
make invoke-searcher
```


## Useful CDK commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation
