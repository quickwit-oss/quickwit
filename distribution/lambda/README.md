
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

Once the virtualenv is activated, you can install the required dependencies.

```bash
pip install .
```

If you prefer using Poetry, achieve the same by running:
```bash
poetry shell
poetry install
```

## Example stacks

Provided demonstration setups:
- HDFS example data: index the the [HDFS
  dataset](https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants-10000.json)
  by triggering the Quickwit lambda manually.
- Mock Data generator: start a mock data generator lambda that pushes mock JSON
  data every X minutes to S3. Those file trigger the Quickwit indexer lambda
  automatically.

## Deploy and run

The Makefile is a usefull entrypoint to show how the Lambda deployment can used.

Configure your shell and AWS account:
```bash
# replace with you AWS account ID and prefered region
export CDK_ACCOUNT=123456789
export CDK_REGION=us-east-1
make bootstrap
```

Deploy, index and query the HDFS dataset:
```bash
make deploy-hdfs
make invoke-hdfs-indexer
make invoke-hdfs-searcher
```

Deploy the mock data generator and query the indexed data:
```bash
make deploy-mock-data
# wait a few minutes...
make invoke-mock-data-searcher
```

## Set up a search API

You can configure an API around the Quickwit search Lambda using the following
CDK code (`quickwit_service` is an instance of the `QuickwitService`` construct):

```python
  api = aws_apigateway.RestApi(
      self,
      "quickwit-search-api",
      rest_api_name=f"Quickwit {index_id} search API",
      deploy=False,
  )
  searcher_integration = aws_apigateway.LambdaIntegration(
      quickwit_service.searcher.lambda_function
  )
  search_resource = (
      api.root.add_resource("v1").add_resource(index_id).add_resource("search")
  )
  search_resource.add_method("POST", searcher_integration)
  api_deployment = aws_apigateway.Deployment(self, "api-deployment", api=api)
  api_stage = aws_apigateway.Stage(
      self, "api", deployment=api_deployment, stage_name="api"
  )
  api.deployment_stage = api_stage
```

Note that the response is always gzipped compressed, regardless the
`Accept-Encoding` request header:

```bash
curl -d '{"query":"*"}' -H "Content-Type: application/json" -X POST https://{api_id}.execute-api.{region}.amazonaws.com/api/v1/{index-id}/search --compressed
```

## Useful CDK commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation
