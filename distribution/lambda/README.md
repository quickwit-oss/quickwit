
# Quickwit on AWS Lambda

- Get the latest [Lambda binaries](https://github.com/quickwit-oss/quickwit/releases/tag/aws-lambda-beta-01)
- Read the [beta release announcement](https://quickwit.io/blog/quickwit-lambda-beta)

## Quickstart

- [Search on 20 million log dataset on S3 with lambda](https://quickwit.io/docs/get-started/tutorials/tutorial-aws-lambda-simple)
- [E2E use case with HTTP API](https://quickwit.io/docs/guides/e2e-serverless-aws-lambda)

## Build and run yourself

### Prerequisites

- Install AWS CDK Toolkit (cdk command)
  - `npm install -g aws-cdk`
- Ensure `curl` and `make` are installed
- To run the invocation example `make` commands, you will also need Python 3.10
  or later and `pip` installed (see [Python venv](#python-venv) below).

### AWS Lambda service quotas

For newly created AWS accounts, a conservative quota of 10 concurrent executions
is applied to Lambda in each individual region. If that's the case, CDK won't be
able to apply the reserved concurrency of the indexing Quickwit lambda. You can
increase the quota without charge using the [Service Quotas
console](https://console.aws.amazon.com/servicequotas/home/services/lambda/quotas).

> **Note:** The request can take hours or even days to be processed.

### Python venv

The Python environment is configured using pipenv:

```
# Install pipenv if needed.
pip install --user pipenv
pipenv shell
pipenv install
```

### Example stacks

Provided demonstration setups:
- HDFS example data: index the the [HDFS
  dataset](https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants-10000.json)
  by triggering the Quickwit lambda manually.
- Mock Data generator: start a mock data generator lambda that pushes mock JSON
  data every X minutes to S3. Those file trigger the Quickwit indexer lambda
  automatically.

### Deploy and run

The Makefile is a useful entrypoint to show how the Lambda deployment can used.

Configure your shell and AWS account:
```bash
# replace with you AWS account ID and preferred region
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

### Configurations

The following environment variables can be configured on the Lambda functions.
Note that only a small subset of all Quickwit configurations are exposed to
simplify the setup and avoid unstable deployments.

| Variable | Description | Default |
|---|---|---|
| QW_LAMBDA_INDEX_ID | the index this Lambda interacts with (one and only one) | required |
| QW_LAMBDA_METASTORE_URI | [Metastore URI](https://quickwit.io/docs/configuration/metastore-config) | required |
| QW_LAMBDA_INDEX_BUCKET | bucket name for split files | required |
| QW_LAMBDA_OPENTELEMETRY_URL | HTTP OTEL tracing collector endpoint | none, OTEL disabled |
| QW_LAMBDA_OPENTELEMETRY_AUTHORIZATION | Authorization header value for HTTP OTEL calls | none, OTEL disabled |
| QW_LAMBDA_ENABLE_VERBOSE_JSON_LOGS | true to enable JSON logging of spans and logs in Cloudwatch | false |
| RUST_LOG | [Rust logging config][1] | info |

[1]: https://rust-lang-nursery.github.io/rust-cookbook/development_tools/debugging/config_log.html


Indexer only:
| Variable | Description | Default |
|---|---|---|
| QW_LAMBDA_INDEX_CONFIG_URI | location of the index configuration file, e.g `s3://mybucket/index-config.yaml` | required |
| QW_LAMBDA_DISABLE_MERGE | true to disable compaction merges | false |
| QW_LAMBDA_DISABLE_JANITOR | true to disable retention enforcement and garbage collection | false |
| QW_LAMBDA_MAX_CHECKPOINTS | maximum number of ingested file names to keep in source history | 100 |

Searcher only:
| Variable | Description | Default |
|---|---|---|
| QW_LAMBDA_PARTIAL_REQUEST_CACHE_CAPACITY | `searcher.partial_request_cache_capacity` node config | 64M |


### Set up a search API

You can configure an HTTP API endpoint around the Quickwit Searcher Lambda. The
mock data example stack shows such a configuration. The API Gateway is enabled
when the `SEARCHER_API_KEY` environment variable is set:

```bash
SEARCHER_API_KEY=my-at-least-20-char-long-key make deploy-mock-data
```

> [!WARNING]  
> The API key is stored in plain text in the CDK stack. For a real world
> deployment, the key should be fetched from something like [AWS Secrets
> Manager](https://docs.aws.amazon.com/cdk/v2/guide/get_secrets_manager_value.html).

Note that the response is always gzipped compressed, regardless the
`Accept-Encoding` request header:

```bash
curl -d '{"query":"quantity:>5", "max_hits": 10}' -H "Content-Type: application/json" -H "x-api-key: my-at-least-20-char-long-key" -X POST https://{api_id}.execute-api.{region}.amazonaws.com/api/v1/mock-sales/search --compressed
```

### Useful CDK commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

### Grafana data source setup

You can query and visualize the Quickwit Searcher Lambda from Grafana by using the [Quickwit data source for Grafana](https://grafana.com/grafana/plugins/quickwit-quickwit-datasource/).

#### Prerequisites

- [Set up HTTP API endpoint for Quickwit Searcher Lambda](#set-up-a-search-api)
- [Install Quickwit data source plugin on Grafana](https://github.com/quickwit-oss/quickwit-datasource#installation)

#### Configure Grafana data source

You need to provide the following information.

|Variable|Description|Example|
|--|--|--|
|HTTP URL| HTTP search endpoint for Quickwit Searcher Lambda | https://*******.execute-api.us-east-1.amazonaws.com/api/v1 |
|Custom HTTP Headers| If you configure API Gateway to require an API key, set `x-api-key` HTTP Header | Header: `x-api-key` <br> Value: API key value|
|Index ID| Same as `QW_LAMBDA_INDEX_ID` | hdfs-logs |

After entering these values, click "Save & test" and you can now query your Quickwit Lambda from Grafana!
