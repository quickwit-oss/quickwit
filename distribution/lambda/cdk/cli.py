"""Helper scripts to test and explore the deployed infrastructure.

These functions are wrapped by the Makefile for convenience."""

import base64
import gzip
import http.client
import json
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass
from functools import cache

import boto3
import botocore.config
import botocore.exceptions
from cdk import app
from cdk.stacks import hdfs_stack, mock_data_stack

region = os.environ["CDK_REGION"]

example_bucket = "quickwit-datasets-public.s3.amazonaws.com"
# example_file = "hdfs-logs-multitenants-10000.json"
example_file = "hdfs-logs-multitenants.json.gz"
INDEXING_BOTO_CONFIG = botocore.config.Config(
    retries={"max_attempts": 0}, read_timeout=60 * 15
)
session = boto3.Session(region_name=region)


@cache
def _get_cloudformation_output_value(stack_name: str, export_name: str) -> str:
    client = session.client("cloudformation")
    stacks = client.describe_stacks(StackName=stack_name)["Stacks"]
    if len(stacks) != 1:
        print(f"Stack {stack_name} not identified uniquely, found {stacks}")
    outputs = stacks[0]["Outputs"]
    for output in outputs:
        if output["ExportName"] == export_name:
            return output["OutputValue"]
    else:
        print(f"Export name {export_name} not found in stack {stack_name}")
        exit(1)


@dataclass
class LambdaResult:
    function_error: str
    log_tail: str
    payload: str

    @staticmethod
    def from_lambda_response(lambda_resp: dict) -> "LambdaResult":
        return LambdaResult(
            function_error=lambda_resp.get("FunctionError", ""),
            log_tail=base64.b64decode(lambda_resp["LogResult"]).decode(),
            payload=lambda_resp["Payload"].read().decode(),
        )

    def extract_report(self) -> str:
        """Expect "REPORT RequestId: xxx Duration: yyy..." as last line in log tail"""
        return self.log_tail.strip().splitlines()[-1]


def _format_lambda_output(lambda_result: LambdaResult, duration=None):
    if lambda_result.function_error != "":
        print("\n## FUNCTION ERROR:")
        print(lambda_result.function_error)
    print("\n## LOG TAIL:")
    print(lambda_result.log_tail)
    print("\n## RESPONSE:")
    print(lambda_result.payload)
    if duration is not None:
        print("\n## TOTAL INVOCATION DURATION:")
        print(duration)


def upload_hdfs_src_file():
    bucket_name = _get_cloudformation_output_value(
        app.HDFS_STACK_NAME, hdfs_stack.INDEX_STORE_BUCKET_NAME_EXPORT_NAME
    )
    uri = f"s3://{bucket_name}/{example_file}"
    try:
        resp = session.client("s3").head_object(Bucket=bucket_name, Key=example_file)
        print(f"{uri} already exists ({resp['ContentLength']} bytes), skipping upload")
        return
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise e
    print(f"upload src file to {uri}")
    conn = http.client.HTTPSConnection(example_bucket)
    conn.request("GET", f"/{example_file}")
    response = conn.getresponse()
    if response.status != 200:
        print(f"Failed to fetch dataset")
        exit(1)
    with tempfile.NamedTemporaryFile() as tmp:
        unzipped_resp = gzip.GzipFile(mode="rb", fileobj=response)
        while True:
            chunk = unzipped_resp.read(1024 * 1024)
            if len(chunk) == 0:
                break
            tmp.write(chunk)
        tmp.flush()
        print(f"downloaded {tmp.tell()} bytes")
        session.client("s3").upload_file(
            Bucket=bucket_name, Filename=tmp.name, Key=example_file
        )


def invoke_hdfs_indexer() -> LambdaResult:
    function_name = _get_cloudformation_output_value(
        app.HDFS_STACK_NAME, hdfs_stack.INDEXER_FUNCTION_NAME_EXPORT_NAME
    )
    print(f"indexer function name: {function_name}")
    bucket_name = _get_cloudformation_output_value(
        app.HDFS_STACK_NAME, hdfs_stack.INDEX_STORE_BUCKET_NAME_EXPORT_NAME
    )
    source_uri = f"s3://{bucket_name}/{example_file}"
    print(f"src_file: {source_uri}")
    invoke_start = time.time()
    resp = session.client("lambda", config=INDEXING_BOTO_CONFIG).invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=f"""{{ "source_uri": "{source_uri}" }}""",
    )
    invoke_duration = time.time() - invoke_start
    lambda_result = LambdaResult.from_lambda_response(resp)
    _format_lambda_output(lambda_result, invoke_duration)
    return lambda_result


def _invoke_searcher(
    stack_name: str, function_export_name: str, payload: str
) -> LambdaResult:
    function_name = _get_cloudformation_output_value(stack_name, function_export_name)
    client = session.client("lambda")
    print(f"searcher function name: {function_name}")
    invoke_start = time.time()
    resp = client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=payload,
    )
    invoke_duration = time.time() - invoke_start
    lambda_result = LambdaResult.from_lambda_response(resp)
    _format_lambda_output(lambda_result, invoke_duration)
    return lambda_result


def invoke_hdfs_searcher(payload: str) -> LambdaResult:
    return _invoke_searcher(
        app.HDFS_STACK_NAME,
        hdfs_stack.SEARCHER_FUNCTION_NAME_EXPORT_NAME,
        payload,
    )


def invoke_mock_data_searcher():
    _invoke_searcher(
        app.MOCK_DATA_STACK_NAME,
        mock_data_stack.SEARCHER_FUNCTION_NAME_EXPORT_NAME,
        """{"query": "id:1", "sort_by": "ts", "max_hits": 10}""",
    )


def _clean_s3_bucket(bucket_name: str, prefix: str = ""):
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()


@cache
def _git_commit():
    return subprocess.run(
        ["git", "describe", "--dirty"], check=True, capture_output=True, text=True
    ).stdout.strip()


def benchmark_hdfs_indexing():
    memory_size = os.environ["INDEXER_MEMORY_SIZE"]
    bucket_name = _get_cloudformation_output_value(
        app.HDFS_STACK_NAME, hdfs_stack.INDEX_STORE_BUCKET_NAME_EXPORT_NAME
    )
    for _ in range(2):
        _clean_s3_bucket(bucket_name, "index/")
        bench_result = {
            "run": "benchmark_hdfs_indexing",
            "ts": time.time(),
            "commit": _git_commit(),
            "memory_size": memory_size,
            "env": {
                k: os.environ[k]
                for k in os.environ.keys()
                if k != "QW_LAMBDA_OPENTELEMETRY_AUTHORIZATION"
            },
        }
        try:
            indexer_result = invoke_hdfs_indexer()
            bench_result["lambda_report"] = indexer_result.extract_report()
        except Exception as e:
            bench_result["invokation_error"] = repr(e)
            print(f"Failed to invoke indexer")

        with open(f"lambda-bench.log", "a+") as f:
            f.write(json.dumps(bench_result))
            f.write("\n")


def benchmark_hdfs_search(payload: str):
    memory_size = os.environ["SEARCHER_MEMORY_SIZE"]
    for _ in range(2):
        bench_result = {
            "run": "benchmark_hdfs_search",
            "ts": time.time(),
            "commit": _git_commit(),
            "memory_size": memory_size,
            "payload": json.loads(payload),
            "env": {
                k: os.environ[k]
                for k in os.environ.keys()
                if k != "QW_LAMBDA_OPENTELEMETRY_AUTHORIZATION"
            },
        }
        try:
            indexer_result = invoke_hdfs_searcher(payload)
            bench_result["lambda_report"] = indexer_result.extract_report()
        except Exception as e:
            bench_result["invokation_error"] = repr(e)
            print(f"Failed to invoke searcher")

        with open(f"lambda-bench.log", "a+") as f:
            f.write(json.dumps(bench_result))
            f.write("\n")
