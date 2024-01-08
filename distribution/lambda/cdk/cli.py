"""Helper scripts to test and explore the deployed infrastructure.

These functions are wrapped by the Makefile for convenience."""

import base64
import gzip
import http.client
import json
import os
import re
import subprocess
import tempfile
import time
from dataclasses import dataclass
from functools import cache
from io import BytesIO

import boto3
import botocore.config
import botocore.exceptions
from cdk import app
from cdk.stacks.examples import hdfs_stack, mock_data_stack

region = os.environ["CDK_REGION"]

example_host = "quickwit-datasets-public.s3.amazonaws.com"
# the publicly hosted file is compressed and suffixed with ".gz"
example_hdfs_file = "hdfs-logs-multitenants.json"
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
    raw_size_bytes: int

    @staticmethod
    def from_lambda_response(lambda_resp: dict) -> "LambdaResult":
        payload = lambda_resp["Payload"].read().decode()
        return LambdaResult(
            function_error=lambda_resp.get("FunctionError", ""),
            log_tail=base64.b64decode(lambda_resp["LogResult"]).decode(),
            payload=payload,
            raw_size_bytes=len(payload),
        )

    @staticmethod
    def from_lambda_gateway_response(lambda_resp: dict) -> "LambdaResult":
        gw_str = lambda_resp["Payload"].read().decode()
        gw_obj = json.loads(gw_str)
        payload = gw_obj["body"]
        if gw_obj["isBase64Encoded"]:
            dec_payload = base64.b64decode(payload)
            if gw_obj.get("headers", {}).get("content-encoding", "") == "gzip":
                payload = (
                    gzip.GzipFile(mode="rb", fileobj=BytesIO(dec_payload))
                    .read()
                    .decode()
                )
            else:
                payload = dec_payload.decode()
        return LambdaResult(
            function_error=lambda_resp.get("FunctionError", ""),
            log_tail=base64.b64decode(lambda_resp["LogResult"]).decode(),
            payload=payload,
            raw_size_bytes=len(gw_str),
        )

    def extract_report(self) -> str:
        """Expect "REPORT RequestId: xxx Duration: yyy..." to be in log tail"""
        for line in reversed(self.log_tail.strip().splitlines()):
            if line.startswith("REPORT"):
                return line
        else:
            raise ValueError(f"Could not find report in log tail")

    def request_id(self) -> str:
        report = self.extract_report()
        match = re.search(r"RequestId: ([0-9a-z\-]+)", report)
        if match:
            return match.group(1)
        else:
            raise ValueError(f"Could not find RequestId in report: {report}")


def _format_lambda_output(
    lambda_result: LambdaResult, duration=None, max_resp_size=10 * 1000
):
    if lambda_result.function_error != "":
        print("\n## FUNCTION ERROR:")
        print(lambda_result.function_error)
    print("\n## LOG TAIL:")
    print(lambda_result.log_tail)
    print("\n## RAW RESPONSE SIZE (BYTES):")
    ratio = lambda_result.raw_size_bytes / len(lambda_result.payload)
    print(f"{lambda_result.raw_size_bytes} ({ratio:.1f}x the final payload)")
    print("\n## RESPONSE:")
    payload_size = len(lambda_result.payload)
    print(lambda_result.payload[:max_resp_size])
    if payload_size > max_resp_size:
        print(f"Response too long ({payload_size}), truncated to {max_resp_size} bytes")

    if duration is not None:
        print("\n## TOTAL INVOCATION DURATION:")
        print(duration)


def upload_hdfs_src_file():
    bucket_name = _get_cloudformation_output_value(
        app.HDFS_STACK_NAME, hdfs_stack.INDEX_STORE_BUCKET_NAME_EXPORT_NAME
    )
    uri = f"s3://{bucket_name}/{example_hdfs_file}"
    try:
        resp = session.client("s3").head_object(
            Bucket=bucket_name, Key=example_hdfs_file
        )
        print(f"{uri} already exists ({resp['ContentLength']} bytes), skipping upload")
        return
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise e
    print(f"download dataset https://{example_host}/{example_hdfs_file}.gz")
    conn = http.client.HTTPSConnection(example_host)
    conn.request("GET", f"/{example_hdfs_file}.gz")
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
        print(f"upload dataset to {uri}")
        session.client("s3").upload_file(
            Bucket=bucket_name, Filename=tmp.name, Key=example_hdfs_file
        )


def invoke_hdfs_indexer() -> LambdaResult:
    function_name = _get_cloudformation_output_value(
        app.HDFS_STACK_NAME, hdfs_stack.INDEXER_FUNCTION_NAME_EXPORT_NAME
    )
    print(f"indexer function name: {function_name}")
    bucket_name = _get_cloudformation_output_value(
        app.HDFS_STACK_NAME, hdfs_stack.INDEX_STORE_BUCKET_NAME_EXPORT_NAME
    )
    source_uri = f"s3://{bucket_name}/{example_hdfs_file}"
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
    stack_name: str,
    function_export_name: str,
    payload: str,
    download_logs: bool,
) -> LambdaResult:
    function_name = _get_cloudformation_output_value(stack_name, function_export_name)
    client = session.client("lambda")
    print(f"searcher function name: {function_name}")
    invoke_start = time.time()
    resp = client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps(
            {
                "headers": {"Content-Type": "application/json"},
                "requestContext": {
                    "http": {"method": "POST"},
                },
                "body": payload,
                "isBase64Encoded": False,
            }
        ),
    )
    invoke_duration = time.time() - invoke_start
    lambda_result = LambdaResult.from_lambda_gateway_response(resp)
    _format_lambda_output(lambda_result, invoke_duration)
    if download_logs:
        download_logs_to_file(lambda_result.request_id(), function_name, invoke_start)
    return lambda_result


def invoke_hdfs_searcher(payload: str, download_logs: bool = True) -> LambdaResult:
    return _invoke_searcher(
        app.HDFS_STACK_NAME,
        hdfs_stack.SEARCHER_FUNCTION_NAME_EXPORT_NAME,
        payload,
        download_logs,
    )


def get_logs(
    function_name: str, request_id: str, timestamp_unix_ms: int, timeout: float = 60
):
    print(f"Getting logs for requestId: {request_id}...")
    client = session.client("logs")
    log_group_name = f"/aws/lambda/{function_name}"
    paginator = client.get_paginator("filter_log_events")
    lower_time_bound = timestamp_unix_ms - 1000 * 3600
    upper_time_bound = timestamp_unix_ms + 1000 * 3600
    last_event_id = ""
    last_event_found = True
    start_time = time.time()
    while time.time() - start_time < timeout:
        for page in paginator.paginate(
            logGroupName=log_group_name,
            filterPattern=f"%{request_id}%",
            startTime=lower_time_bound,
            endTime=upper_time_bound,
        ):
            for event in page["events"]:
                if last_event_found or event["eventId"] == last_event_id:
                    last_event_found = True
                    last_event_id = event["eventId"]
                    yield event["message"]
                    if event["message"].startswith("REPORT"):
                        print(event["message"])
                        lower_time_bound = int(event["timestamp"])
                        last_event_id = "REPORT"
                        break
            if last_event_id == "REPORT":
                break
        if last_event_id == "REPORT":
            break
        elif last_event_id == "":
            print(f"no event found, retrying...")
        else:
            print(f"last event not found, retrying...")
            last_event_found = False
        time.sleep(3)

    else:
        raise TimeoutError(f"Log collection timed out after {timeout}s")


def download_logs_to_file(request_id: str, function_name: str, invoke_start: float):
    with open(f"lambda.{request_id}.log", "w") as f:
        for log in get_logs(
            function_name,
            request_id,
            int(invoke_start * 1000),
        ):
            f.write(log)


def invoke_mock_data_searcher():
    _invoke_searcher(
        app.MOCK_DATA_STACK_NAME,
        mock_data_stack.SEARCHER_FUNCTION_NAME_EXPORT_NAME,
        """{"query": "id:1", "sort_by": "ts", "max_hits": 10}""",
        True,
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
    _clean_s3_bucket(bucket_name, "index/")
    bench_result = {
        "run": "benchmark_hdfs_indexing",
        "ts": time.time(),
        "commit": _git_commit(),
        "memory_size": memory_size,
        "env": {
            k: os.environ[k]
            for k in os.environ.keys()
            if k.startswith("QW_LAMBDA_")
            and k != "QW_LAMBDA_OPENTELEMETRY_AUTHORIZATION"
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
            indexer_result = invoke_hdfs_searcher(payload, download_logs=False)
            bench_result["lambda_report"] = indexer_result.extract_report()
        except Exception as e:
            bench_result["invokation_error"] = repr(e)
            print(f"Failed to invoke searcher")

        with open(f"lambda-bench.log", "a+") as f:
            f.write(json.dumps(bench_result))
            f.write("\n")
